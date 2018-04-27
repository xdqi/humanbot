from gevent import monkey; monkey.patch_all(); del monkey

import traceback
from os import getpid
from threading import current_thread, Thread
from datetime import datetime, timezone
from io import BytesIO
import re
from time import sleep
from logging import getLogger, INFO, WARNING, basicConfig
from pdb import Pdb
from signal import signal, SIGUSR1
from ast import literal_eval

from sqlalchemy.exc import OperationalError

from telethon.errors import SessionPasswordNeededError
from telethon import events, TelegramClient
from telethon.errors.rpc_error_list import AuthKeyUnregisteredError, PeerIdInvalidError, \
    InviteHashExpiredError, InviteHashInvalidError, FloodWaitError
from telethon.tl.types import MessageMediaPhoto, \
    PeerUser, InputUser, User, Chat, ChatFull, Channel, ChannelFull, \
    ChatInvite
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.utils import resolve_id

from senders import invoker
import models
import cache
import config
from models import update_user_real, update_group_real, Session
from utils import upload_pic, ocr, get_now_timestamp, send_message_to_administrators, report_exception, \
    peer_to_internal_id, test_and_join_public_channel, need_to_be_online
import realbot
import sms

logger = getLogger(__name__)

PUBLIC_REGEX = re.compile(r"t(?:elegram)?\.me/([a-zA-Z][\w\d]{3,30}[a-zA-Z\d])")
PUBLIC_AT_REGEX = re.compile(r"@([a-zA-Z][\w\d]{3,30}[a-zA-Z\d])")
INVITE_REGEX = re.compile(r'(t(?:elegram)?\.me/joinchat/[a-zA-Z0-9_-]{22})')
recent_found_links = cache.RedisExpiringSet('recent_found_links', expire=3600)
def find_link_to_join(session, msg: str):
    public_links = set(PUBLIC_REGEX.findall(msg)).union(PUBLIC_AT_REGEX.findall(msg))
    private_links = set(INVITE_REGEX.findall(msg))

    if public_links or private_links:
        logger.info('found links. public: %s, private: %s', public_links, private_links)

    for link in public_links:
        if link in config.GROUP_BLACKLIST:  # false detection of private link
            continue
        if link in recent_found_links:
            logger.warning(f'Group @{link} is in recent found links, skip')
            continue
        recent_found_links.add(link)
        gid, joined = test_and_join_public_channel(session, link)
        if joined:
            group_last_changed.add(str(gid))

    for link in private_links:
        invite_hash = link[-22:]
        if invite_hash in recent_found_links:
            continue
        recent_found_links.add(invite_hash)
        try:
            group = invoker.invoke(CheckChatInviteRequest(invite_hash))
        except (InviteHashExpiredError, InviteHashInvalidError) as e:
            report_exception()
            continue
        except FloodWaitError as e:
            logger.warning('Unable to resolve now, %r', e)
            continue
        if isinstance(group, ChatInvite) and group.participants_count > config.GROUP_MEMBER_JOIN_LIMIT:
            send_message_to_administrators('invitation from {}: {}, {} members\n'
                                           'Join {} with /joinprv {}'.format(
                    link,
                    group.title,
                    group.participants_count,
                    'channel' if group.broadcast else 'group',
                    link[-22:]
                )
            )


class WorkProperties(type):
    def __new__(mcs, class_name, class_bases, class_dict):
        name = class_dict['name']
        new_class_dict = class_dict.copy()
        new_class_dict['status'] = cache.RedisDict(name + '_worker_status')
        new_class_dict['queue'] = cache.RedisQueue(name + '_queue')
        return type.__new__(mcs, class_name, class_bases, new_class_dict)


class Worker(Thread, metaclass=WorkProperties):
    name = ''
    status = None  # type: cache.RedisDict
    queue = None  # type: cache.RedisQueue

    def __init__(self):
        super().__init__(name=self.name)

    def run(self):
        session = Session()

        while True:
            try:
                message = self.queue.get()  # type: str
                if message is None:
                    sleep(0.01)
                    continue
                self.handler(session, message)
                session.commit()
                self.queue.task_done()
                self.status['last'] = get_now_timestamp()
                self.status['size'] = self.queue.qsize()
            except KeyboardInterrupt:
                break
            except:
                report_exception()
                session.rollback()
                self.queue.put(message)

        session.close()
        Session.remove()

    def handler(self, session, message):
        raise NotImplementedError


class MessageInsertWorker(Worker):
    name = 'insert'

    def handler(self, session, message: str):
        chat = models.ChatNew(**literal_eval(message))
        session.add(chat)


class MessageMarkWorker(Worker):
    name = 'mark'

    def handler(self, session, message: str):
        request_changes = literal_eval(message)  # {'chat_id': 114, 'message_id': 514}
        session.query(models.ChatNew).filter(
            models.ChatNew.chat_id == request_changes['chat_id'],
            models.ChatNew.message_id == request_changes['message_id']
        ).update({
            models.ChatNew.flag: models.ChatNew.flag.op('|')(models.ChatFlag.deleted)
        }, synchronize_session='fetch')


def threads_handler(bot, update, text):
    global thread_called_count
    return str(thread_called_count)


def statistics_handler(bot, update, text):
    global start_time, global_count
    return 'Uptime: {}s\nProcessed: {}\nAverage: {}s'.format(
                get_now_timestamp() - int(global_count['start_time']),
                global_count['received_message'],
                float(global_count['total_used_time']) / float(global_count['received_message'])
            )


def workers_handler(bot, update, text):
    insert_last = int(MessageInsertWorker.status['last'])
    insert_size = MessageInsertWorker.queue.qsize()
    find_link_last = int(FindLinkWorker.status['last'])
    find_link_size = FindLinkWorker.queue.qsize()
    mark_last = int(MessageMarkWorker.status['last'])
    mark_size = MessageMarkWorker.queue.qsize()
    return 'Input Message Worker: {} seconds ago, size {}\n' \
           'Mark Worker: {} seconds ago, size {}\n' \
           'Find Link Worker: {} seconds ago, size {}\n'.format(
                get_now_timestamp() - insert_last, insert_size,
                get_now_timestamp() - mark_last, mark_size,
                get_now_timestamp() - find_link_last, find_link_size
            )


class FindLinkWorker(Worker):
    name = 'find_link'

    def handler(self, session, message):
        find_link_to_join(session, message)


def insert_message(chat_id: int, message_id, user_id: int, msg: str, date: datetime, flag=models.ChatFlag.new):
    if not msg:  # Not text message
        return
    utc_timestamp = int(date.timestamp())

    chat = dict(chat_id=chat_id,
                message_id=message_id,
                user_id=user_id,
                text=msg,
                date=utc_timestamp,
                flag=flag)

    MessageInsertWorker.queue.put(repr(chat))
    if FindLinkWorker.queue.qsize() > 50:
        send_message_to_administrators('Find link queue full, worker dead?')
        FindLinkWorker().start()
    else:
        FindLinkWorker.queue.put(msg)


def insert_message_local_timezone(chat_id, message_id, user_id, msg, date: datetime, flag=models.ChatFlag.new):
    utc_date = date.replace(tzinfo=timezone.utc)
    insert_message(chat_id, message_id, user_id, msg, utc_date, flag)


user_last_changed = cache.RedisExpiringSet('user_last_changed', expire=3600)
def update_user(client, user_id):
    if user_id is None or user_id in user_last_changed:  # user should be updated at a minute basis
        return
    try:
        user = client.get_entity(PeerUser(user_id))  # type: User
    except (KeyError, TypeError) as e:
        logger.warning('Get user info failed: %s', user_id)
        report_exception()
        return
    user_last_changed.add(user_id)
    update_user_real(user_id, user.first_name, user.last_name, user.username, user.lang_code)


group_last_changed = cache.RedisExpiringSet('group_last_changed', expire=3600)
def update_group(client, chat_id: int, title: str = None):
    """
    Try to update group information

    :param chat_id: Chat ID (bot marked format)
    :param title: New group title (optional)
    :return: None
    """
    if str(chat_id) in group_last_changed:  # user should be updated at a minute basis
        return
    id, type = resolve_id(chat_id)
    peer = type(id)
    group = client.get_entity(peer)
    group_last_changed.add(str(chat_id))
    if isinstance(group, (Chat, ChatFull)):
        update_group_real(peer_to_internal_id(chat_id), title or group.title, None)
    elif isinstance(group, (Channel, ChannelFull)):
        update_group_real(peer_to_internal_id(chat_id), title or group.title, group.username)


def download_file(client, media: MessageMediaPhoto):
    # download from telegram server
    buffer = BytesIO()
    client.download_media(media, buffer)
    buffer.seek(0)
    logger.info('pic downloaded')

    # calculate path
    original = media.photo.sizes[-1]  # type: PhotoSize
    location = original.location  # type: FileLocation
    now = datetime.now()
    path = '/{}/{}'.format(now.year, now.month)
    filename = '{}-{}_{}_{}.jpg'.format(get_now_timestamp(),
                                        location.dc_id,
                                        location.volume_id,
                                        location.local_id)

    return buffer, path, filename


def download_upload_ocr(client, media: MessageMediaPhoto):
    try:
        buffer, path, filename = download_file(client, media)
    except (ValueError, RuntimeError, OSError, AttributeError):
        report_exception()
        return 'tgpic://download-failed'

    fullpath = upload_pic(buffer, path, filename)
    return ocr(fullpath)


thread_called_count = cache.RedisDict('thread_called_count')
global_count = cache.RedisDict('global_count')
global_count['received_message'] = 0
global_count['total_used_time'] = 0
global_count['start_time'] = get_now_timestamp()
def update_handler_wrapper(func):
    def wrapped(event):
        prev_num = int(thread_called_count.get(current_thread().name, 0))
        thread_called_count[current_thread().name] = prev_num + 1
        process_start_time = datetime.now()
        try:
            func(event)
        except Exception as e:
            report_exception()
            info = 'Exception raised on PID {}, {}\n'.format(getpid(), current_thread())
            exc = traceback.format_exc()
            send_to_admin = True

            # special process with common exceptions
            if isinstance(e, ValueError) and 'find the input entity for "PeerUser' in e.args[0]:
                exc = e.args[0]
                send_to_admin = False
            elif isinstance(e, (AuthKeyUnregisteredError, PeerIdInvalidError)):
                exc = repr(e.args)

            logger.error(info + exc)
            if send_to_admin:  # exception that should be send to administrator
                send_message_to_administrators(info + exc)

            process_end_time = datetime.now()
            process_time = process_end_time - process_start_time
            global_count.incrby('received_message', 1)
            global_count.incrby('total_used_time', int(process_time.total_seconds()))

    return wrapped


@update_handler_wrapper
def update_new_message_handler(event: events.NewMessage.Event):
    text = event.text

    flag = models.ChatFlag.new
    if isinstance(event, events.MessageEdited.Event):
        flag = models.ChatFlag.edited

    if event.photo:
        result = download_upload_ocr(event.client, event.media)
        text = result + '\n' + event.text

    insert_message_local_timezone(event.chat_id, event.message.id, event.sender_id, text, event.message.date, flag)

    update_user(event.client, event.sender_id)
    if event.is_group or event.is_channel:
        update_group(event.client, event.chat_id)

    if need_to_be_online():
        event.client.send_read_acknowledge(event.input_chat, max_id=event.message.id, clear_mentions=True)


@update_handler_wrapper
def update_chat_action_handler(event: events.ChatAction.Event):
    if event.user_added or event.user_joined or event.user_left or event.user_kicked:
        update_user(event.client, event.user_id)
    if event.created or event.new_title:
        update_group(event.client, event.chat_id, event.new_title)


@update_handler_wrapper
def update_deleted_message_handler(event: events.MessageDeleted.Event):
    if not event.chat_id:
        logger.error('got a deleted event with chat_id None and message_id %r', event.deleted_ids)
        return
    for message_id in event.deleted_ids:
        MessageMarkWorker.queue.put(str(dict(chat_id=event.chat_id, message_id=message_id)))
    update_group(event.client, event.chat_id)


def main():
    basicConfig(level=INFO)
    logger.setLevel(INFO)
    getLogger('telethon').setLevel(WARNING)

    # launch clients
    for conf in config.CLIENTS:
        client = conf['client']  # type: TelegramClient

        logger.info(f'Connecting to Telegram Servers with {conf["name"]}...')
        client.connect()

        if not client.is_user_authorized():
            logger.info('Unauthorized user')
            client.send_code_request(conf["phone_number"])
            code_ok = False
            while not code_ok:
                code = input('Enter the auth code: ')
                try:
                    code_ok = client.sign_in(conf["phone_number"], code)
                except SessionPasswordNeededError:
                    password = input('Two step verification enabled. Please enter your password: ')
                    code_ok = client.sign_in(password=password)

        logger.info(f'Client {conf["name"]} initialized succesfully!')

        client.add_event_handler(update_new_message_handler, events.NewMessage)
        client.add_event_handler(update_chat_action_handler, events.ChatAction)
        client.add_event_handler(update_new_message_handler, events.MessageEdited)
        client.add_event_handler(update_deleted_message_handler, events.MessageDeleted)

    # launching bot and workers
    realbot.main()
    FindLinkWorker().start()
    MessageInsertWorker().start()
    MessageMarkWorker().start()
    Thread(target=sms.main).start()

    # for debugging
    signal(SIGUSR1, lambda x, y: Pdb().set_trace(y))

    while 1:
        try:
            sleep(1)
        except KeyboardInterrupt:
            break

    # cleanup
    for conf in config.CLIENTS:
        conf['client'].disconnect()


if __name__ == '__main__':
    main()
