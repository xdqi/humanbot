import asyncio
import traceback
from os import getpid, system
from threading import current_thread, Thread
from datetime import datetime
from logging import getLogger, INFO, WARNING, basicConfig
from pdb import Pdb
from signal import signal, SIGUSR1
from functools import wraps

from telethon.errors import SessionPasswordNeededError
from telethon import events, TelegramClient
from telethon.errors import AuthKeyUnregisteredError, PeerIdInvalidError, \
    ChannelPrivateError
from telethon.tl.types import PeerUser, User, Chat, ChatFull, Channel, ChannelFull
from telethon.tl.custom.message import Message

import cache
import config
from models import update_user_real, update_group_real, insert_message_local_timezone, ChatFlag
from utils import get_now_timestamp, send_to_admin_channel, report_exception, \
    peer_to_internal_id, need_to_be_online, get_photo_address, to_json, block, noblock
import session
import senders
import httpd
import realbot
import workers
from discover import find_link_enqueue


logger = getLogger(__name__)


async def threads_handler(bot, update, text):
    global thread_called_count
    return await thread_called_count.repr()


async def statistics_handler(bot, update, text):
    global start_time, global_count
    return 'Uptime: {}s\nProcessed: {}\nAverage: {}s'.format(
                get_now_timestamp() - int(await global_count['start_time']),
                await global_count['received_message'],
                float(await global_count['total_used_time']) / float(await global_count['received_message'])
            )


user_last_changed = cache.RedisExpiringSet('user_last_changed', expire=3600)
async def update_user(client, user_id):
    if user_id is None or await user_last_changed.contains(user_id):  # user should be updated at a minute basis
        return
    try:
        user = await client.get_entity(PeerUser(user_id))  # type: User
    except (KeyError, TypeError) as e:
        logger.warning('Get user info failed: %s', user_id)
        report_exception()
        return
    await user_last_changed.add(user_id)
    await update_user_real(user_id, user.first_name, user.last_name, user.username, user.lang_code)


group_last_changed = cache.RedisExpiringSet('group_last_changed', expire=3600)
async def update_group(client, chat_id: int, title: str = None):
    """
    Try to update group information

    :param chat_id: Chat ID (bot marked format)
    :param title: New group title (optional)
    :return: None
    """
    if await group_last_changed.contains(str(chat_id)):  # user should be updated at a minute basis
        return
    group = await client.get_entity(chat_id)
    await group_last_changed.add(str(chat_id))
    if isinstance(group, (Chat, ChatFull)):
        await update_group_real(client.conf['uid'], peer_to_internal_id(chat_id), title or group.title, None)
    elif isinstance(group, (Channel, ChannelFull)):
        await update_group_real(client.conf['uid'], peer_to_internal_id(chat_id), title or group.title, group.username)


thread_called_count = cache.RedisDict('thread_called_count')
global_count = cache.RedisDict('global_count')
def update_handler_wrapper(func):
    @wraps(func)
    async def wrapped(event: events.NewMessage):
        prev_num = int(await thread_called_count.get(current_thread().name, 0))
        await thread_called_count.set(current_thread().name, prev_num + 1)
        process_start_time = datetime.now()
        try:
            await func(event)
        except Exception as e:
            report_exception()
            info = 'Exception raised on PID {}, {}\n'.format(getpid(), current_thread())
            exc = traceback.format_exc()
            send_to_admin = True

            # special process with common exceptions
            if isinstance(e, ValueError) and 'find the input entity for "PeerUser' in e.args[0]:
                exc = e.args[0]
                send_to_admin = False
                return
            elif isinstance(e, (AuthKeyUnregisteredError, PeerIdInvalidError)):
                exc = repr(e.args)
            elif isinstance(e, AttributeError):
                if isinstance(event, events.NewMessage):
                    exc += '\nmsg\n' + type(event.message) + repr(event.message)
                exc += str(type(event)) + repr(event)

            logger.error(info + exc)
            if send_to_admin:  # exception that should be send to administrator
                await send_to_admin_channel(info + exc)

        process_end_time = datetime.now()
        process_time = process_end_time - process_start_time
        await global_count.incrby('received_message', 1)
        await global_count.incrby('total_used_time', int(process_time.total_seconds()))

    return wrapped


@update_handler_wrapper
async def update_new_message_handler(event: events.NewMessage.Event):
    if not isinstance(event.message, Message):
        return
    text = event.text

    flag = ChatFlag.new
    if isinstance(event, events.MessageEdited.Event):
        flag = ChatFlag.edited

    if event.photo:
        result = await get_photo_address(event.client, event.media)
        text = config.OCR_HINT + '\n' + result + '\n' + event.text

    await insert_message_local_timezone(event.chat_id, event.message.id, event.sender_id, text, event.message.date, flag)
    await find_link_enqueue(event.raw_text)

    await update_user(event.client, event.sender_id)
    if event.is_group or event.is_channel:
        await update_group(event.client, event.chat_id)

    if await need_to_be_online():
        await event.client.send_read_acknowledge(event.input_chat, max_id=event.message.id, clear_mentions=True)


@update_handler_wrapper
async def update_chat_action_handler(event: events.ChatAction.Event):
    if event.user_added or event.user_joined or event.user_left or event.user_kicked:
        await update_user(event.client, event.user_id)
    if event.user_kicked and event.user_id in [conf['uid'] for conf in config.CLIENTS]:
        msg = f'I, {event.client.conf["name"]}, was kicked by {event.kicked_by.username} (uid {event.kicked_by.id})'
        logger.warning(msg)
        await send_to_admin_channel(msg)
    else:
        try:
            await update_group(event.client, event.chat_id)
        except ChannelPrivateError as e:
            msg = ''
            if event.user:
                msg += f'{event.user.username} (uid {event.user.id}) was kicked by'
            if event.kicked_by:
                msg += f' {event.kicked_by.username} (uid {event.kicked_by.id})'
            if event.chat:
                msg += f'in chat {event.chat.title}'
            if hasattr(event.chat, 'username'):
                msg += f'({event.chat.username})'
            msg += traceback.format_exc()
            logger.warning(msg)
            await send_to_admin_channel(msg)


@update_handler_wrapper
async def update_deleted_message_handler(event: events.MessageDeleted.Event):
    if not event.chat_id:
        logger.error('got a deleted event with chat_id None and message_id %r', event.deleted_ids)
        return
    for message_id in event.deleted_ids:
        await workers.MessageMarkWorker.queue.put(to_json(dict(chat_id=event.chat_id, message_id=message_id)))
    await update_group(event.client, event.chat_id)


# TODO: add handler to handle UpdateChannel

async def main():

    basicConfig(level=INFO)
    logger.setLevel(INFO)
    getLogger('telethon').setLevel(WARNING)

    if config.SESSION_USE_MYSQL:
        session.monkey_patch_sqlite_session()
    senders.create_clients()

    await cache.RedisObject.init()
    await global_count.set('received_message', 0)
    await global_count.set('total_used_time', 0)
    await global_count.set('start_time', get_now_timestamp())

    # launch clients
    for conf in config.CLIENTS:
        client = conf['client']  # type: TelegramClient

        logger.info(f'Connecting to Telegram Servers with {conf["name"]}...')
        await client.connect()

        if not await client.is_user_authorized():
            logger.info('Unauthorized user')
            await client.send_code_request(conf["phone_number"])
            code_ok = False
            while not code_ok:
                code = input('Enter the auth code: ')
                try:
                    code_ok = await client.sign_in(phone=conf["phone_number"], code=code)
                except SessionPasswordNeededError:
                    password = input('Two step verification enabled. Please enter your password: ')
                    code_ok = await client.sign_in(password=password)

        logger.info(f'Client {conf["name"]} initialized succesfully!')

        client.add_event_handler(update_new_message_handler, events.NewMessage)
        client.add_event_handler(update_chat_action_handler, events.ChatAction)
        client.add_event_handler(update_new_message_handler, events.MessageEdited)
        client.add_event_handler(update_deleted_message_handler, events.MessageDeleted)

    # launching bot and workers
    await realbot.main()
    workers.FindLinkWorker().start()
    # workers.MessageInsertWorker().start(4)
    # workers.EntityUpdateWorker().start()
    # workers.MessageMarkWorker().start()
    workers.FetchHistoryWorker().start()
    workers.OcrWorker().start(4)
    noblock(httpd.main())

    # for debugging
    signal(SIGUSR1, lambda x, y: Pdb().set_trace(y))

    while 1:
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            system('killall workers')
            break

    # cleanup
    for conf in config.CLIENTS:
        conf['client'].disconnect()


if __name__ == '__main__':
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    realbot.init(0)
    block(main())
