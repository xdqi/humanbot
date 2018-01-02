import traceback
from os import popen, getpid
from threading import current_thread
from datetime import datetime
from io import BytesIO
from random import randint
import re
from logging import getLogger

from expiringdict import ExpiringDict

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.errors.rpc_error_list import AuthKeyUnregisteredError, PeerIdInvalidError
from telethon.tl.types import \
    UpdateNewChannelMessage, UpdateShortMessage, UpdateShortChatMessage, UpdateNewMessage, \
    UpdateUserStatus, UpdateUserName, Message, MessageService, MessageMediaPhoto, MessageMediaDocument, \
    MessageActionChatEditTitle, \
    PeerUser, InputUser, User, Chat, ChatFull, Channel, ChannelFull, \
    ChatInvite
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.utils import get_peer_id, resolve_id

import models
import config
import realbot
from models import insert_message_local_timezone, update_user_real, update_group_real
from utils import upload, ocr

PUBLIC_REGEX = re.compile(r"t(?:elegram)?\.me/([a-zA-Z][\w\d]{3,30}[a-zA-Z\d])")
INVITE_REGEX = re.compile(r'(t(?:elegram)?\.me/joinchat/[a-zA-Z0-9_-]{22})')
logger = getLogger(__name__)


def send_message_to_administrators(msg: str):
    if len(msg.encode('utf-8')) > 500 or len(msg.splitlines()) > 10:
        buffer = BytesIO(msg.encode('utf-8'))
        now = datetime.now()
        date = now.strftime('%y%m%d')
        timestamp = now.timestamp()
        path = '/log/{}'.format(date)
        thread_name = current_thread().name
        filename = '{}-{}.txt'.format(thread_name, timestamp)
        exception = msg.splitlines()[-1]
        upload(buffer, path, filename)
        msg = 'Long message: ... {}\nURL: http://fra2.dom.ain.kwsv.win{}/{}'.format(
            exception,
            path,
            filename
        )
    gid, peer_type = resolve_id(config.ADMIN_CHANNEL)
    client.send_message(entity=client.get_entity(peer_type(gid)),
                        message='```{}```'.format(msg.strip()),
                        parse_mode='markdown',
                        link_preview=False)


def find_link_to_join(session, msg: str):
    public_links = PUBLIC_REGEX.findall(msg)
    private_links = INVITE_REGEX.findall(msg)

    if public_links + private_links:
        print('found links', 'public:', public_links, 'private:', private_links)

    for link in public_links:
        if link in config.GROUP_BLACKLIST:  # false detection of private link
            continue
        group = client.get_entity(link)
        if isinstance(group, Chat) or (isinstance(group, Channel) and not group.broadcast):
            gid = peer_to_internal_id(group)
            group_exist = session.query(models.Group).filter(models.Group.gid == gid).one_or_none()
            if not group_exist:
                link = group.username if hasattr(group, 'username') else None
                new_group = models.Group(gid=gid, name=group.title, link=link)
                session.add(new_group)
                result = client.invoke(JoinChannelRequest(group))
                send_message_to_administrators('joined public group {}: {} having {} members,'
                                               ' date {}.\nresult: {}'.format(
                    link,
                    group.title,
                    group.participants_count,
                    group.date,
                    result
                )
                )
                group_last_changed[gid] = True

    for link in private_links:
        invite_hash = link[-22:]
        group = client.invoke(CheckChatInviteRequest(invite_hash))
        if isinstance(group, ChatInvite) and group.participants_count > 1 and not group.broadcast:
            send_message_to_administrators('invitation from {}: {}, {} members\n'
                                           'Join group with /joinprv {}'.format(
                link,
                group.title,
                group.participants_count,
                link[-22:]
            )
            )


def peer_to_internal_id(peer):
    """
    Get bot marked ID

    :param peer:
    :return:
    """
    return get_peer_id(peer)


user_last_changed = ExpiringDict(max_len=10000, max_age_seconds=3600)
def update_user(user_id):
    if user_id in user_last_changed:  # user should be updated at a minute basis
        return
    user = client.get_entity(user_id)  # type: User
    user_last_changed[user_id] = True
    update_user_real(user_id, user.first_name, user.last_name, user.username, user.lang_code)


group_last_changed = ExpiringDict(max_len=1000, max_age_seconds=300)
def update_group(chat_id: int, title: str = None):
    """
    Try to update group information

    :param chat_id: Chat ID (bot marked format)
    :param title: New group title (optional)
    :return: None
    """
    if chat_id in group_last_changed:  # user should be updated at a minute basis
        return
    id, type = resolve_id(chat_id)
    peer = type(id)
    group = client.get_entity(peer)
    group_last_changed[chat_id] = True
    if isinstance(group, (Chat, ChatFull)):
        update_group_real(peer_to_internal_id(chat_id), title or group.title, None)
    elif isinstance(group, (Channel, ChannelFull)):
        update_group_real(peer_to_internal_id(chat_id), title or group.title, group.username)


def update_chat_generic(chat_id: int):
    """
    Received message

    :param chat_id: bot marked format chat id
    :return:
    """
    input_entity = client.get_input_entity(chat_id)
    if isinstance(input_entity, InputUser):
        update_user(chat_id)
    else:
        update_group(chat_id)


def update_group_title(chat_id: int, update: MessageActionChatEditTitle):
    """
    Update group title event handler

    :param chat_id: Bot marked chat id
    :param update:
    :return:
    """
    name = update.title
    if chat_id in group_last_changed:
        del group_last_changed[chat_id]
    update_group(chat_id, name)


def download_file(media: MessageMediaPhoto):
    print('pic to download')
    # download from telegram server
    buffer = BytesIO()
    client.download_media(media, buffer)
    buffer.seek(0)
    print('pic downloaded')

    # calculate path
    original = media.photo.sizes[-1]  # type: PhotoSize
    location = original.location  # type: FileLocation
    path = '/{}/{}'.format(location.dc_id, location.volume_id)
    filename = '{}.jpg'.format(location.local_id)

    return buffer, path, filename


def download_upload_ocr(media: MessageMediaPhoto):
    buffer, path, filename = download_file(media)
    fullpath = upload(buffer, path, filename)
    return ocr(fullpath)


def update_message(update: Message):
    if isinstance(update.to_id, PeerUser) and update.to_id.user_id == config.MY_UID:  # private message
        chat = update.from_id
    else:
        chat = peer_to_internal_id(update.to_id)
    if update.message:
        insert_message_local_timezone(chat, update.from_id, update.message, update.date)
    elif isinstance(update.media, (MessageMediaDocument, MessageMediaPhoto)):
        text = update.media.caption or ''  # in case it is `None`
        if isinstance(update.media, MessageMediaPhoto):
            result = download_upload_ocr(update.media)
            text = result + '\n' + text
            insert_message_local_timezone(chat, update.from_id, text, update.date)

    update_chat_generic(chat)
    update_user(update.from_id)
    rnd = randint(0, 10)
    if rnd == 5 and not isinstance(update.to_id, PeerUser):
        client.send_read_acknowledge(client.get_entity(update.to_id), max_id=update.id)


def update_message_from_chat(update: UpdateShortChatMessage):
    insert_message_local_timezone(-update.chat_id, update.from_id, update.message, update.date)
    update_group(-update.chat_id)
    update_user(update.from_id)


def update_message_from_user(update: UpdateShortMessage):
    insert_message_local_timezone(update.user_id, update.user_id, update.message, update.date)
    update_user(update.user_id)
    if update.user_id in config.ADMIN_UIDS:
        output = ''
        if update.message.startswith('/exec'):
            command = update.message[5:].strip()
            print('executing command', command)
            with popen(command, 'r') as f:
                output = f.read()
        elif update.message.startswith('/py'):
            script = update.message[3:].strip()
            print('evaluating script', script)
            output = repr(eval(script))
        elif update.message.startswith('/joinpub'):
            link = update.message[8:].strip()
            print('joining public group', link)
            output = client.invoke(JoinChannelRequest(client.get_entity(link)))
        elif update.message.startswith('/leavepub'):
            link = update.message[9:].strip()
            print('leaving public group', link)
            output = client.invoke(LeaveChannelRequest(client.get_entity(link)))
        elif update.message.startswith('/joinprv'):
            link = update.message[8:].strip()
            print('joining private group', link)
            output = client.invoke(ImportChatInviteRequest(link))
        elif update.message.startswith('/threads'):
            output = thread_called_count
        elif update.message.startswith('/stat'):
            output = 'Uptime: {}s\nProcessed: {}\nAverage: {}s'.format(
                (datetime.now() - start_time).total_seconds(),
                received_message,
                total_used_time / received_message
            )
        if output:
            output = '```{}```'.format(output)
            print('sending message', output)
            client.send_message(entity=update.user_id,
                                message=output,
                                reply_to=update.id,
                                parse_mode='markdown',
                                link_preview=False
                                )


def update_handler(update):
    print('humanbot', update)
    if isinstance(update, (UpdateNewChannelMessage, UpdateNewMessage)):  # message from group/user
        if isinstance(update.message, Message):  # message
            update_message(update.message)
        elif isinstance(update.message, MessageService):  # action
            if isinstance(update.message.action, MessageActionChatEditTitle):
                update_group_title(peer_to_internal_id(update.message.to_id), update.message.action)

    elif isinstance(update, UpdateShortMessage):  # private message
        update_message_from_user(update)

    elif isinstance(update, UpdateShortChatMessage):  # short message from normal group
        update_message_from_chat(update)

    elif isinstance(update, UpdateUserStatus):  # user status update
        pass

    elif isinstance(update, UpdateUserName):  # user name change
        update_user_real(update.user_id, update.first_name, update.last_name, update.username, None)

    else:
        pass


thread_called_count = {}
received_message = 0
total_used_time = 0
start_time = datetime.now()
def update_handler_wrapper(update):
    prev_num = thread_called_count.get(current_thread().name, 0)
    thread_called_count[current_thread().name] = prev_num + 1
    process_start_time = datetime.now()
    try:
        update_handler(update)
    except Exception as e:
        info = 'Exception raised on PID {}, {}\n'.format(getpid(), current_thread())
        exc = traceback.format_exc()
        send_to_admin = True

        # special process with common exceptions
        if isinstance(e, ValueError) and 'encountered this peer before' in e.args[0]:
            exc = e.args[0]
            send_to_admin = False
        elif isinstance(e, (AuthKeyUnregisteredError, PeerIdInvalidError)):
            exc = e.args

        print(info + exc)
        if send_to_admin:  # exception that should be send to administrator
            send_message_to_administrators(info + exc)

    process_end_time = datetime.now()
    process_time = process_end_time - process_start_time
    global received_message, total_used_time
    received_message += 1
    total_used_time += process_time.total_seconds()


def main():
    global client
    client = TelegramClient(session=config.SESSION_NAME,
                            api_id=config.TG_API_ID,
                            api_hash=config.TG_API_HASH,
                            proxy=None,
                            update_workers=8,
                            spawn_read_thread=False)
    print('INFO: Connecting to Telegram Servers...')
    client.connect()

    if not client.is_user_authorized():
        print('INFO: Unauthorized user')
        client.send_code_request(config.PHONE_NUMBER)
        code_ok = False
        while not code_ok:
            code = input('Enter the auth code: ')
            try:
                code_ok = client.sign_in(config.PHONE_NUMBER, code)
            except SessionPasswordNeededError:
                password = input('Two step verification enabled. Please enter your password: ')
                code_ok = client.sign_in(password=password)

    print('INFO: Client initialized succesfully!')

    client.add_update_handler(update_handler_wrapper)
    realbot.main()
    client.idle()
    client.disconnect()


if __name__ == '__main__':
    main()
