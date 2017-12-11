import traceback
from os import popen, getpid
from threading import current_thread
from datetime import datetime, timezone
from ftplib import FTP, Error as FTPError
from io import BytesIO
from requests import get
import re

from expiringdict import ExpiringDict

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.errors.rpc_error_list import AuthKeyUnregisteredError
from telethon.tl.types import \
    UpdateNewChannelMessage, UpdateShortMessage, UpdateShortChatMessage, UpdateNewMessage, \
    UpdateUserStatus, UpdateUserName, Message, MessageService, MessageMediaPhoto, MessageMediaDocument, \
    MessageActionChatEditTitle, \
    PeerUser, InputUser, User, PeerChat, Chat, ChatFull, PeerChannel, Channel, ChannelFull, \
    Photo, PhotoSize, FileLocation, ChatInvite, ChatInviteAlready
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.utils import get_input_user, get_peer_id, get_input_peer, resolve_id

import models
import config
import realbot

PUBLIC_REGEX = re.compile(r"t(?:elegram)?\.me/([a-zA-Z][\w\d]{3,30}[a-zA-Z\d])")
INVITE_REGEX = re.compile(r'(t(?:elegram)?\.me/joinchat/[a-zA-Z0-9_-]{22})')


def send_message_to_administrators(msg: str):
    for admin in config.ADMIN_UIDS:
        client.send_message(entity=client.get_entity(admin),
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


def insert_message(chat_id: int, user_id: int, msg: str, date: datetime):
    if not msg:  # Not text message
        return
    utc_timestamp = int(date.timestamp())

    for i in range(10):
        try:
            session = models.Session()
            chat = models.Chat(chat_id=chat_id, user_id=user_id, text=msg, date=utc_timestamp)
            session.add(chat)
            session.commit()
            break
        except:
            session.rollback()
            send_message_to_administrators('DB write {} failed:\n{}'.format(i, traceback.format_exc()))
    find_link_to_join(session, msg)
    session.close()
    models.Session.remove()


def insert_message_local_timezone(chat_id, user_id, msg, date: datetime):
    utc_date = date.replace(tzinfo=timezone.utc)
    insert_message(chat_id, user_id, msg, utc_date)


def peer_to_internal_id(peer):
    """
    Get bot marked ID

    :param peer:
    :return:
    """
    return get_peer_id(peer, True)


def update_user_real(user_id, first_name, last_name, username, lang_code):
    """
    Update user information to database

    :param user_id:
    :param first_name:
    :param last_name:
    :param username:
    :param lang_code: Optional
    :return:
    """
    print(user_id, first_name, last_name, username, lang_code)

    session = models.Session()
    user = session.query(models.User).filter(models.User.uid == user_id).one_or_none()
    if not user:  # new user
        user = models.User(uid=user_id,
                           first_name=first_name,
                           last_name=last_name,
                           username=username,
                           lang_code=lang_code)

        session.add(user)
    else:  # existing user
        same = user.first_name == first_name and user.last_name == last_name and user.username == username
        if not same:  # information changed
            user.first_name = first_name
            user.last_name = last_name
            user.username = username
            user.lang_code = lang_code
            change = models.UsernameHistory(uid=user_id,
                                            username=username,
                                            first_name=first_name,
                                            last_name=last_name,
                                            lang_code=lang_code,
                                            date=datetime.now().timestamp()
                                            )
            session.add(change)
    try:
        session.commit()
    except:  # PRIMARY KEY CONSTRAINT
        session.rollback()
    session.close()
    models.Session.remove()


def update_group_real(chat_id, name, link):
    """
    Update group information to database

    :param chat_id: Group ID (bot marked format)
    :param name: Group Name
    :param link: Group Public Username (supergroup only)
    :return:
    """
    print(chat_id, name, link)

    session = models.Session()
    group = session.query(models.Group).filter(models.Group.gid == chat_id).one_or_none()
    if not group:  # new group
        group = models.Group(gid=chat_id, name=name, link=link)
        session.add(group)
    else:  # existing group
        same = group.name == name and group.link == link
        if not same:  # information changed
            group.name = name
            group.link = link
            change = models.GroupHistory(gid=chat_id,
                                         name=name,
                                         link=link,
                                         date=datetime.now().timestamp()
                                         )
            session.add(change)
    try:
        session.commit()
    except:  # PRIMARY KEY CONSTRAINT
        session.rollback()
    session.close()
    models.Session.remove()


user_last_changed = ExpiringDict(max_len=10000, max_age_seconds=3600)
def update_user(user_id):
    if user_id in user_last_changed:  # user should be updated at a minute basis
        return
    user = client.get_entity(user_id, force_fetch=True)  # type: User
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
    group = client.get_entity(peer, force_fetch=True)
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


class KosakaFTP(FTP):
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=5, *args, **kwargs)

    def cdp(self, directory):
        if directory != "":
            try:
                self.cwd(directory)
                print('cwd')
            except FTPError:
                print('go up')
                self.cdp("/".join(directory.split("/")[:-1]))
                print('mkd')
                self.mkd(directory)
                print('cwd1')
                self.cwd(directory)


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


def upload_ocr(buffer, path, filename) -> str:
    fullpath = '{}/{}'.format(path, filename)
    # upload to ftp server
    ftp = KosakaFTP()
    ftp.connect(**config.FTP_SERVER)
    ftp.login(**config.FTP_CREDENTIAL)
    ftp.cdp(path)
    ftp.storbinary('STOR {}'.format(filename), buffer)
    buffer.close()
    ftp.close()
    print('pic uploaded')

    # do the ocr on server
    result = 'tgpic://kosaka/{}{}'.format(config.FTP_NAME, fullpath)
    req = get(config.OCR_URL + fullpath, timeout=10)
    ocr_result = req.json()  # type: dict
    if 'body' in ocr_result.keys():
        result += '\n'
        result += ocr_result['body']
    print('pic ocred')

    return result


def download_upload_ocr(media: MessageMediaPhoto):
    buffer, path, filename = download_file(media)
    return upload_ocr(buffer, path, filename)


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


def update_handler_wrapper(update):
    try:
        update_handler(update)
    except Exception as e:
        info = 'Exception raised on PID {}, {}\n'.format(getpid(), current_thread())
        exc = traceback.format_exc()

        # special process with common exceptions
        if isinstance(e, ValueError) and 'encountered this peer before' in e.args[0]:
            exc = e.args[0]
        elif isinstance(e, AuthKeyUnregisteredError):
            exc = e.args
        print(info + exc)
        send_message_to_administrators(info + exc)


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
