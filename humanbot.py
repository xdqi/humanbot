from os import system, popen, getpid
from datetime import datetime
from ftplib import FTP, Error as FTPError
from io import BytesIO
from requests import get

from expiringdict import ExpiringDict

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import \
    UpdateNewChannelMessage, UpdateShortMessage, UpdateShortChatMessage, UpdateNewMessage, \
    UpdateUserStatus, UpdateUserName, Message, MessageService, MessageMediaPhoto, MessageMediaDocument, \
    MessageActionChatEditTitle, \
    PeerUser, InputUser, User, Chat, ChatFull, Channel, ChannelFull, \
    Photo, PhotoSize, FileLocation
from telethon.utils import get_input_user, get_peer_id

import models
import config


def insert_message(chat_id: int, user_id: int, msg: str, date: datetime):
    if not msg:  # Not text message
        return
    utc_timestamp = int(date.timestamp())

    session = models.Session()
    chat = models.Chat(chat_id=chat_id, user_id=user_id, text=msg, date=utc_timestamp)
    session.add(chat)
    session.commit()
    session.close()
    models.Session.remove()


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
    users = session.query(models.User).filter(models.User.uid == user_id)
    if not users:  # new user
        user = models.User(uid=user_id,
                           first_name=first_name,
                           last_name=last_name,
                           username=username,
                           lang_code=lang_code)

        session.add(user)
    else:  # existing user
        user = users[0]
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
    session.commit()
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
    groups = session.query(models.Group).filter(models.Group.gid == chat_id)
    if not groups:  # new group
        group = models.Group(gid=chat_id, name=name, link=link)
        session.add(group)
    else:  # existing group
        group = groups[0]
        same = group.name == name and group.link == link
        if not same:  # information changed
            group.name = name
            group.link = link
            change = models.UsernameHistory(gid=chat_id,
                                            name=name,
                                            link=link,
                                            date=datetime.now().timestamp()
                                            )
            session.add(change)
    session.commit()
    session.close()
    models.Session.remove()


user_last_changed = ExpiringDict(max_len=10000, max_age_seconds=3600)
def update_user(user_id):
    if user_id in user_last_changed:  # user should be updated at a minute basis
        return
    user_last_changed[user_id] = True
    input_user = get_input_user(PeerUser(user_id))  # type: InputUser
    user = client.get_entity(input_user, force_fetch=True)  # type: User
    update_user_real(user_id, user.first_name, user.last_name, user.username, user.lang_code)


group_last_changed = ExpiringDict(max_len=1000, max_age_seconds=300)
def update_group(chat_id: int, title: str=None):
    """
    Try to update group information

    :param chat_id: Chat ID (DO NOT PASS BOT MARKED FORMAT)
    :param title: New group title (optional)
    :return: None
    """
    if chat_id in group_last_changed:  # user should be updated at a minute basis
        return
    del group_last_changed[chat_id]
    group_last_changed[chat_id] = True
    group = client.get_entity(chat_id, force_fetch=True)
    if isinstance(group, (Chat, ChatFull)):
        update_group_real(peer_to_internal_id(chat_id), title or group.title, None)
    elif isinstance(group, (Channel, ChannelFull)):
        update_group_real(peer_to_internal_id(chat_id), title or group.title, group.username)


def update_chat_generic(chat_id: int):
    input_entity = client.get_input_entity(chat_id)
    if isinstance(input_entity, InputUser):
        update_user(chat_id)
    else:
        update_group(chat_id)


def update_group_title(chat_id, update: MessageActionChatEditTitle):
    name = update.title
    del group_last_changed[chat_id]
    update_group(chat_id, name)


class KosakaFTP(FTP):
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


def ocr(media: MessageMediaPhoto) -> str:
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
    result = 'tgpic://kosaka/{}/{}'.format(config.FTP_NAME, fullpath)
    req = get(config.OCR_URL + fullpath)
    ocr_result = req.json()  # type: dict
    if 'body' in ocr_result.keys():
        result += '\n'
        result += ocr_result['body']
    print('pic ocred')

    return result


def update_message(update: Message):
    if isinstance(update.to_id, PeerUser) and update.to_id.user_id == config.MY_UID:  # private message
        chat = update.from_id
    else:
        chat = peer_to_internal_id(update.to_id)
    if update.message:
        insert_message(chat, update.from_id, update.message, update.date)
    elif isinstance(update.media, (MessageMediaDocument, MessageMediaPhoto)):
        text = update.media.caption
        if isinstance(update.media, MessageMediaPhoto):
            result = ocr(update.media)
            text = result + '\n' + text
        insert_message(chat, update.from_id, text, update.date)

    update_chat_generic(chat)


def update_message_from_chat(update: UpdateShortChatMessage):
    insert_message(-update.chat_id, update.from_id, update.message, update.date)
    update_group(update.chat_id)


def update_message_from_user(update: UpdateShortMessage):
    insert_message(update.user_id, update.user_id, update.message, update.date)
    update_user(update.user_id)
    if update.user_id in config.ADMIN_UIDS:
        output = ''
        if update.message.startswith('/exec'):
            command = update.message[5:]
            print('executing command', command)
            with popen(command, 'r') as f:
                output = f.read()
        elif update.message.startswith('/py'):
            script = update.message[3:]
            print('evaluating script', script)
            output = repr(eval(update.message[3:]))
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
    print('pid', getpid(), update)
    if isinstance(update, (UpdateNewChannelMessage, UpdateNewMessage)):  # message from group/user
        if isinstance(update.message, Message):  # message
            update_message(update.message)
        elif isinstance(update.message, MessageService):  # action
            if isinstance(update.message.action, MessageActionChatEditTitle):
                update_group_title(update.message.to_id, update.message.action)

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

    client.add_update_handler(update_handler)
    client.idle()
    client.disconnect()


if __name__ == '__main__':
    main()
