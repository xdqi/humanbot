import traceback
from datetime import datetime, timedelta
from logging import getLogger
from io import BytesIO
from threading import current_thread
from os import makedirs
from random import randint

from requests import get, ReadTimeout
from raven import Client as RavenClient
from raven.transport import ThreadedRequestsHTTPTransport
from minio import Minio

from telegram import Bot, Update, Message
from telegram.ext import CommandHandler, Filters
from telegram.error import TelegramError
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from telethon.utils import get_peer_id, resolve_id

import config
import cache
import senders

logger = getLogger(__name__)
raven_client = RavenClient(config.RAVEN_DSN, transport=ThreadedRequestsHTTPTransport)
minio_client = Minio(endpoint=config.MINIO_SERVER,
                     access_key=config.MINIO_ACCESS_KEY,
                     secret_key=config.MINIO_SECRET_KEY,
                     secure=config.MINIO_SECURE)


class FakeResponse():
    def json(self):
        return {}


def wget_retry(url, remaining_retry=1):
    if remaining_retry == 0:
        traceback.print_exc()
        return FakeResponse()
    try:
        return get(url, timeout=10)
    except ReadTimeout:
        return wget_retry(url, remaining_retry - 1)


def upload_local(buffer: BytesIO, root, path, filename) -> str:
    url_path = '{}/{}'.format(path, filename)
    # copy to local network drive
    makedirs('{}{}'.format(root, path), exist_ok=True)
    with open('{}{}'.format(root, url_path), 'wb') as f:
        f.write(buffer.read())
        buffer.close()
    logger.info('File uploaded to %s', url_path)
    return url_path


def upload_minio(buffer: BytesIO, path, filename) -> str:
    url_path = '{}/{}'.format(path, filename)
    minio_client.put_object(config.MINIO_BUCKET, url_path, buffer, buffer.getbuffer().nbytes)
    return url_path


def upload_pic(buffer, path, filename) -> str:
    return upload_minio(buffer, path, filename)


def upload_log(buffer, path, filename) -> str:
    return upload_local(buffer, config.LOG_PATH, path, filename)


def ocr(fullpath: str):
    # do the ocr on server
    result = 'tgpic://kosaka/{}/{}'.format(config.FTP_NAME, fullpath)
    req = wget_retry(config.OCR_URL + fullpath)
    ocr_result = req.json()  # type: dict
    if 'body' in ocr_result.keys():
        result += '\n'
        result += ocr_result['body']
    logger.info('pic ocred\n%s', result)

    return result


def get_now_timestamp() -> int:
    return int(datetime.now().timestamp())


def tg_html_entity(s: str) -> str:
    s = s.replace('&', '&amp;')
    s = s.replace('<', '&lt;')
    s = s.replace('>', '&gt;')
    return s


def send_to(chat: int, msg: str, strip: bool=True):
    logger.info('Sending to administrators: \n%s', msg)
    if strip and len(msg.encode('utf-8')) > 500 or len(msg.splitlines()) > 10:
        buffer = BytesIO(msg.encode('utf-8'))
        now = datetime.now()
        date = now.strftime('%y/%m/%d')
        timestamp = now.timestamp()
        path = '/{}'.format(date)
        thread_name = current_thread().name
        filename = '{}-{}.txt'.format(thread_name, timestamp)
        exception = msg.splitlines()[-1]
        url_path = upload_log(buffer, path, filename)

        msg = 'Long message: ... {}\nURL: {}{}\nTime: {}'.format(
            exception,
            config.LOG_URL,
            url_path,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
    try:
        senders.bot.send_message(chat_id=chat,
                                 text=msg.strip(),
                                 parse_mode='HTML',
                                 disable_web_page_preview=False)
    except TelegramError:
        report_exception()


def send_to_admin_channel(msg: str):
    send_to(config.ADMIN_CHANNEL, msg)


def send_to_admin_group(msg: str):
    send_to(config.ADMIN_GROUP, msg)


def report_exception():
    return
    raven_client.captureException()


class BasicAdminCommandHandler(CommandHandler):
    commands = []

    def __init__(self, command: str, callback: callable):
        super().__init__(command=command,
                         callback=callback,
                         filters=Filters.chat(chat_id=config.ADMIN_UIDS),
                         )
        BasicAdminCommandHandler.commands.append(command)


class AdminCommandHandler(BasicAdminCommandHandler):
    def __init__(self, command: str, callback: callable):
        super().__init__(command=command,
                         callback=self.wrapper,
                         )
        self.real_callback = callback

    def wrapper(self, bot: Bot, update: Update):
        message = update.message  # type: Message
        text = message.text[1 + len(self.command[0]):].strip()  # command is an array after CommandHandler
        result = self.real_callback(bot, update, text)
        if result:  # we allows no message
            bot.send_message(chat_id=message.chat_id,
                             text=result,
                             parse_mode='HTML',
                             reply_to_message_id=message.message_id
                             )


def show_commands_handler(bot, update, text):
    return 'Commands:' + '\n'.join('/%s' % c for c in BasicAdminCommandHandler.commands)


def peer_to_internal_id(peer):
    """
    Get bot marked ID

    :param peer:
    :return:
    """
    return get_peer_id(peer)


def internal_id_to_peer(marked_id: int):
    i, t = resolve_id(marked_id)
    return t(i)


def get_random_time(hour):
    now = datetime.now()
    time = now.replace(hour=randint(hour - 1, hour + 1), minute=randint(0, 59), second=randint(0, 59))
    return int(time.timestamp())


def need_to_be_online():
    global_count = cache.RedisDict('global_count')
    today = datetime.now().strftime('%Y-%m-%d')

    if global_count['today'] != today:
        global_count['today'] = today
        global_count['online_time'] = get_random_time(config.ONLINE_HOUR)
        global_count['offline_time'] = get_random_time(config.OFFLINE_HOUR)

    if int(global_count['online_time']) < get_now_timestamp() < int(global_count['offline_time']) and randint(0, 10) == 5:
        return True
    return False


def get_photo_address(client: TelegramClient, media: MessageMediaPhoto):
    # get largest photo
    original = media.photo.sizes[-1]  # type: PhotoSize
    location = original.location  # type: FileLocation
    now = datetime.now()
    return repr(dict(
        location=location.to_dict(),
        client=client.get_me(input_peer=True).user_id,
        path='{}/{}'.format(now.year, now.month),
        filename='{}-{}_{}_{}.jpg'.format(get_now_timestamp(),
                                          location.dc_id,
                                          location.volume_id,
                                          location.local_id)
    ))
