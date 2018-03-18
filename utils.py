import re
import traceback
from datetime import datetime
from ftplib import FTP, Error as FTPError
from logging import getLogger
from io import BytesIO
from threading import current_thread
from math import ceil
from typing import List
from os import makedirs

from requests import get, ReadTimeout
from raven import Client as RavenClient

from telegram import Bot, Update, Message
from telegram.ext import CommandHandler, Filters
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Channel

import config
from senders import bot, client

logger = getLogger(__name__)
raven_client = RavenClient(config.RAVEN_DSN)


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


def upload_generic(buffer, root, path, filename) -> str:
    url_path = '{}/{}'.format(path, filename)
    # copy to local network drive
    makedirs('{}{}'.format(root, path), exist_ok=True)
    with open('{}{}'.format(root, url_path), 'wb') as f:
        f.write(buffer.read())
        buffer.close()
    logger.info('File uploaded to %s', url_path)
    return url_path


def upload_pic(buffer, path, filename) -> str:
    return upload_generic(buffer, config.PIC_PATH, path, filename)


def upload_log(buffer, path, filename) -> str:
    return upload_generic(buffer, config.LOG_PATH, path, filename)


def ocr(fullpath: str):
    # do the ocr on server
    result = 'tgpic://kosaka/{}{}'.format(config.FTP_NAME, fullpath)
    req = wget_retry(config.OCR_URL + fullpath)
    ocr_result = req.json()  # type: dict
    if 'body' in ocr_result.keys():
        result += '\n'
        result += ocr_result['body']
    logger.info('pic ocred\n%s', result)

    return result


def get_now_timestamp() -> int:
    return int(datetime.now().timestamp())


def send_message_to_administrators(msg: str):
    logger.info('Sending to administrators: \n%s', msg)
    if len(msg.encode('utf-8')) > 500 or len(msg.splitlines()) > 10:
        buffer = BytesIO(msg.encode('utf-8'))
        now = datetime.now()
        date = now.strftime('%y/%m')
        timestamp = now.timestamp()
        path = '/{}'.format(date)
        thread_name = current_thread().name
        filename = '{}-{}.txt'.format(thread_name, timestamp)
        exception = msg.splitlines()[-1]
        url_path = upload_log(buffer, path, filename)

        msg = 'Long message: ... {}\nURL: {}{}'.format(
            exception,
            config.LOG_URL,
            url_path
        )
    bot.send_message(chat_id=config.ADMIN_CHANNEL,
                     text='```{}```'.format(msg.strip()),
                     parse_mode='markdown',
                     disable_web_page_preview=False)


CHINESE_REGEX = re.compile(r"\u4e00-\u9fff")
def is_chinese_message(message: str):
    return bool(CHINESE_REGEX.findall(message))


def is_chinese_group(group: Channel):
    result = client.invoke(GetHistoryRequest(
        peer=group,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=100,
        max_id=0,
        min_id=0,
        hash=0,
    ))
    # for 100 messages, at least 10 should be chinese text
    return sum(is_chinese_message(m.message) > 0 for m in result.messages) > ceil(len(result.messages) / 10)


def report_exception():
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
        from_user = message.from_user  # type: User
        result = self.real_callback(bot, update, text)
        if result:  # we allows no message
            bot.send_message(chat_id=message.chat_id,
                             text=result,
                             parse_mode='HTML',
                             reply_to_message_id=message.message_id
                             )


def show_commands_handler(bot, update, text):
    return 'Commands:' + '\n'.join('/%s' % c for c in BasicAdminCommandHandler.commands)
