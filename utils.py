import re
import traceback
from datetime import datetime
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
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Channel, Chat, InputChannel
from telethon.utils import get_peer_id

import config
import models
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


def tg_html_entity(s: str) -> str:
    s = s.replace('&', '&amp;')
    s = s.replace('<', '&lt;')
    s = s.replace('>', '&gt;')
    return s


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
                     text=msg.strip(),
                     parse_mode='HTML',
                     disable_web_page_preview=False)


CHINESE_REGEX = re.compile(r"[\u4e00-\u9fff]")
def is_chinese_message(message: str):
    return bool(CHINESE_REGEX.findall(message))


def is_chinese_group(group):
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
    chinese_count = sum(is_chinese_message(m.message) > 0 for m in result.messages)
    all_count = len(result.messages)

    send_message_to_administrators(
        f'''Quick Message Analysis for Group {group.title} (@{group.username})
Message Count: {all_count}, Chinese detected: {chinese_count}
Messages: {[m.message for m in result.messages]}
@{group.username} Result: {chinese_count}/{all_count}
'''
    )
    return chinese_count > ceil(all_count / 10)


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


def test_and_join_public_channel(session, link) -> (int, bool):
    """
    :param session: SQLAlchemy session
    :param link: public link (like im91yun)
    :return: bool: if joined the group/channel
    """
    info = bot.get_chat('@' + link)
    gid = None
    joined = False
    if info.type in ['supergroup', 'channel']:
        gid = info.id
        group_exist = session.query(models.Group).filter(models.Group.gid == gid).one_or_none()
        logger.warning(f'Group @{link} is already in our database, skip')
        if not group_exist:
            link = info.username if hasattr(info, 'username') else None
            count = bot.get_chat_members_count('@' + link)
            if count < config.GROUP_MEMBER_JOIN_LIMIT:
                logger.warning(f'Group @{link} has {count} < {config.GROUP_MEMBER_JOIN_LIMIT} members, skip')
                return gid, False
            new_group = models.Group(gid=gid, name=info.title, link=link)
            session.add(new_group)

            group = client.get_input_entity(link)  # type: InputChannel
            if is_chinese_message(info.title) or \
               is_chinese_message(info.description) or \
               is_chinese_group(group):  # we do it after logging it to our system
                client.invoke(JoinChannelRequest(group))
                send_message_to_administrators(f'joined public {info.type}: {tg_html_entity(info.title)}(@{link})\n'
                                               f'members: {count}\n'
                                               f'creation date {info.date}'
                                               )
            joined = True

    return gid, joined


def peer_to_internal_id(peer):
    """
    Get bot marked ID

    :param peer:
    :return:
    """
    return get_peer_id(peer)
