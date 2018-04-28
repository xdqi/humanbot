import re
import traceback
from datetime import datetime, timedelta
from logging import getLogger
from io import BytesIO
from threading import current_thread
from math import ceil
from time import sleep
from os import makedirs
from random import sample, randint

from requests import get, ReadTimeout
from raven import Client as RavenClient
from raven.transport import ThreadedRequestsHTTPTransport
from minio import Minio

from telegram import Bot, Update, Message
from telegram.ext import CommandHandler, Filters
from telegram.error import TelegramError, BadRequest, RetryAfter, TimedOut
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Channel, Chat, InputChannel
from telethon.errors.rpc_error_list import ChannelsTooMuchError, FloodWaitError
from telethon.utils import get_peer_id, resolve_id

import config
import cache
import models
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

        msg = 'Long message: ... {}\nURL: {}{}\nTime: {}'.format(
            exception,
            config.LOG_URL,
            url_path,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
    try:
        senders.bot.send_message(chat_id=config.ADMIN_CHANNEL,
                                 text=msg.strip(),
                                 parse_mode='HTML',
                                 disable_web_page_preview=False)
    except TelegramError:
        report_exception()


CHINESE_REGEX = re.compile(r"[\u4e00-\u9fff]")
def is_chinese_message(message: str):
    return bool(CHINESE_REGEX.findall(message))


def is_chinese_group(group, info):
    result = senders.invoker.invoke(GetHistoryRequest(
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
    chinese_count = sum(is_chinese_message(m.message) > 0 if hasattr(m, 'message') else False for m in result.messages)
    all_count = len(result.messages)

    send_message_to_administrators(
        f'''Quick Message Analysis for Group {info.title} (@{info.username})
Message Count: {all_count}, Chinese detected: {chinese_count}
Messages: {[m.message if hasattr(m, 'message') else '' for m in result.messages]}
@{info.username} Result: {chinese_count}/{all_count}
'''
    )
    return chinese_count > ceil(all_count / 10)


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


bot_info = cache.RedisDict('bot_info')
def get_available_bot() -> Bot:
    all_bot = config.BOT_TOKENS
    blacklist = set()
    for k, v in bot_info.items():
        if float(v) > get_now_timestamp():
            blacklist.add(k)

    if len(all_bot - blacklist) < 3:
        return None

    return Bot(token=sample(all_bot - blacklist, 1)[0])


def test_and_join_public_channel(session, link) -> (int, bool):
    """
    :param session: SQLAlchemy session
    :param link: public link (like im91yun)
    :return: bool: if joined the group/channel
    """
    gid = None
    joined = False
    fetcher = get_available_bot()
    if not fetcher:
        return None, False
    try:
        sleep(0.1)
        info = fetcher.get_chat('@' + link)
    except (BadRequest, RetryAfter, TimedOut) as e:
        report_exception()
        if isinstance(e, RetryAfter):
            logger.warning('bot retry after %s seconds', e.retry_after)
            bot_info[fetcher.token] = get_now_timestamp() + e.retry_after
        return None, False
    if info.type in ['supergroup', 'channel']:
        gid = info.id
        group_exist = session.query(models.Group).filter(models.Group.gid == gid).one_or_none()
        logger.warning(f'Group @{link} is already in our database, skip')
        if not group_exist:
            link = info.username if hasattr(info, 'username') else None
            count = fetcher.get_chat_members_count('@' + link)
            if count < config.GROUP_MEMBER_JOIN_LIMIT:
                logger.warning(f'Group @{link} has {count} < {config.GROUP_MEMBER_JOIN_LIMIT} members, skip')
                return gid, False

            try:
                group = senders.invoker.get_input_entity(link)  # type: InputChannel
            except FloodWaitError as e:
                logger.warning('Get group via username flooded. %r', e)
                return gid, False
            if (info.title and is_chinese_message(info.title)) or \
               (info.description and is_chinese_message(info.description)) or \
               is_chinese_group(group, info):  # we do it after logging it to our system
                try:
                    result = senders.invoker.invoke(JoinChannelRequest(group))
                except ChannelsTooMuchError:
                    logger.error('Too many groups! It\'s time to sign up for a new account')
                    return gid, False
                send_message_to_administrators(f'joined public {info.type}: {tg_html_entity(info.title)}(@{link})\n'
                                               f'members: {count}\n'
                                               )
                joined = True

            try:
                new_group = models.Group(gid=gid, name=info.title, link=link,
                                         master=senders.invoker.conf['uid'] if joined else None)
                session.add(new_group)
                session.commit()
            except:
                report_exception()
                session.rollback()

    return gid, joined


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


def get_next_online_time():
    now = datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    result = midnight + timedelta(days=1, hours=randint(10, 23), minutes=randint(0, 59), seconds=randint(0, 59))
    return int(result.timestamp())


def need_to_be_online():
    global_count = cache.RedisDict('global_count')
    if not global_count['online_time']:
        global_count['online_time'] = get_next_online_time()
        return False
    if int(global_count['online_time']) - get_now_timestamp() < 600:
        global_count['online_time'] = get_next_online_time()
        return True
    return False
