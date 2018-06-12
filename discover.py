import re
from math import ceil
from random import sample
from time import sleep
from logging import getLogger
from base64 import urlsafe_b64decode
from struct import unpack

from telegram import Bot
from telegram.error import BadRequest, RetryAfter, TimedOut

from telethon.errors import InviteHashExpiredError, InviteHashInvalidError, FloodWaitError, ChannelsTooMuchError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, GetHistoryRequest
from telethon.tl.types import ChatInvite

import cache
import config
import models
import senders
from utils import report_exception, send_to_admin_channel, \
    get_now_timestamp, tg_html_entity

logger = getLogger(__name__)


def extract_uid_gid_from_link(link: str):
    link = link[-22:]
    link_bytes = link.encode('utf-8') + b'=='
    detail = urlsafe_b64decode(link_bytes)
    # join chat link format: uid(u32be), gid(u32be), random(u64be)
    return unpack('>LLQ', detail)


PUBLIC_REGEX = re.compile(r"t(?:elegram)?\.me/([a-zA-Z][\w\d]{3,30}[a-zA-Z\d])")
PUBLIC_AT_REGEX = re.compile(r"@([a-zA-Z][\w\d]{3,30}[a-zA-Z\d])")
INVITE_REGEX = re.compile(r'(t(?:elegram)?\.me/joinchat/[a-zA-Z0-9_-]{22})')
recent_found_links = cache.RedisExpiringSet('recent_found_links', expire=3600)
group_last_changed = cache.RedisExpiringSet('group_last_changed', expire=3600)
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
        uid, gid, rand = extract_uid_gid_from_link(invite_hash)
        if str(gid) in recent_found_links:
            continue
        recent_found_links.add(str(gid))
        group_exist = session.query(models.Group).filter(models.Group.gid == gid).one_or_none()
        if group_exist:
            continue
        try:
            group = senders.invoker.invoke(CheckChatInviteRequest(invite_hash))
        except (InviteHashExpiredError, InviteHashInvalidError) as e:
            report_exception()
            continue
        except FloodWaitError as e:
            logger.warning('Unable to resolve now, %r', e)
            continue
        if isinstance(group, ChatInvite) and group.participants_count > config.GROUP_MEMBER_JOIN_LIMIT:
            send_to_admin_channel('invitation from {}: {}, {} members\n'
                                           'Join {} with /joinprv {}'.format(
                    link, group.title, group.participants_count, 'channel' if group.broadcast else 'group', link[-22:]
                )
            )


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

    send_to_admin_channel(
        f'''Quick Message Analysis for Group {info.title} (@{info.username})
Message Count: {all_count}, Chinese detected: {chinese_count}
Messages: {[m.message if hasattr(m, 'message') else '' for m in result.messages]}
@{info.username} Result: {chinese_count}/{all_count}
'''
    )
    return chinese_count > ceil(all_count / 10)


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
                global_count = cache.RedisDict('global_count')
                try:
                    result = senders.invoker.invoke(JoinChannelRequest(group))
                    global_count['full'] = '0'
                except ChannelsTooMuchError:
                    if global_count['full'] == '0':
                        send_to_admin_channel('Too many groups! It\'s time to sign up for a new account')
                    global_count['full'] = '1'
                    return gid, False
                send_to_admin_channel(f'joined public {info.type}: {tg_html_entity(info.title)}(@{link})\n'
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


def find_link_enqueue(msg: str):
    from workers import FindLinkWorker
    if FindLinkWorker.queue.qsize() > 50:
        send_to_admin_channel('Find link queue full, worker dead?')
        FindLinkWorker().start()
    else:
        FindLinkWorker.queue.put(msg)
