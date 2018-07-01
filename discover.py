import asyncio
import re
from math import ceil
from random import sample
from logging import getLogger
from base64 import urlsafe_b64decode
from struct import unpack

from aiogram import Bot
from aiogram.types import ChatType
from aiogram.utils.exceptions import BadRequest, RetryAfter, NetworkError

from telethon.errors import InviteHashExpiredError, InviteHashInvalidError, FloodWaitError, ChannelsTooMuchError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, GetHistoryRequest
from telethon.tl.types import ChatInvite

import aiomysql.sa
import sqlalchemy

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
recent_found_links = cache.RedisExpiringSet('recent_found_links', expire=86400)
group_last_changed = cache.RedisExpiringSet('group_last_changed', expire=3600)
async def find_link_to_join(engine: aiomysql.sa.Engine, msg: str):
    public_links = set(PUBLIC_REGEX.findall(msg)).union(PUBLIC_AT_REGEX.findall(msg))
    private_links = set(INVITE_REGEX.findall(msg))

    if public_links or private_links:
        logger.info('found links. public: %s, private: %s', public_links, private_links)

    for link in public_links:
        if link in config.GROUP_BLACKLIST:  # false detection of private link
            continue
        if await recent_found_links.contains(link):
            logger.warning(f'Group @{link} is in recent found links, skip')
            continue
        await recent_found_links.add(link)
        gid, joined = await test_and_join_public_channel(engine, link)
        if joined:
            await group_last_changed.add(str(gid))

    for link in private_links:
        invite_hash = link[-22:]
        uid, gid, rand = extract_uid_gid_from_link(invite_hash)
        if gid > 1000000000:  # supergroup or channel
            gid = int('-100' + str(gid))
        else:  # normal group
            gid = -gid
        if await recent_found_links.contains(str(gid)):
            continue
        await recent_found_links.add(str(gid))

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = models.Core.Group.select().where(models.Group.gid == gid)
            records = await conn.execute(stmt)
            if records.rowcount:
                continue

        try:
            group = await senders.invoker(CheckChatInviteRequest(invite_hash))
        except (InviteHashExpiredError, InviteHashInvalidError) as e:
            report_exception()
            continue
        except FloodWaitError as e:
            logger.warning('Unable to resolve now, %r', e)
            continue
        if isinstance(group, ChatInvite) and group.participants_count > config.GROUP_MEMBER_JOIN_LIMIT:
            await send_to_admin_channel('invitation from {} (gid {}): {}, {} members\n'
                                        'Join {} with /joinprv {}'.format(
                link, gid, group.title, group.participants_count,
                'channel' if group.broadcast else 'group', link[-22:]
            ))


bot_info = cache.RedisDict('bot_info')
async def get_available_bot() -> Bot:
    all_bot = config.BOT_TOKENS
    blacklist = set()
    for k, v in await bot_info.items():
        if float(v) > get_now_timestamp():
            blacklist.add(k)

    if len(all_bot - blacklist) < 3:
        return None

    return Bot(token=sample(all_bot - blacklist, 1)[0])


CHINESE_REGEX = re.compile(r"[\u4e00-\u9fff]")
def is_chinese_message(message: str):
    return bool(CHINESE_REGEX.findall(message))


async def is_chinese_group(group, info):
    result = await senders.invoker(GetHistoryRequest(
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

    await send_to_admin_channel(
        f'''Quick Message Analysis for Group {info.title} (@{info.username})
Message Count: {all_count}, Chinese detected: {chinese_count}
Messages: {[m.message if hasattr(m, 'message') else '' for m in result.messages]}
@{info.username} Result: {chinese_count}/{all_count}
'''
    )
    return chinese_count > ceil(all_count / 10)


async def test_and_join_public_channel(engine: aiomysql.sa.Engine, link) -> (int, bool):
    """
    :param session: SQLAlchemy session
    :param link: public link (like im91yun)
    :return: bool: if joined the group/channel
    """
    gid = None
    joined = False
    fetcher = await get_available_bot()
    if not fetcher:
        return None, False
    try:
        await asyncio.sleep(0.1)
        info = await fetcher.get_chat('@' + link)
    except (BadRequest, RetryAfter, NetworkError) as e:
        report_exception()
        if isinstance(e, RetryAfter):
            logger.warning('bot retry after %s seconds', e.timeout)
            await bot_info.set(fetcher._Bot__token, get_now_timestamp() + e.timeout)
        return None, False

    if info.type not in [ChatType.SUPER_GROUP, ChatType.CHANNEL]:
        return None, False

    gid = info.id
    async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
        stmt = models.Core.Group.select().where(models.Group.gid == gid)
        records = await conn.execute(stmt)
        group_exist = bool(records.rowcount)
    if group_exist:
        logger.warning(f'Group @{link} is already in our database, skip')
        return gid, False

    link = info.username if hasattr(info, 'username') else None
    count = await fetcher.get_chat_members_count('@' + link)
    if count < config.GROUP_MEMBER_JOIN_LIMIT:
        logger.warning(f'Group @{link} has {count} < {config.GROUP_MEMBER_JOIN_LIMIT} members, skip')
        return gid, False

    try:
        group = await senders.invoker.get_input_entity(link)  # type: InputChannel
    except FloodWaitError as e:
        logger.warning('Get group via username flooded. %r', e)
        return gid, False
    if (info.title and is_chinese_message(info.title)) or \
       (info.description and is_chinese_message(info.description)) or \
       await is_chinese_group(group, info):  # we do it after logging it to our system
        global_count = cache.RedisDict('global_count')
        try:
            await senders.invoker(JoinChannelRequest(group))
            await global_count.set('full', '0')
        except ChannelsTooMuchError:
            if await global_count['full'] == '0':
                await send_to_admin_channel('Too many groups! It\'s time to sign up for a new account')
                await global_count.set('full', '1')
            return gid, False
        await send_to_admin_channel(f'joined public {info.type}: {tg_html_entity(info.title)}(@{link})\n'
                                       f'members: {count}\n'
                              )
        joined = True

    async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
        stmt = models.Core.Group.insert().values(id=gid,
                                                 name=info.title,
                                                 link=link,
                                                 master=senders.invoker.conf['uid'] if joined else None)
        await conn.execute(stmt)
        await conn.execute('COMMIT')

    return gid, joined


async def find_link_enqueue(msg: str):
    from workers import FindLinkWorker
    if await FindLinkWorker.queue.qsize() > 50:
        await send_to_admin_channel('Find link queue full, worker dead?')
        FindLinkWorker().start()
        return
    if msg:
        await FindLinkWorker.queue.put(msg)
