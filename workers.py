import asyncio
from io import BytesIO
from threading import Thread
import traceback
from logging import getLogger
from cProfile import Profile
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.tl.types import Message, MessageService, InputFileLocation, User, MessageMediaPhoto
from telethon.extensions import markdown
from telethon.errors import AuthKeyUnregisteredError, FloodWaitError, ChannelPrivateError, \
    RpcCallFailError
from aiogram import Bot

import aiomysql.sa
import sqlalchemy

import cache
import config
import models
from senders import clients
from utils import get_now_timestamp, report_exception, upload_pic, ocr, get_photo_address, from_json, to_json, \
    send_to_admin_channel, noblock, block

logger = getLogger(__name__)


class WorkProperties(type):
    def __new__(mcs, class_name, class_bases, class_dict):
        name = class_dict['name']
        new_class_dict = class_dict.copy()
        new_class_dict['status'] = cache.RedisDict(name + '_worker_status')
        new_class_dict['queue'] = cache.RedisQueue(name + '_queue')
        return type.__new__(mcs, class_name, class_bases, new_class_dict)


class Worker(Thread, metaclass=WorkProperties):
    """
    Deprecated thread based worker, for reference only
    """
    name = ''
    status = None  # type: cache.RedisDict
    queue = None  # type: cache.RedisQueue

    def __init__(self):
        super().__init__(name=self.name)

    def run(self):
        logger.info('%s worker has started', self.name)
        session = models.Session()

        while True:
            try:
                message = self.queue.get()  # type: str
                if message is None:
                    import time
                    time.sleep(0.01)
                    continue
                self.handler(session, message)
                session.commit()
                block(self.queue.task_done())
                self.status['last'] = get_now_timestamp()
                self.status['size'] = block(self.queue.qsize())
            except KeyboardInterrupt:
                block(self.queue.put(message))
                break
            except:
                traceback.print_exc()
                report_exception()
                session.rollback()
                block(self.queue.put(message))

        session.close()
        models.Session.remove()

    def start(self, count: int=1):
        if count > 1:
            type(self)().start(count - 1)
        super().start()

    def handler(self, session, message: str):
        raise NotImplementedError

    @classmethod
    async def stat(cls):
        return '{} worker: {} seconds ago, size {}\n'.format(
            cls.name, get_now_timestamp() - int(await cls.status['last']), await cls.queue.qsize())


class CoroutineWorker(metaclass=WorkProperties):
    name = ''
    status = None  # type: cache.RedisDict
    queue = None  # type: cache.RedisQueue

    def __init__(self):
        pass

    async def __call__(self, *args, **kwargs):
        await self.run()

    async def run(self):
        logger.info('%s worker has started', self.name)
        engine = await aiomysql.create_pool(**config.MYSQL_CONFIG, db=config.MYSQL_DATABASE, charset='utf8mb4')

        while True:
            try:
                message = await self.queue.get()  # type: str
                if message is None:
                    await asyncio.sleep(0.01)
                    continue
                await self.handler(engine, message)
                await self.queue.task_done()
                await self.status.set('last', get_now_timestamp())
                await self.status.set('size', await self.queue.qsize())
            except KeyboardInterrupt:
                await self.queue.put(message)
                break
            except:
                traceback.print_exc()
                report_exception()
                await self.queue.put(message)

    def start(self, count: int=1):
        for _ in range(count):
            noblock(type(self)()())

    async def handler(self, engine, message: str):
        raise NotImplementedError

    @classmethod
    async def stat(cls):
        return '{} worker: {} seconds ago, size {}\n'.format(
            cls.name, get_now_timestamp() - int(await cls.status.getitem('last')), await cls.queue.qsize())


class MessageInsertWorker(Worker):
    name = 'insert'

    def handler(self, session, message: str):
        chat = models.ChatNew(**from_json(message))
        session.add(chat)
        session.commit()
        if chat.text.startswith(config.OCR_HINT):
            session.refresh(chat)
            OcrWorker.queue.put(str(chat.id))


class MessageMarkWorker(Worker):
    name = 'mark'

    def handler(self, session, message: str):
        request_changes = from_json(message)  # type: dict # {'chat_id': 114, 'message_id': 514}
        count = session.query(models.ChatNew).filter(
            models.ChatNew.chat_id == request_changes['chat_id'],
            models.ChatNew.message_id == request_changes['message_id']
        ).count()
        if not count:
            request_changes['tries'] = request_changes.get('tries', 0) + 1
            if request_changes['tries'] < 2:
                self.queue.put(to_json(request_changes))
            return

        session.query(models.ChatNew).filter(
            models.ChatNew.chat_id == request_changes['chat_id'],
            models.ChatNew.message_id == request_changes['message_id']
        ).update({
            models.ChatNew.flag: models.ChatNew.flag.op('|')(models.ChatFlag.deleted)
        }, synchronize_session='fetch')


class OcrWorker(CoroutineWorker):
    name = 'ocr'

    async def handler(self, engine: aiomysql.sa.Engine, message: str):
        record_id = int(message)

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = models.Core.ChatNew.select().where(models.ChatNew.id == record_id)
            records = await conn.execute(stmt)
            if not records.rowcount:
                return
            row = await records.fetchone()
            record_text = row.text

        hint, info_text, text = record_text.split('\n', maxsplit=2)

        info = from_json(info_text)
        client = clients[info['client']]
        buffer = BytesIO()

        if isinstance(client, TelegramClient):
            location_info = info['location']
            del location_info['_']
            del location_info['dc_id']
            location = InputFileLocation(**location_info)

            try:
                await client.download_file(location, buffer)
            except AuthKeyUnregisteredError as e:
                report_exception()
                logger.warning('download picture auth key unregistered error %r', e)
                return
        elif isinstance(client, Bot):
            file_id = info['file_id']

            file = await client.get_file(file_id)
            await file.download(destination=buffer)

        buffer.seek(0)
        logger.info('pic downloaded')
        full_path = await upload_pic(buffer, info['path'], info['filename'])
        result = await ocr(full_path)
        logger.info('ocr complete')

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = models.Core.ChatNew.\
                update().\
                where(models.ChatNew.id == record_id).\
                values(text=result + '\n' + text)
            await conn.execute(stmt)


class EntityUpdateWorker(Worker):
    name = 'entity'

    def handler(self, session, message: str):
        info = from_json(message)
        entity_type = info['type']
        del info['type']
        if entity_type == 'user':
            models.update_user(session=session, **info['user'])
        if entity_type == 'group':
            models.update_group(session=session, **info['group'])


class FindLinkWorker(CoroutineWorker):
    name = 'find_link'

    async def handler(self, engine, message):
        from discover import find_link_to_join
        await find_link_to_join(engine, message)


class FetchHistoryWorker(CoroutineWorker):
    name = 'history'

    async def handler(self, engine: aiomysql.sa.Engine, message: str):
        info = from_json(message)
        gid = info['gid']

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = sqlalchemy.select([
                models.Group.gid,
                models.Group.master,
                models.Group.name,
                models.Group.link,
                sqlalchemy.sql.func.min(models.ChatNew.message_id).label('min_message_id')
            ]).where(
                sqlalchemy.and_(models.Group.gid == gid, models.Group.gid == models.ChatNew.chat_id)
            ).group_by(models.Group.gid)
            records = await conn.execute(stmt)
            if not records.rowcount:
                await send_to_admin_channel('fetch: No message id detected or group not joined ever before for group'
                                            f'{gid}')
                return
            row = await records.fetchone()

        gid_, master, name, link, self.first = row
        client = clients[master]

        if isinstance(client, Bot):
            await send_to_admin_channel(f'Group {name}(@{link}) is managed by a bot ({master}), '
                                        f'cannot fetch information')
            return
        # profile = Profile()
        # profile.enable()
        while True:
            try:
                prev = self.first
                await self.fetch(client, gid)
                if prev == self.first:  # no new messages
                    del self.status[gid]
                    await send_to_admin_channel(f'Group {name}(@{link}) all fetched by {master}, last message id is {prev}')

                    break
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except ChannelPrivateError as e:
                await send_to_admin_channel(f'fetch worker failed: group {gid} (managed by {master}) kicked us')
                break
            except KeyboardInterrupt:
                break
            except RpcCallFailError:
                continue
            except:
                await send_to_admin_channel(traceback.format_exc() + '\nfetch worker unknown exception')

        # profile.disable()
        # profile.dump_stats('normal.profile')

    async def fetch(self, client, gid):
        for msg in await client.iter_messages(entity=gid,
                                              limit=None,
                                              offset_id=self.first,
                                              max_id=self.first,
                                              wait_time=0
                                              ):  # type: Message
            self.first = msg.id
            before = datetime.now().timestamp()
            await self.save(client, gid, msg)
            after = datetime.now().timestamp()
            await asyncio.sleep(0.01 - (after - before))

    async def save(self, client, gid, msg: Message):
        if True:
            if isinstance(msg, MessageService):
                return
            text = markdown.unparse(msg.message, msg.entities)

            if isinstance(msg.media, MessageMediaPhoto):
                result = await get_photo_address(client, msg.media)
                text = config.OCR_HISTORY_HINT + '\n' + result + '\n' + text

            await models.insert_message_local_timezone(gid, msg.id, msg.from_id, text, msg.date)
            if msg.input_sender is not None:  # type: User
                await models.update_user_real(
                    msg.sender.id,
                    msg.sender.first_name,
                    msg.sender.last_name,
                    msg.sender.username,
                    msg.sender.lang_code)
            if isinstance(msg.fwd_from, User):
                await models.update_user_real(
                    msg.fwd_from.id,
                    msg.fwd_from.first_name,
                    msg.fwd_from.last_name,
                    msg.fwd_from.username,
                    msg.fwd_from.lang_code)

            await self.status.set('last', get_now_timestamp())
            await self.status.set(str(gid), msg.id)

    @classmethod
    async def stat(cls):
        basic = await super().stat()
        return basic + repr(cls.status)


async def history_add_handler(bot, update, text):
    content = to_json(dict(gid=int(text)))
    await FetchHistoryWorker.queue.put(content)
    return 'Added <pre>{}</pre> into history fetching queue'.format(content)


async def workers_handler(bot, update, text):
    return await MessageInsertWorker.stat() + \
           await MessageMarkWorker.stat() + \
           await FindLinkWorker.stat() + \
           await OcrWorker.stat() + \
           await EntityUpdateWorker.stat() + \
           await FetchHistoryWorker.stat()
