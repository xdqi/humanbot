import asyncio
from io import BytesIO
from threading import Thread
import traceback
from logging import getLogger
from cProfile import Profile
from datetime import datetime, timedelta
from concurrent.futures import CancelledError

from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Message, MessageService, InputFileLocation, User, MessageMediaPhoto, ChatInvite
from telethon.extensions import markdown
from telethon.errors import AuthKeyUnregisteredError, FloodWaitError, ChannelPrivateError, \
    RpcCallFailError, ChannelsTooMuchError
from aiogram import Bot

import aiomysql.sa
import sqlalchemy
from aioinflux import InfluxDBClient

import cache
import config
import models
import realbot
import senders
from utils import get_now_timestamp, report_exception, upload_pic, ocr, get_photo_address, from_json, to_json, \
    send_to_admin_channel, noblock, block, OcrError, tg_html_entity, report_statistics

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
        engine = await models.get_aio_engine()

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
            except (KeyboardInterrupt, CancelledError):
                await self.queue.put(message)
                break
            except BaseException as e:
                logger.error('%s worker fails: %s', str(type(self)), e)
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
    cache = cache.RedisDailyDict('ocr')
    lock = asyncio.Lock()

    async def try_cache(self, path: str):
        ts, file_id = path.split('-', maxsplit=1)
        result = await self.cache[file_id]
        if result == config.OCR_PROCESSING_HINT:
            return True
        elif result is None:
            return False
        else:
            return result

    async def remove_cache(self, path: str):
        ts, file_id = path.split('-', maxsplit=1)
        await self.cache.delitem(file_id)

    async def do_ocr(self, info: dict):
        client = senders.clients[info['client']]
        buffer = BytesIO()

        if isinstance(client, TelegramClient):
            location_info = info['location']
            try:
                del location_info['_']
            except KeyError:
                pass
            try:
                del location_info['dc_id']
            except KeyError:
                pass
            location = InputFileLocation(**location_info)

            try:
                await client.download_file(location, buffer)
            except AuthKeyUnregisteredError as e:
                report_exception()
                logger.warning('download picture auth key unregistered error %r', e)
                return config.OCR_HINT + '\n' + to_json(info)
            except FloodWaitError as e:
                logger.warning('download picture flooded (%s), wait %s seconds', info['client'], e.seconds)
                return config.OCR_HINT + '\n' + to_json(info)
        elif isinstance(client, Bot):
            file_id = info['file_id']

            try:
                await client.download_file_by_id(file_id, destination=buffer)
            except RuntimeError:
                realbot.init(0)
                raise

        buffer.seek(0)
        full_path = await upload_pic(buffer, info['path'], info['filename'])

        await report_statistics(measurement='bot',
                                tags={'master': info['client'],
                                      'type': 'ocr'},
                                fields={'count': 1})

        return await ocr(full_path)

    async def handler(self, engine: aiomysql.sa.Engine, message: str):
        ocr_request = from_json(message)  # type: dict # {'chat_id': 114, 'message_id': 514}

        record_id = ocr_request['id']

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = models.Core.ChatNew.select().where(models.ChatNew.id == record_id)
            records = await conn.execute(stmt)
            if not records.rowcount:
                logger.warning('ocr record %s found %s items (try %s), fail',
                               record_id, records.rowcount, ocr_request.get('tries', 0))
                ocr_request['tries'] = ocr_request.get('tries', 0) + 1
                if ocr_request['tries'] < 1000:
                    await OcrWorker.queue.put(to_json(ocr_request))
                    await asyncio.sleep(0.1)
                return
            row = await records.fetchone()
            record_text = row.text

        hint, info_text, text = record_text.split('\n', maxsplit=2)

        info = from_json(info_text)
        logger.info('ocr %s started', record_id)

        # async with self.lock:
        cached = await self.try_cache(info['filename'])

        if isinstance(cached, str):
            logger.info('ocr %s cached', record_id)
            result = cached
        elif cached is True:
            logger.info('ocr %s need retry', record_id)
            ocr_request['tries'] = ocr_request.get('tries', 0) + 1
            if ocr_request['tries'] < 100:
                await asyncio.sleep(0.1)
                await self.queue.put(to_json(ocr_request))
            else:
                ocr_request['tries'] = 0
                await self.remove_cache(info['filename'])
                await self.queue.put(to_json(ocr_request))
            return
        elif cached is False:
            ts, file_id = info['filename'].split('-', maxsplit=1)
            await self.cache.set(file_id, config.OCR_PROCESSING_HINT)
            try:
                result = await self.do_ocr(info)
            except OcrError:
                result = config.OCR_FAILED_HINT + '\n' + to_json(info)
            await self.cache.set(file_id, result)
            logger.info('ocr %s complete', record_id)

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = models.Core.ChatNew.\
                update().\
                where(models.ChatNew.id == record_id).\
                values(text=result + '\n' + text)
            await conn.execute(stmt)
            await conn.execute('COMMIT;')


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

        master = row.master
        self.master = row.master
        name = row.name
        link = row.link
        self.first = row.min_message_id

        print(senders.clients.keys(), 'master is', repr(master))
        client = senders.clients.get(master, None)

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
                    await self.status.delitem(gid)
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
        async for msg in client.iter_messages(entity=gid,
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

            await report_statistics(measurement='bot',
                                    tags={'master': self.master,
                                          'type': 'history'},
                                    fields={'count': 1})

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
        return basic + await cls.status.repr()


class InviteWorker(CoroutineWorker):
    name = 'invite'

    async def handler(self, engine: aiomysql.sa.Engine, message: str):
        info = from_json(message)

        await report_statistics(measurement='bot',
                                tags={'type': 'invite'},
                                fields={'count': 1})

        async with engine.acquire() as conn:  # type: aiomysql.sa.SAConnection
            stmt = models.Core.GroupInvite.insert().values(**info)
            await conn.execute(stmt)
            await conn.execute('COMMIT')


class JoinGroupWorker(CoroutineWorker):
    name = 'join'
    wait_until = 0

    async def handler(self, engine: aiomysql.sa.Engine, message: str):
        self.wait_until = 0

        info = from_json(message)
        link_type = info['link_type']
        link = info['link']
        group_type = info['group_type']
        title = info['title']
        count = info['count']
        if link_type == 'public':
            try:
                group = await senders.invoker.get_input_entity(link)  # type: InputChannel
            except FloodWaitError as e:
                logger.warning('Get group via username flooded. %r', e)
                await self.queue.put(message)
                return

        global_count = cache.RedisDict('global_count')
        try:
            if link_type == 'public':
                await senders.invoker(JoinChannelRequest(group))
                full_link = '@' + link
            elif link_type == 'private':
                await senders.invoker(ImportChatInviteRequest(link))
                full_link = 't.me/joinchat/' + link

            await report_statistics(measurement='bot',
                                    tags={'type': 'join',
                                          'group_type': link_type},
                                    fields={'count': 1})
            await send_to_admin_channel(f'joined {link_type} {group_type}\n'
                                        f'{tg_html_entity(title)} ({full_link})\n'
                                        f'members: {count}'
                                        )
            await global_count.set('full', '0')
        except ChannelsTooMuchError:
            if await global_count['full'] == '0':
                await send_to_admin_channel('Too many groups! It\'s time to sign up for a new account')
                await global_count.set('full', '1')
            return
        except FloodWaitError as e:
            await self.queue.put(message)
            self.wait_until = get_now_timestamp() + e.seconds
            await send_to_admin_channel(f'Join group triggered flood, sleeping for {e.seconds} seconds.')
            await asyncio.sleep(e.seconds)
            return


class ReportStatisticsWorker(CoroutineWorker):
    name = 'report'
    global_statistics = cache.RedisDict('global_statistics')

    async def run(self):
        logger.info('%s worker has started', self.name)

        self.influxdb_client = InfluxDBClient(**config.INFLUXDB_CONFIG)
        if config.INFLUXDB_URL:
            self.influxdb_client._url = config.INFLUXDB_URL

        while True:
            try:
                await self.report()
                await self.status.set('last', get_now_timestamp())
                await asyncio.sleep(30)
            except (KeyboardInterrupt, CancelledError):
                await self.influxdb_client.close()
                break
            except:
                traceback.print_exc()
                report_exception()
                continue

    async def report(self):
        for k, v in await self.global_statistics.items():
            noblock(self.global_statistics.set(k, 0))

            measurement, tags_ = k.split('|', maxsplit=1)
            tags = from_json(tags_)
            key = tags['key']
            del tags['key']

            noblock(self.influxdb_client.write(dict(
                time=datetime.now(),
                measurement=measurement,
                tags=tags,
                fields={key: v}
            )))


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
           await InviteWorker.stat() + \
           await JoinGroupWorker.stat() + \
           await FetchHistoryWorker.stat() + \
           await ReportStatisticsWorker.stat()
