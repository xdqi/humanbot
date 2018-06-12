from ast import literal_eval
from io import BytesIO
from threading import Thread
from time import sleep
import traceback
from logging import getLogger
from cProfile import Profile
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.tl.types import Message, MessageService, InputFileLocation, User, MessageMediaPhoto
from telethon.extensions import markdown
from telethon.errors.rpc_error_list import AuthKeyUnregisteredError, FloodWaitError, ChannelPrivateError, \
    RpcCallFailError
from telegram import Bot

import cache
import config
import models
from senders import clients
from utils import get_now_timestamp, report_exception, upload_pic, ocr, get_photo_address, from_json, to_json, \
    send_to_admin_channel

logger = getLogger(__name__)


class WorkProperties(type):
    def __new__(mcs, class_name, class_bases, class_dict):
        name = class_dict['name']
        new_class_dict = class_dict.copy()
        new_class_dict['status'] = cache.RedisDict(name + '_worker_status')
        new_class_dict['queue'] = cache.RedisQueue(name + '_queue')
        return type.__new__(mcs, class_name, class_bases, new_class_dict)


class Worker(Thread, metaclass=WorkProperties):
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
                    sleep(0.01)
                    continue
                self.handler(session, message)
                session.commit()
                self.queue.task_done()
                self.status['last'] = get_now_timestamp()
                self.status['size'] = self.queue.qsize()
            except KeyboardInterrupt:
                break
            except:
                traceback.print_exc()
                report_exception()
                session.rollback()
                self.queue.put(message)

        session.close()
        models.Session.remove()

    def start(self, count: int=1):
        if count > 1:
            type(self)().start(count - 1)
        super().start()

    def handler(self, session, message: str):
        raise NotImplementedError

    @classmethod
    def stat(cls):
        return '{} worker: {} seconds ago, size {}\n'.format(
            cls.name, get_now_timestamp() - int(cls.status['last']), cls.queue.qsize())


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


class OcrWorker(Worker):
    name = 'ocr'

    def handler(self, session, message: str):
        record_id = int(message)
        record = session.query(models.ChatNew).filter(models.ChatNew.id == record_id).one_or_none()  # type: models.ChatNew

        if record is None:
            return

        hint, info_text, text = record.text.split('\n', maxsplit=2)

        info = from_json(info_text)
        client = clients[info['client']]
        buffer = BytesIO()

        if isinstance(client, TelegramClient):
            location_info = info['location']
            del location_info['_']
            del location_info['dc_id']
            location = InputFileLocation(**location_info)

            try:
                client.download_file(location, buffer)
            except AuthKeyUnregisteredError as e:
                report_exception()
                logger.warning('download picture auth key unregistered error %r', e)
                return
        elif isinstance(client, Bot):
            file_id = info['file_id']

            file = client.get_file(file_id)
            file.download(out=buffer)

        buffer.seek(0)
        logger.info('pic downloaded')
        full_path = upload_pic(buffer, info['path'], info['filename'])
        result = ocr(full_path)
        logger.info('ocr complete')

        record.text = result + '\n' + text


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


class FindLinkWorker(Worker):
    name = 'find_link'

    def handler(self, session, message):
        from discover import find_link_to_join
        find_link_to_join(session, message)


class FetchHistoryWorker(Worker):
    name = 'history'

    def handler(self, session, message: str):
        info = from_json(message)
        gid = info['gid']

        record = session.query(
            models.Group.gid, models.Group.master, models.func.min(models.ChatNew.message_id)
        ).filter(
            models.Group.gid == gid, models.Group.gid == models.ChatNew.chat_id
        ).group_by(models.Group.gid).one_or_none()

        if not record:
            logger.warning('No message id detected or group not joined ever before')
            return
        gid_, master, self.first = record
        client = clients[master]

        if isinstance(client, Bot):
            logger.warning('Group is managed by a bot, cannot fetch information')
            return
        profile = Profile()
        profile.enable()
        while True:
            try:
                prev = self.first
                self.fetch(client, gid)
                if prev == self.first:  # no new messages
                    break
            except FloodWaitError as e:
                sleep(e.seconds + 1)
            except ChannelPrivateError as e:
                send_to_admin_channel(f'fetch worker failed: group {gid} (managed by {master}) kicked us')
            except KeyboardInterrupt:
                break
            except RpcCallFailError:
                continue
            except:
                send_to_admin_channel(traceback.format_exc() + '\nfetch worker unknown exception')

        profile.disable()
        profile.dump_stats('normal.profile')

    def fetch(self, client, gid):
        for msg in client.iter_messages(entity=gid,
                                        limit=None,
                                        offset_id=self.first,
                                        max_id=self.first,
                                        wait_time=0
                                        ):  # type: Message
            self.first = msg.id
            before = datetime.now().timestamp()
            self.save(client, gid, msg)
            after = datetime.now().timestamp()
            sleep(0.01 - (after - before))

    def save(self, client, gid, msg: Message):
        if True:
            if isinstance(msg, MessageService):
                return
            text = markdown.unparse(msg.message, msg.entities)

            if isinstance(msg.media, MessageMediaPhoto):
                result = get_photo_address(client, msg.media)
                text = config.OCR_HISTORY_HINT + '\n' + result + '\n' + text

            models.insert_message_local_timezone(gid, msg.id, msg.from_id, text, msg.date)
            if msg.input_sender is not None:  # type: User
                models.update_user_real(msg.sender.id,
                                        msg.sender.first_name,
                                        msg.sender.last_name,
                                        msg.sender.username,
                                        msg.sender.lang_code)
            if isinstance(msg.fwd_from, User):
                models.update_user_real(msg.fwd_from.id,
                                        msg.fwd_from.first_name,
                                        msg.fwd_from.last_name,
                                        msg.fwd_from.username,
                                        msg.fwd_from.lang_code)

            self.status['last'] = get_now_timestamp()
            self.status['gid'] = gid
            self.status['message_id'] = msg.id

    @classmethod
    def stat(cls):
        basic = super().stat()
        return basic + f'Group ID: {cls.status["gid"]}\n' \
                       f'Message ID: {cls.status["message_id"]}\n'


def history_add_handler(bot, update, text):
    content = to_json(dict(gid=int(text)))
    FetchHistoryWorker.queue.put(content)
    return 'Added <pre>{}</pre> into history fetching queue'.format(content)


def workers_handler(bot, update, text):
    return MessageInsertWorker.stat() + \
           MessageMarkWorker.stat() + \
           FindLinkWorker.stat() + \
           OcrWorker.stat() + \
           EntityUpdateWorker.stat() + \
           FetchHistoryWorker.stat()
