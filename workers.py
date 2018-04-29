from ast import literal_eval
from io import BytesIO
from threading import Thread
from time import sleep
import traceback
from logging import getLogger

from telethon import TelegramClient
from telethon.tl.types import Message, MessageService, InputFileLocation, User, MessageMediaPhoto
from telethon.extensions import markdown
from telethon.errors.rpc_error_list import AuthKeyUnregisteredError
from telegram import Bot

import cache
import config
import models
from senders import clients
from utils import get_now_timestamp, report_exception, upload_pic, ocr, get_photo_address

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
        return '{} Worker: {} seconds ago, size {}\n'.format(
            cls.name, get_now_timestamp() - int(cls.status['last']), cls.queue.qsize())


class MessageInsertWorker(Worker):
    name = 'insert'

    def handler(self, session, message: str):
        chat = models.ChatNew(**literal_eval(message))
        session.add(chat)
        session.commit()
        session.refresh(chat)
        if chat.text.startswith(config.OCR_HINT):
            OcrWorker.queue.put(str(chat.id))


class MessageMarkWorker(Worker):
    name = 'mark'

    def handler(self, session, message: str):
        request_changes = literal_eval(message)  # {'chat_id': 114, 'message_id': 514}
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
        hint, info_text, text = record.text.split('\n', maxsplit=2)

        info = literal_eval(info_text)
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


class FindLinkWorker(Worker):
    name = 'find_link'

    def handler(self, session, message):
        from discover import find_link_to_join
        find_link_to_join(session, message)


class FetchHistoryWorker(Worker):
    name = 'history'

    def handler(self, session, message: str):
        info = literal_eval(message)
        gid = info['gid']

        record = session.query(
            models.Group.gid, models.Group.master, models.func.min(models.ChatNew.message_id)
        ).filter(
            models.Group.gid == gid, models.Group.gid == models.ChatNew.chat_id
        ).group_by(models.Group.gid).one_or_none()

        if not record:
            logger.warning('No message id detected or group not joined ever before')
            return
        gid_, master, first = record
        client = clients[master]

        if isinstance(client, Bot):
            logger.warning('Group is managed by a bot, cannot fetch information')
            return

        for msg in client.iter_messages(entity=gid,
                                        limit=None,
                                        offset_id=first,
                                        max_id=first,
                                        ):  # type: Message
            if isinstance(msg, MessageService):
                continue
            text = markdown.unparse(msg.message, msg.entities)

            if isinstance(msg.media, MessageMediaPhoto):
                result = get_photo_address(client, msg.media)
                text = config.OCR_HISTORY_HINT + '\n' + result + '\n' + text

            models.insert_message_local_timezone(gid, msg.id, msg.from_id, text, msg.date)
            if msg.sender:  # type: User
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


def workers_handler(bot, update, text):
    return MessageInsertWorker.stat() + \
           MessageMarkWorker.stat() + \
           FindLinkWorker.stat() + \
           OcrWorker.stat() + \
           FetchHistoryWorker.stat()
