import traceback
from os import getpid
from threading import current_thread
from typing import List
from datetime import datetime
from logging import getLogger, INFO, DEBUG

from aiogram import Bot, Dispatcher
from aiogram.types import Update, ChatType, Message, User, PhotoSize, ContentType
import aiogram.dispatcher.webhook

import config
import workers
import senders
import discover
from utils import report_exception, get_now_timestamp, send_to_admin_channel, to_json
import cache
from models import update_user_real, update_group_real, insert_message, ChatFlag
import admin
import httpd
import humanbot

logger = getLogger(__name__)


async def update_user(user: User):
    await update_user_real(user.id, user.first_name, user.last_name, user.username, user.language_code)


bot_group_last_changed = cache.RedisExpiringSet('bot_group_last_changed', expire=300)
async def update_group(bot: Bot, chat_id: int):
    if await bot_group_last_changed.contains(str(chat_id)):
        return
    chat = await bot.get_chat(chat_id)
    await bot_group_last_changed.add(str(chat_id))
    if chat.type in [ChatType.GROUP, ChatType.SUPER_GROUP]:
        await update_group_real((await bot.me).id, chat.id, chat.title, chat.username)


async def message(bot: Bot, msg: Message, flag: ChatFlag):
    user = msg.from_user  # type: User
    text = msg.text

    if msg.photo:  # type: List[PhotoSize]
        text = msg.md_text or msg.caption or ''  # in case of `None`
        photo = max(msg.photo, key=lambda p: p.file_size)  # type: PhotoSize
        now = datetime.now()

        info = to_json(dict(
            client=(await bot.me).id,
            file_id=photo.file_id,
            path='{}/{}'.format(now.year, now.month),
            filename='{}-{}.jpg'.format(get_now_timestamp(), photo.file_id)
        ))

        text = config.OCR_HINT + '\n' + info + '\n' + text

    if user:
        uid = user.id
        await update_user(user)
    else:
        uid = None

    await insert_message(msg.chat.id, msg.message_id, uid, text, msg.date, flag, find_link=False)
    await discover.find_link_enqueue(msg.text)
    await update_group(bot, msg.chat.id)


async def error_handler(bot: Bot, update: Update, error: Exception):
    report_exception()
    logger.error('Exception raised on PID %s %s', getpid(), current_thread())
    await send_to_admin_channel(f'Exception raised on PID {getpid()} {current_thread()}\n {traceback.format_exc()}')
    traceback.print_exc()


async def delete_webhook(*args, **kwargs):
    return True


class MyDispatcher(Dispatcher):
    def make_message_handler(self, callback, flag: ChatFlag):
        async def my_message_handler(msg):
            await callback(self.bot, msg, flag)

        return my_message_handler

    def register_listen_handler(self, callback, *kargs, **kwargs):
        self.register_message_handler(callback=self.make_message_handler(callback, ChatFlag.new), *kargs, **kwargs)
        self.register_edited_message_handler(callback=self.make_message_handler(callback, ChatFlag.edited), *kargs, **kwargs)

    def register_command_handler(self, commands, callback, *kargs, **kwargs):
        async def command_handler(message: Message):
            text = message.text[1 + len(message.get_full_command()):].strip()
            result = await callback(self, message, text)
            if result:  # we allows no message
                await message.reply(text=result, parse_mode='HTML')

        self.register_message_handler(callback=command_handler,
                                      commands=commands,
                                      func=lambda msg: msg.chat.id in config.ADMIN_UIDS)


def make_webhook_handler(dispatcher):
    class MyWebhookRequestHandler(aiogram.dispatcher.webhook.WebhookRequestHandler):
        def get_dispatcher(self):
            return dispatcher

    return MyWebhookRequestHandler


async def main():
    logger.setLevel(INFO)
    Bot.delete_webhook = delete_webhook
    aiogram.dispatcher.webhook._check_ip = lambda: True

    for conf in config.BOTS:
        bot = Bot(token=conf['token'])
        dispatcher = MyDispatcher(bot)

        # set up message handlers
        senders.clients[conf['uid']] = bot
        senders.bots[conf['uid']] = bot

        # admin bot only
        if conf['token'] == config.BOT_TOKEN:
            res = await bot.set_webhook(url=conf['url'])
            logger.info('Start webhook for %s returns %s', conf['name'], res)

            dispatcher.register_command_handler('exec', admin.execute_command_handler)
            dispatcher.register_command_handler('py', admin.evaluate_script_handler)
            dispatcher.register_command_handler('joinpub', admin.join_public_group_handler)
            dispatcher.register_command_handler('joinprv', admin.join_private_group_handler)
            dispatcher.register_command_handler('leave', admin.leave_group_handler)
            dispatcher.register_command_handler(['stats', 'stat'], humanbot.statistics_handler)
            dispatcher.register_command_handler('threads', humanbot.threads_handler)
            dispatcher.register_command_handler('workers', workers.workers_handler)
            dispatcher.register_command_handler('fetch', workers.history_add_handler)
            dispatcher.register_command_handler('dialogs', admin.dialogs_handler)
            # dispatcher.register_command_handler('help', show_commands_handler)

        dispatcher.register_listen_handler(message, content_types=ContentType.TEXT | ContentType.PHOTO | ContentType.DOCUMENT)

        dispatcher.register_errors_handler(error_handler)

        # start webhook server
        httpd.app.router.add_route('*', conf['path'], make_webhook_handler(dispatcher), name=conf['path'])

        logger.info('Webhook server is ready for %s', conf['name'])


if __name__ == '__main__':
    main()
