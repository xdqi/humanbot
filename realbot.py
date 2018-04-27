import traceback
from os import getpid
from threading import current_thread
from typing import List
from io import BytesIO
from datetime import datetime
from logging import getLogger, INFO, DEBUG

from telegram.ext import Updater, MessageHandler, Filters
from telegram import Update, Bot, Message, User, PhotoSize, Chat

import config
from utils import upload_pic, ocr, report_exception, AdminCommandHandler, show_commands_handler
import cache
from models import update_user_real, update_group_real
import admin
import models
import humanbot

logger = getLogger(__name__)


def update_user(user: User):
    update_user_real(user.id, user.first_name, user.last_name, user.username, user.language_code)


bot_group_last_changed = cache.RedisExpiringSet('bot_group_last_changed', expire=300)
def update_group(bot: Bot, chat_id: int):
    if str(chat_id) in bot_group_last_changed:
        return
    chat = bot.get_chat(chat_id)
    bot_group_last_changed.add(str(chat_id))
    if chat.type in [Chat.GROUP, Chat.SUPERGROUP]:
        update_group_real(chat.id, chat.title, chat.username)


def message(bot: Bot, update: Update):
    if hasattr(update, 'message'):
        msg = update.message  # type: Message
        flag = models.ChatFlag.new
    else:
        msg = update.edited_message  # type: Message
        flag = models.ChatFlag.edited

    user = msg.from_user  # type: User
    text = msg.text

    if msg.photo:  # type: List[PhotoSize]
        text = msg.caption or ''  # in case of `None`
        photo = max(msg.photo, key=lambda p: p.file_size)  # type: PhotoSize
        file = bot.get_file(photo.file_id)

        buffer = BytesIO()
        file.download(out=buffer)
        buffer.seek(0)

        now = datetime.now()
        path = '/{}/{}'.format(now.year, now.month)
        filename = '{}-{}.jpg'.format(int(now.timestamp()), file.file_id)

        fullpath = upload_pic(buffer, path, filename)
        result = ocr(fullpath)
        logger.info('ocr result:\n%s', result)
        text = result + '\n' + text

    humanbot.insert_message(msg.chat_id, msg.message_id, user.id, text, msg.date, flag)
    update_user(user)
    update_group(bot, msg.chat_id)


def log_message(bot: Bot, update: Update):
    logger.debug('realbot %s', update)


def error_handler(bot: Bot, update: Update, error: Exception):
    try:
        raise error
    except:
        report_exception()
        logger.error('Exception raised on PID %s %s', getpid(), current_thread())
        traceback.print_exc()


def main():
    logger.setLevel(INFO)
    Bot.delete_webhook = lambda self: True

    for conf in config.BOTS:
        updater = Updater(token=conf['token'])
        # set up webhook
        updater.start_webhook(listen=conf['host'],
                              port=conf['port'],
                              url_path=conf['path'])

        # set up message handlers
        dispatcher = updater.dispatcher

        # admin bot only
        if conf['token'] == config.BOT_TOKEN:
            res = updater.bot.set_webhook(url=conf['url'])
            logger.info('Start webhook for %s returns %s', conf['name'], res)

            dispatcher.add_handler(AdminCommandHandler('exec', admin.execute_command_handler))
            dispatcher.add_handler(AdminCommandHandler('py', admin.evaluate_script_handler))
            dispatcher.add_handler(AdminCommandHandler('joinpub', admin.join_public_group_handler))
            dispatcher.add_handler(AdminCommandHandler('joinprv', admin.join_private_group_handler))
            dispatcher.add_handler(AdminCommandHandler('leave', admin.leave_group_handler))
            dispatcher.add_handler(AdminCommandHandler(['stats', 'stat'], humanbot.statistics_handler))
            dispatcher.add_handler(AdminCommandHandler('threads', humanbot.threads_handler))
            dispatcher.add_handler(AdminCommandHandler('workers', humanbot.workers_handler))
            dispatcher.add_handler(AdminCommandHandler('help', show_commands_handler))

        message_handler = MessageHandler(filters=Filters.text, edited_updates=True, callback=message)
        dispatcher.add_handler(message_handler)

        picture_handler = MessageHandler(filters=Filters.photo | Filters.document, edited_updates=True, callback=message)
        dispatcher.add_handler(picture_handler)

        all_handler = MessageHandler(filters=Filters.all, callback=log_message)
        dispatcher.add_handler(all_handler)

        dispatcher.add_error_handler(error_handler)

        logger.info('Webhook server is ready for %s', conf['name'])


if __name__ == '__main__':
    main()
