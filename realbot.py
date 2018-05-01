import traceback
from os import getpid
from threading import current_thread
from typing import List
from datetime import datetime
from logging import getLogger, INFO, DEBUG
from threading import Thread
import json

from telegram.ext import Updater, MessageHandler, Filters
from telegram import Update, Bot, Message, User, PhotoSize, Chat
import flask

import config
import workers
import senders
from utils import report_exception, AdminCommandHandler, show_commands_handler, get_now_timestamp, send_to_admin_channel, to_json
import cache
from models import update_user_real, update_group_real, insert_message, ChatFlag
import admin
import httpd
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
        update_group_real(bot.id, chat.id, chat.title, chat.username)


def message(bot: Bot, update: Update):
    if hasattr(update, 'message'):
        msg = update.message  # type: Message
        flag = ChatFlag.new
    else:
        msg = update.edited_message  # type: Message
        flag = ChatFlag.edited

    user = msg.from_user  # type: User
    text = msg.text

    if msg.photo:  # type: List[PhotoSize]
        text = msg.text or msg.caption or ''  # in case of `None`
        photo = max(msg.photo, key=lambda p: p.file_size)  # type: PhotoSize
        now = datetime.now()

        info = to_json(dict(
            client=bot.id,
            file_id=photo.file_id,
            path='{}/{}'.format(now.year, now.month),
            filename='{}-{}.jpg'.format(get_now_timestamp(), photo.file_id)
        ))

        text = config.OCR_HINT + '\n' + info + '\n' + text

    if user:
        uid = user.id
        update_user(user)
    else:
        uid = None

    insert_message(msg.chat_id, msg.message_id, uid, text, msg.date, flag)
    update_group(bot, msg.chat_id)


def log_message(bot: Bot, update: Update):
    logger.debug('realbot %s', update)


def error_handler(bot: Bot, update: Update, error: Exception):
    try:
        raise error
    except:
        report_exception()
        logger.error('Exception raised on PID %s %s', getpid(), current_thread())
        send_to_admin_channel(f'Exception raised on PID {getpid()} {current_thread()}\n {traceback.format_exc()}')
        traceback.print_exc()


def bot_handler(updater: Updater):
    if flask.request.headers.get('content-type').lower() == 'application/json':
        json_string = flask.request.get_data().decode('utf-8')
        logger.debug('Webhook received data: ' + json_string)

        update = Update.de_json(json.loads(json_string), updater.bot)
        logger.debug('Received Update with ID %d on Webhook' % update.update_id)

        updater.update_queue.put(update)
        return ''
    else:
        flask.abort(403)


class MyUpdater(Updater):
    def _start_webhook(self, listen, port, url_path, cert, key, bootstrap_retries, clean,
                       webhook_url, allowed_updates):
        self.logger.debug('Updater thread started (webhook)')
        httpd.app.add_url_rule(url_path, url_path, lambda: bot_handler(self), methods=['POST'])

    def _stop_httpd(self):
        pass


def main():
    logger.setLevel(INFO)
    Bot.delete_webhook = lambda self: True

    for conf in config.BOTS:
        updater = MyUpdater(token=conf['token'])
        updater.start_webhook(url_path=conf['path'])

        # set up message handlers
        dispatcher = updater.dispatcher
        senders.clients[conf['uid']] = updater.bot
        senders.bots[conf['uid']] = updater

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
            dispatcher.add_handler(AdminCommandHandler('workers', workers.workers_handler))
            dispatcher.add_handler(AdminCommandHandler('fetch', workers.history_add_handler))
            dispatcher.add_handler(AdminCommandHandler('dialogs', admin.dialogs_handler))
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
