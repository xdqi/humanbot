from typing import List
from io import BytesIO
from datetime import datetime

from expiringdict import ExpiringDict

from telegram.ext import Updater, MessageHandler, Filters
from telegram import Update, Bot, Message, User, PhotoSize, Chat

import config
from humanbot import insert_message, update_user_real, update_group_real, upload_ocr


def update_user(user: User):
    update_user_real(user.id, user.first_name, user.last_name, user.username, user.language_code)


group_last_changed = ExpiringDict(max_len=100, max_age_seconds=300)
def update_group(bot: Bot, chat_id: int):
    if chat_id in group_last_changed.keys():
        return
    chat = bot.get_chat(chat_id)
    group_last_changed[chat_id] = True
    if chat.type in [Chat.GROUP, Chat.SUPERGROUP]:
        update_group_real(chat.id, chat.title, chat.username)


def message(bot: Bot, update: Update):
    msg = update.message  # type: Message
    user = msg.from_user  # type: User
    insert_message(msg.chat_id, user.id, msg.text, msg.date)
    update_user(user)
    update_group(bot, msg.chat_id)


def picture(bot: Bot, update: Update):
    msg = update.message  # type: Message
    user = msg.from_user  # type: User
    caption = msg.caption
    text = caption

    if msg.photo:  # type: List[PhotoSize]
        photo = max(msg.photo, key=lambda p: p.file_size)  # type: PhotoSize
        file = bot.get_file(photo.file_id)

        buffer = BytesIO()
        file.download(out=buffer)

        now = datetime.now()
        path = 'bot/{}'.format(now.strftime('%y%m%d'))
        filename = file.file_id

        result = upload_ocr(buffer, path, filename)
        text = result + '\n' + caption
    insert_message(msg.chat_id, user.id, text, msg.date)

    update_user(user)
    update_group(bot, msg.chat_id)


def main():
    updater = Updater(token=config.BOT_TOKEN)

    # set up webhook
    updater.start_webhook(listen=config.BOT_WEBHOOK_LISTEN,
                          port=config.BOT_WEBHOOK_PORT,
                          url_path=config.BOT_WEBHOOK_PATH)
    updater.bot.set_webhook(webhook_url=config.BOT_WEBHOOK_URL)

    # set up message handlers
    dispatcher = updater.dispatcher

    message_handler = MessageHandler(filters=Filters.text, callback=message)
    dispatcher.add_error_handler(message_handler)

    picture_handler = MessageHandler(Filters.photo | Filters.document, callback=picture)
    dispatcher.add_error_handler(picture_handler)


if __name__ == '__main__':
    main()
