from telegram import Bot
from telethon import TelegramClient
import config

bot = Bot(token=config.BOT_TOKEN)
client = TelegramClient(session=config.SESSION_NAME,
                        api_id=config.TG_API_ID,
                        api_hash=config.TG_API_HASH,
                        proxy=None,
                        update_workers=4)
