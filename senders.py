from telegram import Bot
from telethon import TelegramClient
import config
from typing import List

__all__ = ['bot', 'invoker', 'clients']

bot = Bot(token=config.BOT_TOKEN)
invoker = None  # type: TelegramClient
clients = []  # type: List[TelegramClient]


def create_client(session: str):
    return TelegramClient(session=session,
                          api_id=config.TG_API_ID,
                          api_hash=config.TG_API_HASH,
                          proxy=None,
                          update_workers=4)


for conf in config.CLIENTS:
    client = create_client(conf['session_name'])
    if conf['session_name'] == config.INVOKER_SESSION_NAME:
        invoker = client
