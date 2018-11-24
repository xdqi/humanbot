from aiogram import Bot
from telethon import TelegramClient
import config
from typing import Dict

__all__ = ['bot', 'invoker', 'clients']

bot = None  # type: Bot
invoker = None  # type: TelegramClient
clients = {}  # type: Dict[int, TelegramClient]
bots = {}


def create_client(session: str) -> TelegramClient:
    return TelegramClient(session=session,
                          api_id=config.TG_API_ID,
                          api_hash=config.TG_API_HASH,
                          proxy=None)


def bind_client_conf(client: TelegramClient, conf):
    conf['client'] = client
    client.conf = conf
    clients[conf['uid']] = client


def create_clients():
    global invoker, bot
    for conf in config.CLIENTS:
        client = create_client(conf['session_name'])
        if conf['session_name'] == config.INVOKER_SESSION_NAME:
            invoker = client
        bind_client_conf(client, conf)

    for conf in config.NEW_BOTS:
        client = create_client(conf['session_name'])
        bind_client_conf(client, conf)
