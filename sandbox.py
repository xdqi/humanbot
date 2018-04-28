from telethon import TelegramClient, events
import config

# These example values won't work. You must get your own api_id and
# api_hash from https://my.telegram.org, under API Development.
api_id = config.TG_API_ID
api_hash = config.TG_API_HASH

client = TelegramClient('sandbox', api_id, api_hash, update_workers=1)

client.start()


def handler(u):
    print(u)


client.add_event_handler(handler, events.Raw)
input('press any key to stop')