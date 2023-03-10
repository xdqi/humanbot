import asyncio
import traceback
from datetime import datetime, timedelta
from logging import getLogger
from io import BytesIO
from threading import current_thread
from os import makedirs
from random import randint
from base64 import b64encode
from ujson import dumps as to_json, loads as from_json

import aiohttp
from raven import Client as RavenClient
from raven_aiohttp import AioHttpTransport
import aiobotocore
import botocore.config

from aiogram.utils.exceptions import TelegramAPIError
from telethon import TelegramClient
from telethon.tl.types import Photo
from telethon.utils import get_peer_id, resolve_id, get_input_location

from b2 import B2
import config
import cache
import senders

logger = getLogger(__name__)
raven_client = RavenClient(config.RAVEN_DSN, transport=AioHttpTransport)

async def aiohttp_init():
    global aiohttp_session, aiobotocore_client, b2_api
    aiohttp_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))

    session = aiobotocore.get_session()
    protocol = 'https://' if config.MINIO_SECURE else 'http://'
    aiobotocore_client = session.create_client('s3',
                                               verify=config.MINIO_VERIFY,
                                               endpoint_url=protocol + config.MINIO_SERVER,
                                               aws_secret_access_key=config.MINIO_SECRET_KEY,
                                               aws_access_key_id=config.MINIO_ACCESS_KEY,
                                               region_name=config.MINIO_REGION,
                                               config=botocore.config.Config(signature_version='s3v4'))
    b2_api = B2(config.B2_APPLICATION_KEY_ID, config.B2_APPLICATION_KEY_SECRET)


class OcrError(BaseException):
    pass


async def wget_retry(url, remaining_retry=5):
    if remaining_retry == 0:
        raise OcrError
    try:
        async with aiohttp_session.get(url) as resp:
            return await resp.json(content_type=None, encoding='utf-8')
    except (aiohttp.ServerTimeoutError, asyncio.TimeoutError):
        return await wget_retry(url, remaining_retry - 1)


async def upload_local(buffer: BytesIO, root, path, filename) -> str:
    url_path = '{}/{}'.format(path, filename)
    # copy to local network drive
    makedirs('{}{}'.format(root, path), exist_ok=True)
    with open('{}{}'.format(root, url_path), 'wb') as f:
        f.write(buffer.read())
        buffer.close()
    logger.info('File uploaded to %s', url_path)
    return url_path


async def upload_minio(buffer: BytesIO, path, filename) -> str:
    url_path = '{}/{}'.format(path, filename)
    await aiobotocore_client.put_object(Bucket=config.MINIO_BUCKET,
                                        Key=url_path,
                                        Body=buffer,
                                        ContentLength=buffer.getbuffer().nbytes
                                        )
    return url_path


async def upload_b2(buffer: BytesIO, path, filename) -> str:
    url_path = '{}/{}'.format(path, filename)
    await b2_api.upload(config.B2_BUCKET_ID, url_path, buffer, buffer.getbuffer().nbytes)
    return url_path


async def upload_pic(buffer, path, filename) -> str:
    return await upload_b2(buffer, path, filename)


async def upload_log(buffer, path, filename) -> str:
    return await upload_local(buffer, config.LOG_PATH, path, filename)


async def ocr(fullpath: str):
    # do the ocr on server
    result = 'tgpic://kosaka/{}/{}'.format(config.FTP_NAME, fullpath)
    ocr_result = await wget_retry(config.OCR_URL + fullpath)  # type: dict
    if 'ocr' in ocr_result.keys():
        result += '\nOCR result:\n'
        result += ocr_result['ocr']
    if 'barcode' in ocr_result.keys():
        result += '\nBarcode result:\n'
        result += ocr_result['barcode']

    logger.info('pic ocred & qred\n%s', result)

    return result


def get_now_timestamp() -> int:
    return int(datetime.now().timestamp())


def tg_html_entity(s: str) -> str:
    s = s.replace('&', '&amp;')
    s = s.replace('<', '&lt;')
    s = s.replace('>', '&gt;')
    return s


async def send_to(chat: int, msg: str, strip: bool = True):
    logger.info('Sending to administrators: \n%s', msg)
    html = tg_html_entity(msg)
    if strip and len(html.encode('utf-8')) > 500 or len(msg.splitlines()) > 10:
        buffer = BytesIO(msg.encode('utf-8'))
        now = datetime.now()
        date = now.strftime('%y/%m/%d')
        timestamp = now.timestamp()
        path = '/{}'.format(date)
        thread_name = current_thread().name  # todo: there may be a problem
        filename = '{}-{}.txt'.format(thread_name, timestamp)
        exception = html.splitlines()[-1]
        url_path = await upload_log(buffer, path, filename)

        html = 'Long message: ... {}\nURL: {}{}\nTime: {}'.format(
            exception,
            config.LOG_URL,
            url_path,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
    try:
        await senders.bot.send_message(chat_id=chat,
                                       text=html.strip(),
                                       parse_mode='HTML',
                                       disable_web_page_preview=False)
    except TelegramAPIError:
        report_exception()


async def send_to_admin_channel(msg: str):
    await send_to(config.ADMIN_CHANNEL, msg)


async def send_to_admin_group(msg: str):
    await send_to(config.ADMIN_GROUP, msg)


def report_exception():
    raven_client.captureException()


def peer_to_internal_id(peer):
    """
    Get bot marked ID

    :param peer:
    :return:
    """
    return get_peer_id(peer)


def internal_id_to_peer(marked_id: int):
    i, t = resolve_id(marked_id)
    return t(i)


def get_random_time(hour):
    now = datetime.now()
    time = now.replace(hour=randint(hour - 1, hour + 1), minute=randint(0, 59), second=randint(0, 59))
    return int(time.timestamp())


async def need_to_be_online():
    global_count = cache.RedisDict('global_count')
    today = datetime.now().strftime('%Y-%m-%d')

    if await global_count['today'] != today:
        await global_count.set('today', today)
        await global_count.set('online_time', get_random_time(config.ONLINE_HOUR))
        await global_count.set('offline_time', get_random_time(config.OFFLINE_HOUR))

    if int(await global_count['online_time']) < get_now_timestamp() < int(await global_count['offline_time']) and \
            randint(0, 10) == 5:
        return True
    return False


async def get_photo_address(client: TelegramClient, media: Photo):
    # get largest photo
    original = media.sizes[-1]
    location = original.location  # type: FileLocation
    now = datetime.now()
    dc_id, input_location = get_input_location(media)
    input_location_json = input_location.to_dict()
    input_location_json['file_reference'] = b64encode(input_location_json['file_reference']).decode('utf-8')
    return to_json(dict(
        location=location.to_dict(),
        input_location=input_location_json,
        client=(await client.get_me(input_peer=True)).user_id,
        path='{}/{}'.format(now.year, now.month),
        filename='{}-{}_{}_{}.jpg'.format(get_now_timestamp(),
                                          dc_id,
                                          location.volume_id,
                                          location.local_id)
    ))


async def report_statistics(measurement: str, tags: dict, fields: dict):
    global_statistics = cache.RedisDict('global_statistics')

    for k, v in fields.items():
        new_tags = tags.copy()
        new_tags['key'] = k
        await global_statistics.incrby(measurement + '|' + to_json(new_tags), v)


def block(c):
    return asyncio.get_event_loop().run_until_complete(c)


def noblock(c):
    asyncio.get_event_loop().create_task(c)
