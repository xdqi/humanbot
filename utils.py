import traceback
from datetime import datetime
from ftplib import FTP, Error as FTPError
from logging import getLogger
from io import BytesIO
from threading import current_thread

from requests import get, ReadTimeout

import config
from senders import bot


logger = getLogger(__name__)


class KosakaFTP(FTP):
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=5, *args, **kwargs)

    def cdp(self, directory):
        if directory != "":
            try:
                self.cwd(directory)
                logger.info('cd to %s', directory)
            except FTPError:
                new_dir = "/".join(directory.split("/")[:-1])
                logger.debug('go up to %s', new_dir)
                self.cdp(new_dir)
                logger.debug('mkdir %s', directory)
                self.mkd(directory)
                logger.debug('cd %s', directory)
                self.cwd(directory)


def remove_ocr_spaces(msg: str):
    parts = msg.split(' ')
    result = ''
    for i in range(len(parts) - 1):
        result += parts[i]

        prev = parts[i][-1]
        after = parts[i + 1][0]
        if ord(prev) < 1328 and ord(after) < 1328:  # detect latin/cyrillic character only
            result += ' '

    result += parts[-1]
    return result


class FakeResponse():
    def json(self):
        return {}


def wget_retry(url, remaining_retry=1):
    if remaining_retry == 0:
        traceback.print_exc()
        return FakeResponse()
    try:
        return get(url, timeout=10)
    except ReadTimeout:
        return wget_retry(url, remaining_retry - 1)


def upload(buffer, path, filename) -> str:
    fullpath = '{}/{}'.format(path, filename)
    # upload to ftp server
    ftp = KosakaFTP()
    ftp.connect(**config.FTP_SERVER)
    ftp.login(**config.FTP_CREDENTIAL)
    ftp.cdp(path)
    ftp.storbinary('STOR {}'.format(filename), buffer)
    buffer.close()
    ftp.close()
    logger.info('File uploaded to %s', fullpath)
    return fullpath


def ocr(fullpath: str):
    # do the ocr on server
    result = 'tgpic://kosaka/{}{}'.format(config.FTP_NAME, fullpath)
    req = wget_retry(config.OCR_URL + fullpath)
    ocr_result = req.json()  # type: dict
    if 'body' in ocr_result.keys():
        result += '\n'
        result += remove_ocr_spaces(ocr_result['body'])
    logger.info('pic ocred\n%s', result)

    return result


def get_now_timestamp() -> int:
    return int(datetime.now().timestamp())


def send_message_to_administrators(msg: str):
    logger.info('Sending to administrators: \n%s', msg)
    if len(msg.encode('utf-8')) > 500 or len(msg.splitlines()) > 10:
        buffer = BytesIO(msg.encode('utf-8'))
        now = datetime.now()
        date = now.strftime('%y%m%d')
        timestamp = now.timestamp()
        path = '/log/{}'.format(date)
        thread_name = current_thread().name
        filename = '{}-{}.txt'.format(thread_name, timestamp)
        exception = msg.splitlines()[-1]
        upload(buffer, path, filename)
        msg = 'Long message: ... {}\nURL: http://fra2.dom.ain.kwsv.win{}/{}'.format(
            exception,
            path,
            filename
        )
    bot.send_message(chat_id=config.ADMIN_CHANNEL,
                     text='```{}```'.format(msg.strip()),
                     parse_mode='markdown',
                     disable_web_page_preview=False)
