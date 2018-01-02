import traceback
from datetime import datetime
from ftplib import FTP, Error as FTPError
from logging import getLogger

from requests import get, ReadTimeout

import config


logger = getLogger(__name__)


class KosakaFTP(FTP):
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=5, *args, **kwargs)

    def cdp(self, directory):
        if directory != "":
            try:
                self.cwd(directory)
                print('cwd')
            except FTPError:
                print('go up')
                self.cdp("/".join(directory.split("/")[:-1]))
                print('mkd')
                self.mkd(directory)
                print('cwd1')
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


def wget_retry(url, retry=2):
    if not retry:
        traceback.print_exc()
        return FakeResponse()
    try:
        return get(url, timeout=10)
    except ReadTimeout:
        return wget_retry(url, retry - 1)


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
    print('file uploaded')
    return fullpath


def ocr(fullpath: str):
    # do the ocr on server
    result = 'tgpic://kosaka/{}{}'.format(config.FTP_NAME, fullpath)
    req = wget_retry(config.OCR_URL + fullpath)
    ocr_result = req.json()  # type: dict
    if 'body' in ocr_result.keys():
        result += '\n'
        result += remove_ocr_spaces(ocr_result['body'])
    print('pic ocred\n', result)

    return result


def get_now_timestamp() -> int:
    return int(datetime.now().timestamp())
