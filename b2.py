from hashlib import sha1
from io import BytesIO

import aiohttp
from aiohttp.client_exceptions import ClientError
import cache
import logging

B2_API_BASE = '{0}/b2api/v2/{1}'
logger = logging.getLogger(__name__)


class B2Bare(object):
    def __init__(self, application_key_id, application_key_secret):
        self.application_key_id = application_key_id
        self.application_key_secret = application_key_secret
        self._session = None  # type: aiohttp.ClientSession
        self.create_session()

    def create_session(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def authorize_account(self):
        req = await self._session.get(url=B2_API_BASE.format('https://api.backblazeb2.com', 'b2_authorize_account'),
                                      auth=aiohttp.BasicAuth(login=self.application_key_id,
                                                             password=self.application_key_secret))
        rsp = await req.json()
        return req.status, rsp

    async def get_upload_url(self, authorization_token: str, api_url: str, bucket_id: str):
        req = await self._session.post(url=B2_API_BASE.format(api_url, 'b2_get_upload_url'),
                                       headers={'Authorization': authorization_token},
                                       json={'bucketId': bucket_id})
        rsp = await req.json()
        return req.status, rsp

    async def upload_file(self, upload_url, upload_authorization_token, filename, buffer: BytesIO, length):
        headers = {
            'Authorization': upload_authorization_token,
            'X-Bz-File-Name': filename,
            'Content-Type': 'b2/x-auto',
            'Content-Length': str(length),
            'X-Bz-Content-Sha1': sha1(buffer.read()).hexdigest()
        }
        buffer.seek(0)
        req = await self._session.post(url=upload_url,
                                       headers=headers,
                                       data=buffer)
        rsp = await req.json()
        return req.status, rsp


class B2(B2Bare):
    def __init__(self, application_key_id, application_key_secret):
        super().__init__(application_key_id, application_key_secret)
        self.authorization_token = cache.RedisExpiringValue('b2_authorization_token')
        self.api_url = cache.RedisExpiringValue('b2_api_url')

    async def refresh_authorization_token(self, retry=5):
        try:
            code, rsp = await self.authorize_account()
        except ClientError:
            await self.refresh_authorization_token(retry - 1)
            return
        if code != 200:
            logging.warning("refresh token failed: code %d, %r", code, rsp)
            await self.refresh_authorization_token(retry - 1)
            return
        await self.authorization_token.set(rsp['authorizationToken'])
        await self.api_url.set(rsp['apiUrl'])

    async def upload(self, bucket_id, filename, buffer: BytesIO, length):
        if not await self.authorization_token.get():
            await self.refresh_authorization_token()

        for _ in range(5):
            try:
                code, rsp = await self.get_upload_url(await self.authorization_token.get(), await self.api_url.get(), bucket_id)
            except ClientError:
                continue
            if code == 401:
                await self.refresh_authorization_token()
            if code != 200:
                logging.warning('get upload url failed: code %d, %r', code, rsp)
                continue

            upload_url = rsp['uploadUrl']
            upload_authorization_token = rsp['authorizationToken']
            break

        for _ in range(5):
            try:
                code, rsp = await self.upload_file(upload_url, upload_authorization_token, filename, buffer, length)
            except ClientError:
                continue
            if code != 200:
                logging.warning('upload failed: code %d, %r', code, rsp)
            break
