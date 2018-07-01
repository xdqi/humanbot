import utils
import logging
import config

from aiohttp import web
from twilio.twiml.voice_response import VoiceResponse
from twilio.twiml.messaging_response import MessagingResponse

logger = logging.getLogger(__name__)
app = web.Application()
routes = web.RouteTableDef()


@routes.post(config.VOICE_WEBHOOK_PATH)
async def record(request):

    post_data = await request.post()
    data = {**post_data, **request.GET}

    sender = data.get('From', '<unknown number>')
    me = data.get('To', '<unknown number>')
    logger.warning(f'Recorded from {sender} to {me}.')
    await utils.send_to_admin_channel(f'Recorded voice from {sender} to {me}.')

    response = VoiceResponse()
    response.record()
    response.hangup()
    return web.Response(text=str(response), content_type='text/xml')


@routes.post(config.SMS_WEBHOOK_PATH)
async def sms(request):
    post_data = await request.post()
    data = {**post_data, **request.GET}

    sender = data.get('From', '<unknown number>')
    me = data.get('To', '<unknown number>')
    body = data.get('Body', '<unknown message>')

    logger.warning(f'Received SMS from {sender} to {me}: \n{body}')
    await utils.send_to_admin_channel(f'Received SMS from {sender} to {me}: \n{body}')
    return web.Response(text=str(str(MessagingResponse())), content_type='text/xml')


async def main():
    logger.info('Bot, SMS and Audio webhook server started')
    app.add_routes(routes)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=config.SMS_WEBHOOK_LISTEN, port=config.SMS_WEBHOOK_PORT)
    await site.start()
