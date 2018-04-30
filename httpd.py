import utils
import logging
import config

from flask import Flask, request
from gevent.wsgi import WSGIServer
from twilio.twiml.voice_response import VoiceResponse
from twilio.twiml.messaging_response import MessagingResponse


logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route(config.VOICE_WEBHOOK_PATH, methods=['POST'])
def record():
    sender = request.values.get('From', '<unknown number>')
    me = request.values.get('To', '<unknown number>')
    logger.warning(f'Recorded from {sender} to {me}.')
    utils.send_to_admin_channel(f'Recorded voice from {sender} to {me}.')

    response = VoiceResponse()
    response.record()
    response.hangup()
    return str(response)


@app.route(config.SMS_WEBHOOK_PATH, methods=['POST'])
def sms():
    sender = request.values.get('From', '<unknown number>')
    me = request.values.get('To', '<unknown number>')
    body = request.values.get('Body', '<unknown message>')

    logger.warning(f'Received SMS from {sender} to {me}: \n{body}')
    utils.send_to_admin_channel(f'Received SMS from {sender} to {me}: \n{body}')
    return str(MessagingResponse())


def main():
    logger.info('Bot, SMS and Audio webhook server started')
    server = WSGIServer((config.SMS_WEBHOOK_LISTEN, config.SMS_WEBHOOK_PORT), app)
    server.serve_forever()
