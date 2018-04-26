import http.server
import cgi
import utils
import logging
import config

logger = logging.getLogger(__name__)


class SmsHandler(http.server.BaseHTTPRequestHandler):
    server_version = 'WebhookHandler/1.0'

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })
        sender = form.getvalue('From', '<unknown number>')
        me = form.getvalue('To', '<unknown number>')
        body = form.getvalue('Body', '<unknown message>')
        logger.warning(f'Received SMS from {sender} to {me}: \n{body}')
        utils.send_message_to_administrators(f'Received SMS from {sender} to {me}: \n{body}')
        self.send_response(204)
        self.end_headers()


def main():
    server = http.server.HTTPServer((config.SMS_WEBHOOK_LISTEN, config.SMS_WEBHOOK_PORT), SmsHandler)
    logger.info('SMS Webhook server started')
    server.serve_forever()


if __name__ == '__main__':
    main()
