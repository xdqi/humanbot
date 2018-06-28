from logging import getLogger
from os import popen

from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telegram import Bot, Update

import senders
import utils


logger = getLogger(__name__)


def execute_command_handler(bot: Bot, update: Update, text: str):
    logger.info('executing command %s', text)
    with popen(text) as f:
        return '<pre>' + utils.tg_html_entity(f.read()) + '</pre>'


def evaluate_script_handler(bot: Bot, update: Update, text: str):
    logger.info('evaluating script %s', text)
    return '<pre>' + utils.tg_html_entity(repr(eval(text))) + '</pre>'


def join_public_group_handler(bot: Bot, update: Update, text: str):
    logger.info('joining public group %s', text)
    output = senders.invoker(JoinChannelRequest(senders.invoker.get_entity(text)))
    return str(output)


def join_private_group_handler(bot: Bot, update: Update, text: str):
    output = senders.invoker(ImportChatInviteRequest(text))
    return str(output)


def leave_group_handler(bot: Bot, update: Update, text: str):
    try:
        link = int(text)
    except ValueError:
        link = text
        pass
    logger.info('leaving public group %s', link)
    output = senders.invoker(LeaveChannelRequest(senders.invoker.get_entity(link)))
    return str(output)


def dialogs_handler(bot: Bot, update: Update, text: str):
    result = ''
    for uid, client in senders.clients.items():
        if isinstance(client, Bot):
            continue
        result += f'-- For client with UID {uid}\n'
        for dialog in client.get_dialogs():
            result += f'UPDATE groups SET master = {uid} WHERE gid = {dialog.id};\n'
    result += '-- Generation complete\n'
    utils.send_to_admin_channel(result)

    return 'Generation complete'
