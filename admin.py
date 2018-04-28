from logging import getLogger
from os import popen

from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telegram import Bot, Update

from senders import invoker, clients
import utils


logger = getLogger(__name__)


def execute_command_handler(bot: Bot, update: Update, text: str):
    logger.info('executing command %s', text)
    with popen(text) as f:
        return f.read()


def evaluate_script_handler(bot: Bot, update: Update, text: str):
    logger.info('evaluating script %s', text)
    return repr(eval(text))


def join_public_group_handler(bot: Bot, update: Update, text: str):
    logger.info('joining public group %s', text)
    output = invoker.invoke(JoinChannelRequest(invoker.get_entity(text)))
    return str(output)


def join_private_group_handler(bot: Bot, update: Update, text: str):
    output = invoker.invoke(ImportChatInviteRequest(text))
    return str(output)


def leave_group_handler(bot: Bot, update: Update, text: str):
    try:
        link = int(text)
    except ValueError:
        link = text
        pass
    logger.info('leaving public group %s', link)
    output = invoker.invoke(LeaveChannelRequest(invoker.get_entity(link)))
    return str(output)


def dialogs_handler(bot: Bot, update: Update, text: str):
    result = ''
    for client in clients:
        uid = client.get_me(input_peer=True).user_id
        result += f'-- For client with UID {uid}\n'
        for dialog in client.get_dialogs():
            result += f'UPDATE groups SET master = {uid} WHERE gid = {dialog.id};\n'
    result += '-- Generation complete\n'
    utils.send_message_to_administrators(result)

    return 'Generation complete'
