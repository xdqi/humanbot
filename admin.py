import asyncio
import subprocess
from logging import getLogger

from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from aiogram import Bot
from aiogram.types import Message

import senders
import utils


logger = getLogger(__name__)


async def execute_command_handler(bot: Bot, message: Message, text: str):
    logger.info('executing command %s', text)
    process = await asyncio.create_subprocess_shell(text, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = await process.communicate()
    return '<pre>' + utils.tg_html_entity(stdout.decode()) + '</pre>'


async def evaluate_script_handler(bot: Bot, message: Message, text: str):
    logger.info('evaluating script %s', text)
    try:
        result = repr(eval(text))
    except (BaseException, SyntaxError) as e:
        result = str(type(e)) + ': ' + repr(e)
    return '<pre>' + utils.tg_html_entity(result) + '</pre>'


async def join_public_group_handler(bot: Bot, message: Message, text: str):
    logger.info('joining public group %s', text)
    output = await senders.invoker(JoinChannelRequest(await senders.invoker.get_input_entity(text)))
    return str(output)


async def join_private_group_handler(bot: Bot, message: Message, text: str):
    output = await senders.invoker(ImportChatInviteRequest(text))
    return str(output)


async def leave_group_handler(bot: Bot, message: Message, text: str):
    try:
        link = int(text)
    except ValueError:
        link = text
        pass
    logger.info('leaving public group %s', link)
    output = await senders.invoker(LeaveChannelRequest(await senders.invoker.get_input_entity(link)))
    return str(output)


async def dialogs_handler(bot: Bot, message: Message, text: str):
    result = ''
    for uid, client in senders.clients.items():
        if isinstance(client, Bot):
            continue
        result += f'-- For client with UID {uid}\n'
        for dialog in await client.get_dialogs():
            result += f'UPDATE groups SET master = {uid} WHERE gid = {dialog.id};\n'
    result += '-- Generation complete\n'
    await utils.send_to_admin_channel(result)

    return 'Generation complete'
