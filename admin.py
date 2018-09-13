import asyncio
import subprocess
from logging import getLogger

from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import InviteHashInvalidError, FloodWaitError, UserNotParticipantError
from aiogram import Bot
from aiogram.types import Message

import senders
import utils
import discover
import models


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
    engine = await models.get_aio_engine()
    output = await discover.test_and_join_public_channel(engine, text)
    return str(output)


async def join_private_group_handler(bot: Bot, message: Message, text: str):
    try:
        output = await senders.invoker(ImportChatInviteRequest(text))
    except (InviteHashInvalidError, FloodWaitError) as e:
        output = str(type(e)) + ':' + repr(e)

    return str(output)


async def leave_group_handler(bot: Bot, message: Message, text: str):
    try:
        link = int(text)
    except ValueError:
        link = text
        pass
    logger.info('leaving public group %s', link)
    try:
        output = await senders.invoker(LeaveChannelRequest(await senders.invoker.get_input_entity(link)))
    except UserNotParticipantError:
        return 'user not in group %s' % text
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
