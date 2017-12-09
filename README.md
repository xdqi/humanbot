# humanbot

Telegram bot that records all messages.

## Prerequisites

Obtain a Telegram API key at https://core.telegram.org/api/obtaining_api_id

Register a Telegram account with Telegram client


## Installation

Installing libraries:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

Initialising database:

`python3 models.py`

Modifying configuration and then:

`mv config.py.sample config.py`

## Start

`python3 humanbot.py`
