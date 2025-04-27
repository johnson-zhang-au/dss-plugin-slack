from dataiku.customrecipe import get_output_names_for_role, get_recipe_config
from datetime import datetime
import os
import asyncio
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from slackchat.slack_bot import SlackChatBot
import dataiku
import pandas as pd
from utils.logging import logger  # Import the LazyLogger instance

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack message fetcher recipe.")

slack_bot_auth = config.get("bot_auth_settings", {}) 
# Initialize the SlackChatBot
slack_chat_bot = SlackChatBot(slack_bot_auth)

# Get parameters from the recipe configuration
start_date = config.get('start_date')
channel_names = config.get('channel_names', [])
user_emails = config.get('user_emails', [])

# Convert start_date to timestamp
if start_date:
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
else:
    raise ValueError("Start date is required")

# Fetch messages
messages = asyncio.run(slack_chat_bot.fetch_messages_from_channels(start_timestamp, channel_names, user_emails))
logger.info(f"Fetched {len(messages)} messages")

# Convert list of messages to a DataFrame
df = pd.DataFrame(messages)

# Get the output dataset
output_name = get_output_names_for_role('data_output')[0]
output_dataset = dataiku.Dataset(output_name)

# Write messages to the output dataset
output_dataset.write_with_schema(df)
logger.info(f"Successfully wrote {len(messages)} messages to {output_name}")