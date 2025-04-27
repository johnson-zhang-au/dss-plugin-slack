from dataiku.customrecipe import get_output_names_for_role, get_recipe_config
from datetime import datetime, timedelta
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
date_range_type = config.get('date_range_type', 'period')
period = config.get('period', '1mo')
start_date = config.get('start_date')
channel_names = config.get('channel_names', [])
user_emails = config.get('user_emails', [])
include_private_channels = config.get('include_private_channels', False)

# Define period to days mapping
PERIOD_DAYS = {
    '1d': 1,
    '5d': 5,
    '1mo': 30,
    '3mo': 90,
    '6mo': 180,
    '1y': 365,
    '2y': 730,
    '5y': 1825,
    '10y': 3650
}
# Calculate start timestamp based on date range type
if date_range_type == 'period':
    now = datetime.now()
    if period == 'ytd':
        start_timestamp = datetime(now.year, 1, 1).timestamp()
    elif period == 'max':
        start_timestamp = 0  # Unix epoch start
    elif period in PERIOD_DAYS:
        start_timestamp = (now - timedelta(days=PERIOD_DAYS[period])).timestamp()
    else:
        raise ValueError(f"Unsupported period: {period}")
elif start_date:
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
else:
    raise ValueError("Either period or start date is required")

# Fetch messages
messages = asyncio.run(slack_chat_bot.fetch_messages_from_channels(
    start_timestamp, 
    channel_names, 
    user_emails,
    include_private_channels=include_private_channels
))
logger.info(f"Fetched {len(messages)} messages")

# Define the schema for Slack messages
slack_message_schema = {
    'type': str,
    'user': str,
    'text': str,
    'ts': str,
    'team': str,
    'channel_id': str,
    'channel_name': str,
    'subtype': str,
    'bot_id': str,
    'bot_profile': str,
    'blocks': list,
    'response_type': str,
    'thread_ts': str
}

# Create DataFrame with the correct schema
if messages:
    df = pd.DataFrame(messages)
else:
    # Create empty DataFrame with the correct schema
    df = pd.DataFrame(columns=slack_message_schema.keys())
    # Set the correct data types
    for col, dtype in slack_message_schema.items():
        df[col] = df[col].astype(dtype)

# Get the output dataset
output_name = get_output_names_for_role('data_output')[0]
output_dataset = dataiku.Dataset(output_name)

# Write messages to the output dataset
output_dataset.write_with_schema(df)
logger.info(f"Successfully wrote {len(messages)} messages to {output_name}")