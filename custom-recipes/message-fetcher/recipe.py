from dataiku.customrecipe import get_output_names_for_role, get_recipe_config
from datetime import datetime
import logging
import os
import asyncio
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from slack.slack_bot import SlackBot
import dataiku

# Get the recipe configuration
config = get_recipe_config()

# Set up logging
logging_level = config.get('logging_level', "INFO")

# Map string levels to logging constants
level_mapping = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

level = level_mapping.get(logging_level, logging.INFO)  # Default to INFO if not found
logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up the logger with the script name
script_name = os.path.basename(__file__).split('.')[0]
logger = logging.getLogger(script_name)
logger.setLevel(level)


slack_bot_auth = config.get("bot_auth_settings", {}) 
# Initialize the SlackChatBot
slack_chat_bot = SlackChatBot(slack_bot_auth)

# Slack token from environment variable
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
if not SLACK_BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN environment variable is not set")

slack_client = AsyncWebClient(token=SLACK_BOT_TOKEN)

# Get parameters from the recipe configuration
start_date = config.get('start_date')
channel_ids = config.get('channel_ids', [])
user_ids = config.get('user_ids', [])

# Convert start_date to timestamp
if start_date:
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
else:
    raise ValueError("Start date is required")

async def fetch_channels():
    """Fetch all channels the bot has access to."""
    try:
        response = await slack_client.conversations_list(types="public_channel,private_channel")
        return response.get("channels", [])
    except SlackApiError as e:
        logger.error(f"Error fetching channels: {e.response['error']}")
        return []

async def fetch_messages(channel_id, start_timestamp, user_ids):
    """Fetch messages from a specific channel."""
    try:
        messages = []
        next_cursor = None

        while True:
            response = await slack_client.conversations_history(
                channel=channel_id,
                oldest=start_timestamp,
                limit=200,
                cursor=next_cursor
            )
            for message in response.get("messages", []):
                if not user_ids or any(user_id in message.get("user", "") for user_id in user_ids):
                    messages.append(message)

            next_cursor = response.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break

        return messages
    except SlackApiError as e:
        logger.error(f"Error fetching messages from channel {channel_id}: {e.response['error']}")
        return []

async def fetch_messages_from_channels(start_timestamp, channel_ids=None, user_ids=None):
    """Fetch messages from specified channels or all channels."""
    channels = await fetch_channels()

    if channel_ids:
        channels = [channel for channel in channels if channel["id"] in channel_ids]

    tasks = []
    for channel in channels:
        tasks.append(fetch_messages(channel["id"], start_timestamp, user_ids))

    results = await asyncio.gather(*tasks)
    all_messages = [message for channel_messages in results for message in channel_messages]
    return all_messages

async def main():
    # Fetch messages
    messages = await fetch_messages_from_channels(start_timestamp, channel_ids, user_ids)
    logger.info(f"Fetched {len(messages)} messages")

    # Get the output dataset
    output_name = get_output_names_for_role('data_output')[0]
    output_dataset = dataiku.Dataset(output_name)

    # Write messages to the output dataset
    output_dataset.write_with_schema(messages)
    logger.info(f"Successfully wrote {len(messages)} messages to {output_name}")

# Run the main function
try:
    asyncio.run(main())
except Exception as e:
    logger.error(f"Error fetching Slack messages: {str(e)}")
    raise