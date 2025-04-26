from dataiku.customrecipe import get_output_names_for_role, get_recipe_config
from datetime import datetime
import logging
import os
import asyncio
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from slack.slack_bot import SlackChatBot
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



# Get parameters from the recipe configuration
start_date = config.get('start_date')
channel_ids = config.get('channel_ids', [])
user_ids = config.get('user_ids', [])

# Convert start_date to timestamp
if start_date:
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
else:
    raise ValueError("Start date is required")


async def main():
    # Fetch messages
    messages = await slack_chat_bot.fetch_messages_from_channels(start_timestamp, channel_ids, user_ids)
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