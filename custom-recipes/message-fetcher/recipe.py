from dataiku.customrecipe import get_output_names_for_role, get_recipe_config
from datetime import datetime, timedelta
import os
import asyncio
import time
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from slackchat.slack_bot import SlackChatBot
import dataiku
import pandas as pd
from utils.logging import logger  # Import the LazyLogger instance

# Start timing for performance tracking
start_time = time.time()

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack message fetcher recipe.")
logger.debug(f"Recipe configuration: {config}")

try:
    # Get bot authentication settings
    slack_bot_auth = config.get("bot_auth_settings", {})
    if not slack_bot_auth:
        logger.error("Bot authentication settings are missing or empty")
        raise ValueError("Bot authentication settings are required")
    
    logger.debug("Initializing SlackChatBot")
    # Initialize the SlackChatBot
    slack_chat_bot = SlackChatBot(slack_bot_auth)
    
    # Get parameters from the recipe configuration
    date_range_type = config.get('date_range_type', 'period')
    channel_id_or_name = config.get('channel_id_or_name', 'id')
    period = config.get('period', '1mo')
    start_date = config.get('start_date')
    channel_names = config.get('channel_names', [])
    channel_ids = config.get('channel_ids', [])
    user_emails = config.get('user_emails', [])
    include_private_channels = config.get('include_private_channels', False)
    resolve_users = config.get('resolve_users', True)
    
    logger.info(f"Configuration: date_range_type={date_range_type}, channel_id_or_name={channel_id_or_name}")
    logger.info(f"Channel filtering: {len(channel_ids) if channel_ids else 0} channel IDs, {len(channel_names) if channel_names else 0} channel names")
    logger.info(f"User filtering: {len(user_emails) if user_emails else 0} user emails")
    logger.info(f"User resolution: {'Enabled' if resolve_users else 'Disabled'}")
    logger.info(f"Private channels: {'Included' if include_private_channels else 'Excluded'}")
    
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
    now = datetime.now()
    
    if date_range_type == 'period':
        if period == 'ytd':
            start_timestamp = datetime(now.year, 1, 1).timestamp()
            start_date_readable = datetime(now.year, 1, 1).strftime("%Y-%m-%d")
        elif period == 'max':
            start_timestamp = 0  # Unix epoch start
            start_date_readable = "1970-01-01"
        elif period in PERIOD_DAYS:
            start_date = now - timedelta(days=PERIOD_DAYS[period])
            start_timestamp = start_date.timestamp()
            start_date_readable = start_date.strftime("%Y-%m-%d")
        else:
            logger.error(f"Unsupported period: {period}")
            raise ValueError(f"Unsupported period: {period}")
    elif date_range_type == 'custom' and start_date:
        try:
            parsed_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            start_timestamp = parsed_date.timestamp()
            start_date_readable = parsed_date.strftime("%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Failed to parse start date '{start_date}': {e}")
            raise ValueError(f"Failed to parse start date '{start_date}': {e}")
    else:
        logger.error("Either period or start date is required")
        raise ValueError("Either period or start date is required")
    
    logger.info(f"Fetching messages from {start_date_readable} to present")
    logger.debug(f"Start timestamp: {start_timestamp}")
    
    # Initialize start time for message fetching performance tracking
    fetch_start_time = time.time()
    
    # Prepare args for fetch_messages_from_channels
    fetch_args = {
        'start_timestamp': start_timestamp,
        'user_emails': user_emails,
        'include_private_channels': include_private_channels,
        'resolve_users': resolve_users
    }
    
    # Fetch messages
    if channel_id_or_name == 'id':
        if not channel_ids:
            logger.warn("No channel IDs provided. Will fetch from all accessible channels.")
        else:
            logger.info(f"Fetching messages by channel IDs: {channel_ids}")
        
        fetch_args['channel_ids'] = channel_ids
        fetch_args['channel_names'] = None
    elif channel_id_or_name == 'name':
        if not channel_names:
            logger.warn("No channel names provided. Will fetch from all accessible channels.")
        else:
            logger.info(f"Fetching messages by channel names: {channel_names}")
        
        fetch_args['channel_names'] = channel_names
        fetch_args['channel_ids'] = None
    else:
        logger.error(f"Invalid channel_id_or_name: {channel_id_or_name}")
        raise ValueError("Can only be filtered either by channel IDs or names")
    
    try:
        messages = asyncio.run(slack_chat_bot.fetch_messages_from_channels(**fetch_args))
        
        # Calculate fetch duration
        fetch_duration = time.time() - fetch_start_time
        logger.info(f"Fetched {len(messages)} messages in {fetch_duration:.2f} seconds")
        logger.debug(f"Average time per message: {fetch_duration / max(len(messages), 1):.4f} seconds")
        
        if not messages:
            logger.warn("No messages were fetched. Check filters and date range.")
        
        # Start time for DataFrame processing
        df_start_time = time.time()
        
        # Define the schema for Slack messages with all potential fields
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
            'thread_ts': str
        }
        
        # Add user fields if users are resolved
        if resolve_users:
            user_fields = {
                'user_name': str,
                'user_email': str
            }
            slack_message_schema.update(user_fields)
        
        # Create DataFrame
        if messages:
            logger.debug("Creating DataFrame from messages")
            
            # Ensure complex fields are serialized to strings
            for message in messages:
                for key, value in list(message.items()):
                    if value is None:
                        message[key] = ''
                    elif isinstance(value, (dict, list)) and key not in ['reply_users_info']:
                        message[key] = str(value)
                
                """ 
                # Remove any internal fields not meant for output
                keys_to_remove = []
                for key in message:
                    if key not in slack_message_schema and key != 'reply_users_info':
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    del message[key]
                """
            
            df = pd.DataFrame(messages)
            
            # Ensure all schema columns exist
            for col in slack_message_schema:
                if col not in df.columns:
                    df[col] = None
        else:
            # Create empty DataFrame with the correct schema
            logger.warn("Creating empty DataFrame as no messages were found")
            df = pd.DataFrame(columns=slack_message_schema.keys())
            
        # Set the correct data types
        for col, dtype in slack_message_schema.items():
            if col in df.columns:
                # Convert None to empty string for string columns before setting type
                if dtype == str:
                    df[col] = df[col].fillna('')
                df[col] = df[col].astype(dtype)
        
        # Log DataFrame info
        logger.debug(f"DataFrame shape: {df.shape}")
        logger.debug(f"DataFrame columns: {df.columns.tolist()}")
        
        # Calculate DataFrame processing time
        df_duration = time.time() - df_start_time
        logger.debug(f"DataFrame processing completed in {df_duration:.2f} seconds")
        
        # Get the output dataset
        output_name = get_output_names_for_role('data_output')[0]
        output_dataset = dataiku.Dataset(output_name)
        
        # Write messages to the output dataset
        logger.info(f"Writing {len(df)} rows to dataset {output_name}")
        write_start_time = time.time()
        output_dataset.write_with_schema(df)
        write_duration = time.time() - write_start_time
        logger.info(f"Successfully wrote {len(df)} messages to {output_name} in {write_duration:.2f} seconds")
        
        # Log overall performance
        total_duration = time.time() - start_time
        logger.info(f"Recipe completed successfully in {total_duration:.2f} seconds")
        if len(messages) > 0:
            logger.debug(f"Performance metrics:")
            logger.debug(f"  - Message fetching: {fetch_duration:.2f}s ({fetch_duration/total_duration*100:.1f}%)")
            logger.debug(f"  - DataFrame processing: {df_duration:.2f}s ({df_duration/total_duration*100:.1f}%)")
            logger.debug(f"  - Dataset writing: {write_duration:.2f}s ({write_duration/total_duration*100:.1f}%)")
    
    except SlackApiError as e:
        logger.error(f"Slack API error: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error fetching messages: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Recipe failed: {e}", exc_info=True)
    raise

logger.info("Slack message fetcher recipe completed.")