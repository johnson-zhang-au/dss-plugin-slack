from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
import dataiku
import pandas as pd
import asyncio
import time
from datetime import datetime, timedelta
from utils.logging import logger
from collections import defaultdict
import json
from dkuslackclient.message_formatter import MessageFormatter

# Start timing for performance tracking
start_time = time.time()

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack Conversation Formatter recipe.")
logger.debug(f"Recipe configuration: {config}")

# Function to get date key based on timestamp
def get_date_key(timestamp_str, period='day'):
    if not timestamp_str:
        return 'unknown'
    try:
        ts = float(timestamp_str)
        dt = datetime.fromtimestamp(ts)
        if period == 'day':
            return dt.strftime('%Y-%m-%d')
        elif period == 'week':
            # Use ISO week format (year-week number)
            return f"{dt.year}-W{dt.isocalendar()[1]:02d}"
        elif period == 'month':
            return dt.strftime('%Y-%m')
        else:
            return 'all'
    except:
        return 'unknown'

try:
    # Get parameters from the recipe configuration
    aggregate_threads = config.get('aggregate_threads', True)
    format_by = config.get('format_by', 'channel')
    group_by_channel = config.get('group_by_channel', False)
    output_format = config.get('output_format', 'markdown')
    include_metadata = config.get('include_metadata', True)
    
    # Get excluded subtypes
    excluded_subtypes = set(config.get('exclude_subtypes', []))
    
    logger.info(f"Configuration: format_by={format_by}, output_format={output_format}")
    logger.info(f"Thread aggregation: {'Enabled' if aggregate_threads else 'Disabled'}")
    logger.info(f"Include metadata: {'Enabled' if include_metadata else 'Disabled'}")
    logger.info(f"Excluded subtypes: {excluded_subtypes}")
    
    if format_by != 'channel' and group_by_channel:
        logger.info(f"Secondary channel grouping: Enabled")
    
    # Get input and output datasets
    input_name = get_input_names_for_role('input_messages')[0]
    output_name = get_output_names_for_role('formatted_output')[0]
    
    input_dataset = dataiku.Dataset(input_name)
    output_dataset = dataiku.Dataset(output_name)
    
    # Read the input dataset
    logger.info(f"Reading messages from input dataset: {input_name}")
    df = input_dataset.get_dataframe()
    
    logger.info(f"Read {len(df)} messages from input dataset")
    
    # Check if we have any messages to process
    if len(df) == 0:
        logger.warn("No messages to process. Creating empty output dataset.")
        empty_df = pd.DataFrame(columns=['channel_name', 'date', 'conversation_timeline'])
        output_dataset.write_with_schema(empty_df)
        logger.info("Recipe completed successfully with empty output.")
        exit(0)
    
    # Process messages according to format_by parameter
    logger.info(f"Processing messages grouped by: {format_by}")
    
    # Convert DataFrame to list of dictionaries for processing
    messages_list = df.to_dict('records')
    
    # Clean up None values
    for message in messages_list:
        for key, value in message.items():
            if pd.isna(value):
                message[key] = ''
    
    # Group messages based on format_by
    message_groups = defaultdict(list)
    
    if format_by == 'channel':
        # Group by channel
        for message in messages_list:
            # Use channel_name if available, fall back to channel_id, then to "unknown"
            channel_name = message.get('channel_name')
            if not channel_name or channel_name == '':
                channel_name = message.get('channel_id', 'unknown')
                if channel_name == '':
                    channel_name = 'unknown'
            message_groups[channel_name].append(message)
    elif format_by in ['day', 'week', 'month']:
        # Group by time period
        for message in messages_list:
            date_key = get_date_key(message.get('ts', ''), format_by)
            if group_by_channel:
                # When also grouping by channel, create composite keys
                channel_name = message.get('channel_name')
                if not channel_name or channel_name == '':
                    channel_name = message.get('channel_id', 'unknown')
                    if channel_name == '':
                        channel_name = 'unknown'
                group_key = f"{date_key}|{channel_name}"
                message_groups[group_key].append(message)
            else:
                # Regular date-only grouping
                message_groups[date_key].append(message)
    else:  # 'all' - put everything in one group if not grouping by channel
        if group_by_channel:
            # Group by channel within 'all'
            for message in messages_list:
                channel_name = message.get('channel_name')
                if not channel_name or channel_name == '':
                    channel_name = message.get('channel_id', 'unknown')
                    if channel_name == '':
                        channel_name = 'unknown'
                message_groups[f"all|{channel_name}"].append(message)
        else:
            # No channel grouping, just one big group
            message_groups['all_messages'] = messages_list
    
    # Format each group and prepare the output DataFrame
    result_data = []
    
    for group_key, messages in message_groups.items():
        logger.info(f"Formatting conversation for group: {group_key} with {len(messages)} messages")
        
        # Format the conversation using MessageFormatter
        formatted_conversation = MessageFormatter.format_messages(
            messages,
            format_type=output_format,
            include_meta=include_metadata,
            exclude_subtypes=excluded_subtypes
        )
        
        # Create a row for the output dataset
        row = {
            'group_key': group_key,
            'message_count': len(messages),
            'conversation_timeline': formatted_conversation
        }
        
        # Add channel and/or date information depending on grouping
        if format_by == 'channel' or (group_by_channel and '|' in group_key):
            if '|' in group_key:
                # Extract date and channel from composite key
                date_part, channel_part = group_key.split('|', 1)
                row['date'] = date_part
                row['channel_name'] = channel_part
            else:
                # Just a channel key
                row['channel_name'] = group_key
        else:
            # Just a date key
            row['date'] = group_key
        
        result_data.append(row)
    
    # Create output DataFrame
    output_df = pd.DataFrame(result_data)
    
    # Write to the output dataset
    logger.info(f"Writing {len(output_df)} formatted conversations to output dataset")
    output_dataset.write_with_schema(output_df)
    
    # Log overall performance
    total_duration = time.time() - start_time
    logger.info(f"Recipe completed successfully in {total_duration:.2f} seconds")

except Exception as e:
    logger.error(f"Recipe failed: {str(e)}", exc_info=True)
    raise

logger.info("Slack Conversation Formatter recipe completed.") 