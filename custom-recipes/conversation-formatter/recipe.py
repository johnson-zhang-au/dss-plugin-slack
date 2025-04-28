from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
import dataiku
import pandas as pd
import asyncio
import time
from datetime import datetime, timedelta
from utils.logging import logger
from collections import defaultdict
import json

# Start timing for performance tracking
start_time = time.time()

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack Conversation Formatter recipe.")
logger.debug(f"Recipe configuration: {config}")

try:
    # Get parameters from the recipe configuration
    aggregate_threads = config.get('aggregate_threads', True)
    format_by = config.get('format_by', 'channel')
    output_format = config.get('output_format', 'markdown')
    include_metadata = config.get('include_metadata', True)
    
    logger.info(f"Configuration: format_by={format_by}, output_format={output_format}")
    logger.info(f"Thread aggregation: {'Enabled' if aggregate_threads else 'Disabled'}")
    logger.info(f"Include metadata: {'Enabled' if include_metadata else 'Disabled'}")
    
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
    
    # Function to aggregate thread replies with parent messages
    def aggregate_thread_replies(messages_input):
        """
        Aggregates thread replies with their parent messages.
        
        Parameters:
        -----------
        messages_input : pd.DataFrame or list of dict
            Messages to aggregate, either as a pandas DataFrame or a list of message dictionaries.
            
        Returns:
        --------
        list of dict
            Messages with thread replies aggregated under parent messages.
        """
        # Handle different input types
        if isinstance(messages_input, pd.DataFrame):
            logger.info(f"Aggregating thread replies for {len(messages_input)} messages from DataFrame")
            # Convert DataFrame to list of dictionaries for easier processing
            messages = messages_input.to_dict('records')
            
            # Clean up None values first
            for message in messages:
                for key, value in message.items():
                    if pd.isna(value):
                        message[key] = ''
        else:
            # Assume it's already a list of message dictionaries
            logger.info(f"Aggregating thread replies for {len(messages_input)} messages from list")
            messages = messages_input
            
            # Still clean up None values for consistency
            for message in messages:
                for key, value in message.items():
                    if value is None or (hasattr(pd, 'isna') and pd.isna(value)):
                        message[key] = ''
        
        # Create collections to hold different types of messages
        regular_messages = []  # Messages not part of any thread
        thread_parents = {}    # Thread parent messages, keyed by thread_ts
        thread_replies = {}    # Thread replies, grouped by thread_ts
        
        # First pass: categorize messages
        for message in messages:
            # If a message has thread_ts, it's part of a thread
            if message.get('thread_ts') and message.get('thread_ts') != '':
                thread_ts = message.get('thread_ts')
                
                # If thread_ts equals ts, this is the parent message of the thread
                if thread_ts == message.get('ts'):
                    # Use the same object but with a clearer variable name
                    parent_message = message
                    thread_parents[thread_ts] = parent_message
                else:
                    # This is a reply in a thread
                    if thread_ts not in thread_replies:
                        thread_replies[thread_ts] = []
                    thread_replies[thread_ts].append(message)
            else:
                # Not part of any thread, just a regular message
                regular_messages.append(message)
        
        # Second pass: build result by combining messages with their replies
        result_messages = []
        
        # Add regular messages
        result_messages.extend(regular_messages)
        
        # Process thread parents and add replies
        for thread_ts, parent in thread_parents.items():
            # Add thread_replies field to parent message
            if thread_ts in thread_replies:
                # Sort replies by timestamp
                replies = sorted(
                    thread_replies[thread_ts], 
                    key=lambda x: float(x.get('ts', 0))
                )
                parent['thread_replies'] = replies
                logger.debug(f"Thread {thread_ts} has {len(replies)} replies")
            else:
                parent['thread_replies'] = []
            
            # Add parent with its replies to result
            result_messages.append(parent)
        
        # Sort all messages by timestamp
        result_messages = sorted(result_messages, key=lambda x: float(x.get('ts', 0)))
        
        logger.info(f"Found {sum(len(replies) for replies in thread_replies.values())} thread replies for {len(thread_parents)} parent messages")
        return result_messages
    
    # Function to format messages into a conversation timeline
    def format_conversation(messages, format_type='markdown', include_meta=True):
        """
        Formats messages and their thread replies into a chronological timeline.
        
        :param messages: List of message objects
        :param format_type: 'markdown', 'text', or 'json'
        :param include_meta: Whether to include metadata like timestamps and user info
        :return: String containing the formatted conversation
        """
        logger.info(f"Formatting {len(messages)} messages into {format_type} conversation timeline")
        
        # Sort all messages by timestamp
        sorted_messages = sorted(messages, key=lambda x: float(x.get('ts', 0)))
        
        if format_type == 'json':
            # For JSON format, just structure the data and return as JSON
            formatted_data = []
            for message in sorted_messages:
                msg_data = {
                    'text': message.get('text', ''),
                    'timestamp': message.get('ts', ''),
                    'user': message.get('user_name') or message.get('user', 'Unknown User') if include_meta else None
                }
                
                if 'thread_replies' in message and message['thread_replies']:
                    msg_data['replies'] = []
                    for reply in message['thread_replies']:
                        reply_data = {
                            'text': reply.get('text', ''),
                            'timestamp': reply.get('ts', ''),
                            'user': reply.get('user_name') or reply.get('user', 'Unknown User') if include_meta else None
                        }
                        msg_data['replies'].append(reply_data)
                
                formatted_data.append(msg_data)
            
            return json.dumps(formatted_data, indent=2)
        
        # For markdown and text formats
        formatted_output = []
        
        if format_type == 'markdown':
            formatted_output.append("# Conversation Timeline\n")
        else:  # Plain text
            formatted_output.append("CONVERSATION TIMELINE\n" + "="*21 + "\n")
        
        for message in sorted_messages:
            # Format timestamp as readable date/time if metadata is included
            time_str = ""
            user_name = ""
            
            if include_meta:
                ts = float(message.get('ts', 0))
                dt = datetime.fromtimestamp(ts)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                user_name = message.get('user_name') or message.get('user', 'Unknown User')
            
            # Format main message
            if format_type == 'markdown':
                if include_meta:
                    formatted_output.append(f"## {time_str} - {user_name}")
                formatted_output.append(f"{message.get('text', '')}\n")
            else:  # Plain text
                if include_meta:
                    formatted_output.append(f"{time_str} - {user_name}")
                    formatted_output.append("-" * 40)
                formatted_output.append(f"{message.get('text', '')}\n")
            
            # Format thread replies if present
            if 'thread_replies' in message and message['thread_replies']:
                if format_type == 'markdown':
                    formatted_output.append("### Thread Replies:")
                else:  # Plain text
                    formatted_output.append("Thread Replies:")
                    formatted_output.append("-" * 15)
                
                for reply in message['thread_replies']:
                    # Format reply timestamp and user info if metadata is included
                    reply_prefix = ""
                    if include_meta:
                        reply_ts = float(reply.get('ts', 0))
                        reply_dt = datetime.fromtimestamp(reply_ts)
                        reply_time = reply_dt.strftime("%Y-%m-%d %H:%M:%S")
                        reply_user = reply.get('user_name') or reply.get('user', 'Unknown User')
                        
                        if format_type == 'markdown':
                            reply_prefix = f"- **{reply_time} - {reply_user}**: "
                        else:  # Plain text
                            reply_prefix = f"  {reply_time} - {reply_user}: "
                    else:
                        reply_prefix = "- " if format_type == 'markdown' else "  "
                    
                    # Format reply
                    formatted_output.append(f"{reply_prefix}{reply.get('text', '')}")
                
                formatted_output.append("")  # Add blank line after thread
        
        # Join all lines into a single string
        return "\n".join(formatted_output)
    
    # Process messages according to format_by parameter
    logger.info(f"Processing messages grouped by: {format_by}")
    
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
    
    # Aggregate thread replies if requested
    if aggregate_threads:
        messages_list = aggregate_thread_replies(df)
    else:
        # Process DataFrame rows directly without to_dict conversion
        messages_list = []
        for idx, row in df.iterrows():
            # Create a message dict with None values cleaned up
            message = {}
            for col in df.columns:
                value = row[col]
                message[col] = '' if pd.isna(value) else value
            messages_list.append(message)
    
    # Group messages based on format_by
    message_groups = defaultdict(list)
    
    if format_by == 'channel':
        # Group by channel
        for message in messages_list:
            channel_name = message.get('channel_name', 'unknown')
            message_groups[channel_name].append(message)
    elif format_by in ['day', 'week', 'month']:
        # Group by time period
        for message in messages_list:
            date_key = get_date_key(message.get('ts', ''), format_by)
            message_groups[date_key].append(message)
    else:  # 'all' - put everything in one group
        message_groups['all_messages'] = messages_list
    
    # Format each group and prepare the output DataFrame
    result_data = []
    
    for group_key, messages in message_groups.items():
        logger.info(f"Formatting conversation for group: {group_key} with {len(messages)} messages")
        
        # Format the conversation for this group
        formatted_conversation = format_conversation(
            messages,
            format_type=output_format,
            include_meta=include_metadata
        )
        
        # Create a row for the output dataset
        row = {
            'group_key': group_key,
            'message_count': len(messages),
            'conversation_timeline': formatted_conversation
        }
        
        # Add channel or date information
        if format_by == 'channel':
            row['channel_name'] = group_key
        else:
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