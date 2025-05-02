from datetime import datetime
from utils.logging import logger

class MessageFormatter:
    """
    A standalone class for formatting Slack messages into various formats.
    This class handles the core message formatting functionality without any external dependencies.
    """
    
    # Constants for message subtypes that are considered noise
    NOISE_SUBTYPES = {
        'channel_join',
        'channel_leave',
        'tombstone',
        'bot_message',
        'channel_archive',
        'channel_unarchive'
    }
    
    @staticmethod
    def aggregate_thread_replies(messages):
        """
        Aggregates thread replies with their parent messages.
        
        Parameters:
        -----------
        messages : list of dict
            Messages to aggregate as a list of message dictionaries.
            
        Returns:
        --------
        list of dict
            Messages with thread replies aggregated under parent messages.
        """
        logger.info(f"Aggregating thread replies for {len(messages)} messages")
        
        # Clean up None values
        for message in messages:
            for key, value in message.items():
                if value is None:
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
    
    @staticmethod
    def format_messages(messages, format_type='json', include_meta=True, exclude_subtypes=None):
        """
        Format messages into a clean, consistent structure.
        
        :param messages: List of message objects
        :param format_type: 'json', 'markdown', or 'text'
        :param include_meta: Whether to include metadata like timestamps and user info
        :param exclude_subtypes: Set of message subtypes to exclude (default: NOISE_SUBTYPES)
        :return: Formatted messages (string for markdown/text, list for JSON)
        """
        logger.info(f"Formatting {len(messages)} messages into {format_type} format")
        
        # Use default noise subtypes if none provided
        if exclude_subtypes is None:
            exclude_subtypes = MessageFormatter.NOISE_SUBTYPES
        
        # Filter out messages with excluded subtypes
        filtered_messages = [
            msg for msg in messages 
            if not (msg.get('subtype') and msg.get('subtype') in exclude_subtypes)
        ]
        
        if len(filtered_messages) < len(messages):
            logger.info(f"Filtered out {len(messages) - len(filtered_messages)} messages with excluded subtypes")
        
        # First aggregate thread replies
        aggregated_messages = MessageFormatter.aggregate_thread_replies(filtered_messages)
        
        # Sort all messages by timestamp
        sorted_messages = sorted(aggregated_messages, key=lambda x: float(x.get('ts', 0)))
        
        if format_type == 'json':
            # For JSON format, return a list of formatted message objects
            formatted_messages = []
            for message in sorted_messages:
                # Get formatted timestamp
                formatted_time = ""
                if include_meta and message.get('ts'):
                    ts = float(message.get('ts', 0))
                    dt = datetime.fromtimestamp(ts)
                    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                
                # Get channel information
                channel_info = None
                if include_meta:
                    channel_name = message.get('channel_name')
                    if not channel_name or channel_name == '':
                        channel_name = message.get('channel_id', 'unknown')
                        if channel_name == '':
                            channel_name = 'unknown'
                    channel_info = channel_name
                
                # Create formatted message
                formatted_message = {
                    'text': message.get('text', ''),
                    'timestamp': message.get('ts', ''),
                    'formatted_time': formatted_time if include_meta else None,
                    'user': message.get('user_name') or message.get('user', 'Unknown User') if include_meta else None,
                    'channel': channel_info
                }
                
                # Add thread replies if present
                if 'thread_replies' in message and message['thread_replies']:
                    formatted_message['replies'] = []
                    for reply in message['thread_replies']:
                        # Get formatted timestamp for the reply
                        reply_formatted_time = ""
                        if include_meta and reply.get('ts'):
                            reply_ts = float(reply.get('ts', 0))
                            reply_dt = datetime.fromtimestamp(reply_ts)
                            reply_formatted_time = reply_dt.strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Get channel information for reply
                        reply_channel_info = None
                        if include_meta:
                            reply_channel_name = reply.get('channel_name')
                            if not reply_channel_name or reply_channel_name == '':
                                reply_channel_name = reply.get('channel_id', 'unknown')
                                if reply_channel_name == '':
                                    reply_channel_name = 'unknown'
                            reply_channel_info = reply_channel_name
                        
                        reply_data = {
                            'text': reply.get('text', ''),
                            'timestamp': reply.get('ts', ''),
                            'formatted_time': reply_formatted_time if include_meta else None,
                            'user': reply.get('user_name') or reply.get('user', 'Unknown User') if include_meta else None,
                            'channel': reply_channel_info
                        }
                        formatted_message['replies'].append(reply_data)
                
                formatted_messages.append(formatted_message)
            
            logger.info(f"Successfully formatted {len(formatted_messages)} messages to JSON")
            return formatted_messages
        
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
            channel_str = ""
            
            if include_meta:
                ts = float(message.get('ts', 0))
                dt = datetime.fromtimestamp(ts)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                user_name = message.get('user_name') or message.get('user', 'Unknown User')
                
                # Add channel information
                channel_name = message.get('channel_name')
                if not channel_name or channel_name == '':
                    channel_name = message.get('channel_id', 'unknown')
                    if channel_name == '':
                        channel_name = 'unknown'
                channel_str = f" [{channel_name}]"
            
            # Format main message
            if format_type == 'markdown':
                if include_meta:
                    formatted_output.append(f"## {time_str} - {user_name} in {channel_str}")
                formatted_output.append(f"{message.get('text', '')}\n")
            else:  # Plain text
                if include_meta:
                    formatted_output.append(f"{time_str} - {user_name} in {channel_str}")
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
                        
                        # Add channel information for reply
                        reply_channel_name = reply.get('channel_name')
                        if not reply_channel_name or reply_channel_name == '':
                            reply_channel_name = reply.get('channel_id', 'unknown')
                            if reply_channel_name == '':
                                reply_channel_name = 'unknown'
                        reply_channel_str = f" [{reply_channel_name}]"
                        
                        if format_type == 'markdown':
                            reply_prefix = f"- **{reply_time} - {reply_user} in {reply_channel_str}**: "
                        else:  # Plain text
                            reply_prefix = f"  {reply_time} - {reply_user} in {reply_channel_name}: "
                    else:
                        reply_prefix = "- " if format_type == 'markdown' else "  "
                    
                    # Format reply
                    formatted_output.append(f"{reply_prefix}{reply.get('text', '')}")
                
                formatted_output.append("")  # Add blank line after thread
        
        # Join all lines into a single string
        result = "\n".join(formatted_output)
        logger.info(f"Successfully formatted {len(sorted_messages)} messages to {format_type}")
        return result 