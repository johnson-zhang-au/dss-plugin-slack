# This file is the implementation of Slack agent tool
from dataiku.llm.agent_tools import BaseAgentTool
from utils.logging import logger
from slackclient.slack_client import SlackClient
import asyncio
import re
from datetime import datetime, timedelta

class SlackTool(BaseAgentTool):
    def set_config(self, config, plugin_config):
        self.config = config
        self.slack_auth = self.config["slack_auth_settings"]
        #self.workspace_name = self.config["slack_auth_settings"]["workspace_name"]
        self.workspace_name = "Dataiku"
        
        # Create a SlackClient instance with the configured token
        self.slack_client = None
        
        # Set up logging
        self.setup_logging()

    def setup_logging(self):
        """
        Sets up the logging level using the logger.
        """
        # Get the logging level from the configuration, default to INFO
        logging_level = self.config.get("logging_level", "INFO")

        try:
            # Set the logging level dynamically
            logger.set_level(logging_level)
            logger.info(f"Logging initialized with level: {logging_level}")
        except ValueError as e:
            # Handle invalid logging levels
            logger.error(f"Invalid logging level '{logging_level}': {str(e)}")
            raise

    def initialize_slack_client(self):
        """
        Initialize the SlackClient if it hasn't been initialized yet.
        """
        if self.slack_client is None:
            self.slack_client = SlackClient(self.slack_auth)
            logger.info("SlackClient initialized successfully")

    def get_descriptor(self, tool):
        logger.debug("Generating descriptor for the Slack tool.")
        return {
            "description": "Interacts with a Slack workspace to list channels, get users and user profiles",
            "inputSchema": {
                "$id": "https://dataiku.com/agents/tools/slack/input",
                "title": "Input for the Slack tool",
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "slack_list_channels", 
                            "slack_get_users", 
                            "slack_get_user_profile",
                            "slack_post_message",
                            "slack_reply_to_thread",
                            "slack_add_reaction",
                            "slack_get_channel_history",
                            "slack_get_thread_replies",
                            "slack_search_messages"
                        ],
                        "description": "The action to perform (slack_list_channels, slack_get_users, slack_get_user_profile, slack_post_message, slack_reply_to_thread, slack_add_reaction, slack_get_channel_history, slack_get_thread_replies, or slack_search_messages)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of items to return (default: 100, max: 200)",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 100
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Pagination cursor for next page"
                    },
                    "user_id": {
                        "type": "string",
                        "description": "User ID (required for slack_get_user_profile action)"
                    },
                    "include_private_channels": {
                        "type": "boolean",
                        "description": "Whether to include private channels (default: false)",
                        "default": False
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (required for channel-related actions)"
                    },
                    "text": {
                        "type": "string",
                        "description": "Message text (required for posting messages)"
                    },
                    "thread_ts": {
                        "type": "string",
                        "description": "Thread timestamp (required for thread-related actions)"
                    },
                    "timestamp": {
                        "type": "string",
                        "description": "Message timestamp (required for reaction actions)"
                    },
                    "reaction": {
                        "type": "string",
                        "description": "Emoji name without colons (required for reaction actions)"
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query (required for slack_search_messages action)"
                    },
                    "sort": {
                        "type": "string",
                        "enum": ["score", "timestamp"],
                        "description": "Sort order for search results (default: score)",
                        "default": "score"
                    },
                    "sort_dir": {
                        "type": "string",
                        "enum": ["asc", "desc"],
                        "description": "Sort direction for search results (default: desc)",
                        "default": "desc"
                    }
                },
                "required": ["action"]
            }
        }

    def invoke(self, input, trace):
        args = input["input"]
        action = args["action"]

        logger.info(f"Invoking action: {action}")
        logger.debug(f"Input arguments: {args}")

        # Initialize the Slack client
        self.initialize_slack_client()

        if action == "slack_list_channels":
            return self.slack_list_channels(args)
        elif action == "slack_get_users":
            return self.slack_get_users(args)
        elif action == "slack_get_user_profile":
            return self.slack_get_user_profile(args)
        elif action == "slack_post_message":
            return self.slack_post_message(args)
        elif action == "slack_reply_to_thread":
            return self.slack_reply_to_thread(args)
        elif action == "slack_add_reaction":
            return self.slack_add_reaction(args)
        elif action == "slack_get_channel_history":
            return self.slack_get_channel_history(args)
        elif action == "slack_get_thread_replies":
            return self.slack_get_thread_replies(args)
        elif action == "slack_search_messages":
            return self.slack_search_messages(args)
        else:
            logger.error(f"Invalid action: {action}")
            raise ValueError(f"Invalid action: {action}")

    def slack_list_channels(self, args):
        """
        List public or pre-defined channels in the workspace.
        
        Args:
            limit (int, optional): Maximum number of channels to return (default: 100, max: 200)
            cursor (str, optional): Pagination cursor for next page
            include_private_channels (bool, optional): Whether to include private channels (default: False)
            
        Returns:
            List of channels with their IDs and information
        """
        logger.debug("Starting 'slack_list_channels' action.")
        limit = min(args.get("limit", 100), 200)  # Default 100, max 200
        include_private_channels = args.get("include_private_channels", False)
        
        try:
            # Use the SlackClient's fetch_channels method
            all_channels, accessible_channels = asyncio.run(self.slack_client.fetch_channels(
                include_private_channels=include_private_channels
            ))
            
            logger.info(f"Found {len(all_channels)} total channels, {len(accessible_channels)} accessible channels")
            
            # Apply pagination - for simplicity we're handling limit after fetching all channels
            # In a production environment, you might want to implement proper pagination
            channel_list = []
            for channel in accessible_channels[:limit]:
                channel_info = {
                    "id": channel["id"],
                    "name": channel["name"],
                    "is_private": channel.get("is_private", False),
                    "num_members": channel.get("num_members", 0),
                    "topic": channel.get("topic", {}).get("value", ""),
                    "purpose": channel.get("purpose", {}).get("value", ""),
                    "created": channel.get("created", 0)
                }
                channel_list.append(channel_info)
            
            result = {
                "channels": channel_list,
                "count": len(channel_list),
                "total_count": len(accessible_channels)
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Listed {len(channel_list)} channels from Slack workspace {self.workspace_name}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error listing channels: {str(e)}")
            raise

    def slack_get_users(self, args):
        """
        Get list of workspace users with basic profile information.
        Using the Slack Web API directly as SlackClient doesn't have a direct method for this.
        
        Args:
            limit (int, optional): Maximum users to return (default: 100, max: 200)
            cursor (str, optional): Pagination cursor for next page
            
        Returns:
            List of users with their basic profiles
        """
        logger.debug("Starting 'slack_get_users' action.")
        limit = min(args.get("limit", 100), 200)  # Default 100, max 200
        cursor = args.get("cursor", None)
        
        try:
            # Use the Slack client's WebClient directly
            response = self.slack_client._slack_client.users_list(
                limit=limit,
                cursor=cursor
            )
            
            users = response["members"]
            logger.info(f"Found {len(users)} users in workspace {self.workspace_name}")
            
            # Format the response
            user_list = []
            for user in users:
                # Skip bots and deleted users if needed
                if user.get("is_bot", False) or user.get("deleted", False):
                    continue
                    
                user_info = {
                    "id": user["id"],
                    "name": user["name"],
                    "real_name": user.get("real_name", ""),
                    "display_name": user.get("profile", {}).get("display_name", ""),
                    "email": user.get("profile", {}).get("email", ""),
                    "is_admin": user.get("is_admin", False),
                    "is_owner": user.get("is_owner", False),
                    "is_restricted": user.get("is_restricted", False),
                    "updated": user.get("updated", 0)
                }
                user_list.append(user_info)
            
            result = {
                "users": user_list,
                "count": len(user_list),
                "next_cursor": response.get("response_metadata", {}).get("next_cursor", "")
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Listed {len(user_list)} users from Slack workspace {self.workspace_name}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error listing users: {str(e)}")
            raise

    def slack_get_user_profile(self, args):
        """
        Get detailed profile information for a specific user.
        
        Args:
            user_id (str): The user's ID
            
        Returns:
            Detailed user profile information
        """
        logger.debug("Starting 'slack_get_user_profile' action.")
        
        # Check required fields
        if "user_id" not in args:
            logger.error("Missing required field: user_id")
            raise ValueError("Missing required field: user_id")
        
        user_id = args["user_id"]
        
        try:
            # Use the SlackClient's _get_user_by_id method through asyncio
            user_id, display_name, email = asyncio.run(self.slack_client._get_user_by_id(user_id))
            
            if not user_id:
                logger.error(f"User with ID {args['user_id']} not found")
                raise ValueError(f"User with ID {args['user_id']} not found")
            
            # Get the full user profile for more details
            response = self.slack_client._slack_client.users_info(user=user_id)
            user = response["user"]
            
            logger.info(f"Retrieved profile for user {display_name} (ID: {user_id})")
            
            # Create a detailed user profile object
            profile = user.get("profile", {})
            user_info = {
                "id": user["id"],
                "name": user["name"],
                "real_name": user.get("real_name", ""),
                "display_name": profile.get("display_name", ""),
                "email": profile.get("email", ""),
                "phone": profile.get("phone", ""),
                "title": profile.get("title", ""),
                "status_text": profile.get("status_text", ""),
                "status_emoji": profile.get("status_emoji", ""),
                "image_original": profile.get("image_original", ""),
                "image_512": profile.get("image_512", ""),
                "team": user.get("team_id", ""),
                "time_zone": user.get("tz", ""),
                "time_zone_label": user.get("tz_label", ""),
                "time_zone_offset": user.get("tz_offset", 0),
                "is_admin": user.get("is_admin", False),
                "is_owner": user.get("is_owner", False),
                "is_primary_owner": user.get("is_primary_owner", False),
                "is_restricted": user.get("is_restricted", False),
                "is_ultra_restricted": user.get("is_ultra_restricted", False),
                "is_bot": user.get("is_bot", False),
                "updated": user.get("updated", 0),
                "is_app_user": user.get("is_app_user", False),
                "has_2fa": user.get("has_2fa", False)
            }
            
            return {
                "output": user_info,
                "sources": [{
                    "toolCallDescription": f"Retrieved detailed profile for user {display_name} (ID: {user_id})"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error retrieving user profile: {str(e)}")
            raise

    def slack_post_message(self, args):
        """
        Post a new message to a Slack channel.
        
        Args:
            channel_id (str): The ID of the channel to post to
            text (str): The message text to post
            
        Returns:
            Message posting confirmation and timestamp
        """
        logger.debug("Starting 'slack_post_message' action.")
        
        # Check required fields
        if "channel_id" not in args:
            logger.error("Missing required field: channel_id")
            raise ValueError("Missing required field: channel_id")
        if "text" not in args:
            logger.error("Missing required field: text")
            raise ValueError("Missing required field: text")
        
        channel_id = args["channel_id"]
        text = args["text"]
        
        try:
            # Post the message using the Slack client
            response = self.slack_client._slack_client.chat_postMessage(
                channel=channel_id,
                text=text
            )
            
            if not response["ok"]:
                logger.error(f"Failed to post message: {response.get('error')}")
                raise ValueError(f"Failed to post message: {response.get('error')}")
            
            logger.info(f"Successfully posted message to channel {channel_id}")
            
            result = {
                "ok": True,
                "channel": channel_id,
                "ts": response["ts"],
                "message": {
                    "text": text,
                    "ts": response["ts"]
                }
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Posted message to channel {channel_id}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error posting message: {str(e)}")
            raise

    def slack_reply_to_thread(self, args):
        """
        Reply to a specific message thread.
        
        Args:
            channel_id (str): The channel containing the thread
            thread_ts (str): Timestamp of the parent message
            text (str): The reply text
            
        Returns:
            Reply confirmation and timestamp
        """
        logger.debug("Starting 'slack_reply_to_thread' action.")
        
        # Check required fields
        if "channel_id" not in args:
            logger.error("Missing required field: channel_id")
            raise ValueError("Missing required field: channel_id")
        if "thread_ts" not in args:
            logger.error("Missing required field: thread_ts")
            raise ValueError("Missing required field: thread_ts")
        if "text" not in args:
            logger.error("Missing required field: text")
            raise ValueError("Missing required field: text")
        
        channel_id = args["channel_id"]
        thread_ts = args["thread_ts"]
        text = args["text"]
        
        try:
            # Post the reply using the Slack client
            response = self.slack_client._slack_client.chat_postMessage(
                channel=channel_id,
                text=text,
                thread_ts=thread_ts
            )
            
            if not response["ok"]:
                logger.error(f"Failed to post reply: {response.get('error')}")
                raise ValueError(f"Failed to post reply: {response.get('error')}")
            
            logger.info(f"Successfully posted reply to thread {thread_ts} in channel {channel_id}")
            
            result = {
                "ok": True,
                "channel": channel_id,
                "ts": response["ts"],
                "thread_ts": thread_ts,
                "message": {
                    "text": text,
                    "ts": response["ts"]
                }
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Posted reply to thread {thread_ts} in channel {channel_id}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error posting reply: {str(e)}")
            raise

    def slack_add_reaction(self, args):
        """
        Add an emoji reaction to a message.
        
        Args:
            channel_id (str): The channel containing the message
            timestamp (str): Message timestamp to react to
            reaction (str): Emoji name without colons
            
        Returns:
            Reaction confirmation
        """
        logger.debug("Starting 'slack_add_reaction' action.")
        
        # Check required fields
        if "channel_id" not in args:
            logger.error("Missing required field: channel_id")
            raise ValueError("Missing required field: channel_id")
        if "timestamp" not in args:
            logger.error("Missing required field: timestamp")
            raise ValueError("Missing required field: timestamp")
        if "reaction" not in args:
            logger.error("Missing required field: reaction")
            raise ValueError("Missing required field: reaction")
        
        channel_id = args["channel_id"]
        timestamp = args["timestamp"]
        reaction = args["reaction"]
        
        try:
            # Use the SlackClient's _send_reaction method
            asyncio.run(self.slack_client._send_reaction(
                channel_id=channel_id,
                reaction_name=reaction,
                event_timestamp=timestamp
            ))
            
            logger.info(f"Successfully added reaction '{reaction}' to message {timestamp} in channel {channel_id}")
            
            result = {
                "ok": True,
                "channel": channel_id,
                "timestamp": timestamp,
                "reaction": reaction
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Added reaction '{reaction}' to message {timestamp} in channel {channel_id}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error adding reaction: {str(e)}")
            raise

    def slack_get_channel_history(self, args):
        """
        Get recent messages from a channel.
        
        Args:
            channel_id (str): The channel ID
            limit (int, optional): Number of messages to retrieve (default: 10)
            time_range (str, optional): How far back to fetch messages (e.g., '1d', '40h', '1w'). Default: '1d', Max: '1M'
            
        Returns:
            List of messages with their content and metadata
        """
        logger.debug("Starting 'slack_get_channel_history' action.")
        
        # Check required fields
        if "channel_id" not in args:
            logger.error("Missing required field: channel_id")
            raise ValueError("Missing required field: channel_id")
        
        channel_id = args["channel_id"]
        limit = min(args.get("limit", 10), 100)  # Default 10, max 100
        
        # Parse time range
        time_range = args.get("time_range", "1d")
        try:
            # Parse the time range string (e.g., "1d", "40h", "1w")
            match = re.match(r"(\d+)([hdwM])", time_range)
            if not match:
                raise ValueError(f"Invalid time range format: {time_range}. Expected format: <number><unit> where unit is h (hours), d (days), w (weeks), or M (months)")
            
            number = int(match.group(1))
            unit = match.group(2)
            
            # Convert to seconds
            if unit == 'h':
                seconds = number * 3600
            elif unit == 'd':
                seconds = number * 86400
            elif unit == 'w':
                seconds = number * 604800
            elif unit == 'M':
                if number > 1:
                    raise ValueError("Maximum time range is 1 month")
                seconds = number * 2592000  # 30 days
            else:
                raise ValueError(f"Invalid time unit: {unit}")
            
            # Calculate start timestamp
            start_timestamp = (datetime.now() - timedelta(seconds=seconds)).timestamp()
            
        except ValueError as e:
            logger.error(f"Error parsing time range: {str(e)}")
            raise
        
        try:
            # Use the SlackClient's fetch_messages method
            messages = asyncio.run(self.slack_client.fetch_messages(
                channel_id=channel_id,
                start_timestamp=start_timestamp,
                channel_name=None,  # Not needed for this action
                resolve_users=True  # Get user info
            ))
            
            # Apply limit
            messages = messages[:limit]
            logger.info(f"Retrieved {len(messages)} messages from channel {channel_id}")
            
            # Format the messages
            formatted_messages = []
            for message in messages:
                formatted_message = {
                    "ts": message.get("ts"),
                    "text": message.get("text", ""),
                    "user": {
                        "id": message.get("user"),
                        "name": message.get("user_name", ""),
                        "email": message.get("user_email", "")
                    },
                    "thread_ts": message.get("thread_ts"),
                    "reply_count": message.get("reply_count", 0),
                    "reply_users_count": message.get("reply_users_count", 0),
                    "latest_reply": message.get("latest_reply"),
                    "subtype": message.get("subtype"),
                    "is_starred": message.get("is_starred", False),
                    "reactions": message.get("reactions", [])
                }
                formatted_messages.append(formatted_message)
            
            result = {
                "messages": formatted_messages,
                "count": len(formatted_messages),
                "time_range": time_range,
                "start_timestamp": start_timestamp
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Retrieved {len(formatted_messages)} messages from channel {channel_id} from the last {time_range}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error getting channel history: {str(e)}")
            raise

    def slack_get_thread_replies(self, args):
        """
        Get all replies in a message thread.
        
        Args:
            channel_id (str): The channel containing the thread
            thread_ts (str): Timestamp of the parent message
            
        Returns:
            List of replies with their content and metadata
        """
        logger.debug("Starting 'slack_get_thread_replies' action.")
        
        # Check required fields
        if "channel_id" not in args:
            logger.error("Missing required field: channel_id")
            raise ValueError("Missing required field: channel_id")
        if "thread_ts" not in args:
            logger.error("Missing required field: thread_ts")
            raise ValueError("Missing required field: thread_ts")
        
        channel_id = args["channel_id"]
        thread_ts = args["thread_ts"]
        
        try:
            # Use the new fetch_thread_replies method
            replies, error = asyncio.run(self.slack_client.fetch_thread_replies(
                channel_id=channel_id,
                thread_ts=thread_ts,
                resolve_users=True  # Get user info
            ))
            
            if error:
                logger.error(error)
                raise ValueError(error)
            
            logger.info(f"Retrieved {len(replies)} replies from thread {thread_ts} in channel {channel_id}")
            
            # Format the replies
            formatted_replies = []
            for reply in replies:
                formatted_reply = {
                    "ts": reply.get("ts"),
                    "text": reply.get("text", ""),
                    "user": {
                        "id": reply.get("user"),
                        "name": reply.get("user_name", ""),
                        "email": reply.get("user_email", "")
                    },
                    "parent_user_id": reply.get("parent_user_id"),
                    "subtype": reply.get("subtype"),
                    "is_starred": reply.get("is_starred", False),
                    "reactions": reply.get("reactions", [])
                }
                formatted_replies.append(formatted_reply)
            
            result = {
                "replies": formatted_replies,
                "count": len(formatted_replies),
                "thread_ts": thread_ts,
                "channel_id": channel_id
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Retrieved {len(formatted_replies)} replies from thread {thread_ts} in channel {channel_id}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error getting thread replies: {str(e)}")
            raise

    def slack_search_messages(self, args):
        """
        Search messages with keywords.
        
        Args:
            query (str): The search query
            limit (int, optional): Maximum number of results to return (default: 100, max: 200)
            sort (str, optional): Sort order (score or timestamp, default: score)
            sort_dir (str, optional): Sort direction (asc or desc, default: desc)
            context_messages (int, optional): Number of messages before and after to include (default: 5)
            
        Returns:
            List of matching messages with their content and metadata
        """
        logger.debug("Starting 'slack_search_messages' action.")
        
        # Check required fields
        if "query" not in args:
            logger.error("Missing required field: query")
            raise ValueError("Missing required field: query")
        
        query = args["query"]
        limit = min(args.get("limit", 100), 200)  # Default 100, max 200
        sort = args.get("sort", "score")
        sort_dir = args.get("sort_dir", "desc")
        context_messages = args.get("context_messages", 5)  # Default 5 messages of context
        
        try:
            # Use the new search_messages_with_context method
            messages, error = asyncio.run(self.slack_client.search_messages_with_context(
                query=query,
                context_messages=context_messages,
                limit=limit,
                sort=sort,
                sort_dir=sort_dir
            ))
            
            if error:
                logger.error(error)
                raise ValueError(error)
            
            logger.info(f"Found {len(messages)} messages matching query: {query}")
            
            result = {
                "messages": messages,
                "count": len(messages),
                "query": query,
                "context_messages": context_messages
            }
            
            return {
                "output": result,
                "sources": [{
                    "toolCallDescription": f"Found {len(messages)} messages matching query: {query}"
                }]
            }
        
        except Exception as e:
            logger.error(f"Error searching messages: {str(e)}")
            raise
