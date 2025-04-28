# This file is the implementation of Slack agent tool
from dataiku.llm.agent_tools import BaseAgentTool
from utils.logging import logger
from slackclient.slack_client import SlackClient
import asyncio

class SlackTool(BaseAgentTool):
    def set_config(self, config, plugin_config):
        self.config = config
        self.slack_auth = self.config["slack_api_connection"]
        #self.workspace_name = self.config["slack_api_connection"]["workspace_name"]
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
                        "enum": ["slack_list_channels", "slack_get_users", "slack_get_user_profile"],
                        "description": "The action to perform (slack_list_channels, slack_get_users, or slack_get_user_profile)"
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
