import asyncio
import re
from utils.logging import logger
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.signature import SignatureVerifier

from cachetools import TTLCache
from datetime import datetime
import logging
import math


class DKUSlackClient():
    """
    A client for interacting with Slack, providing functionality for handling messages,
    querying Dataiku Answers, and sending responses or reactions.
    """
    # Constants
    CHANNEL_FETCH_LIMIT = 200  # Maximum number of channels to fetch per API call
    MESSAGE_FETCH_LIMIT = 200  # Maximum number of messages to fetch per API call
    USER_FETCH_LIMIT = 100  # Maximum number of users to fetch per API call
    CACHE_TTL = 86400  # 24 hours in seconds
    CACHE_MAXSIZE = math.inf  # Maximum number of items in cache
    
    # Slack API rate limit tiers
    # https://api.slack.com/apis/rate-limits
    TIER_1_LIMIT = 1    # Methods with the strictest rate limit (Access tier 1 methods infrequently)
    TIER_2_LIMIT = 4    # Methods with moderate rate limit
    TIER_3_LIMIT = 8   # Methods for paginating collections of conversations or users
    TIER_4_LIMIT = 20   # Methods with the loosest rate limit (Enjoy a large request quota)

    def __init__(self, slack_token: str):
        """Initialize the Slack client with a token.
        
        Args:
            slack_token: The Slack API token to use for authentication.
        """
        if not slack_token:
            logger.error("Required Slack token is missing!")
            raise ValueError("Required Slack token is missing.")
            
        self._slack_token = slack_token
        self._is_bot_token = False
        self._bot_user_id = None
        self._bot_user_name = None
        self.signature_verifier = None
        self._slack_async_web_client = None
        self._bot_prefix = None
        self._slack_user_cache = TTLCache(maxsize=self.CACHE_MAXSIZE, ttl=self.CACHE_TTL)
        self._slack_channel_name_cache = TTLCache(maxsize=self.CACHE_MAXSIZE, ttl=self.CACHE_TTL)
        self._slack_channel_members_cache = TTLCache(maxsize=self.CACHE_MAXSIZE, ttl=self.CACHE_TTL)
        
        # Create semaphores for different API tiers
        self._tier_1_semaphore = asyncio.Semaphore(self.TIER_1_LIMIT)  # Tier 1: Strictest limit
        self._tier_2_semaphore = asyncio.Semaphore(self.TIER_2_LIMIT)  # Tier 2: conversations_list
        self._tier_3_semaphore = asyncio.Semaphore(self.TIER_3_LIMIT)  # Tier 3: conversations_history, users_lookupByEmail, conversations_replies
        self._tier_4_semaphore = asyncio.Semaphore(self.TIER_4_LIMIT)  # Tier 4: users_info, conversations_members
        
        self._initialize_and_test()

    def _initialize_and_test(self):
        """Initialize Slack client and test the token."""
        try:
            # Initialize AsyncWebClient only
            self._slack_async_web_client = AsyncWebClient(token=self._slack_token)
            logger.debug("Slack AsyncWebClient initialized successfully.")
            
            # Test token using async client directly
            response = asyncio.run(self._slack_async_web_client.auth_test())
            if not response["ok"]:
                logger.error("Token test failed: %s", response.get("error"))
                raise ValueError(f"Token test failed: {response.get('error')}")
            
            # Log authentication information
            logger.info("Token test successful")
            logger.info("Workspace: %s (ID: %s)", response.get("team"), response.get("team_id"))
            logger.info("User: %s (ID: %s)", response.get("user"), response.get("user_id"))
            logger.info("URL: %s", response.get("url"))
            
            # Determine if this is a bot token
            self._is_bot_token = "bot_id" in response
            if self._is_bot_token:
                logger.info("This is a bot token (Bot ID: %s)", response.get("bot_id"))
                self._bot_user_id = response.get("bot_id")
                self._bot_user_name = response.get("bot_user_name")
            else:
                logger.info("This is a user token")
                
        except Exception as e:
            logger.error("Error initializing and testing token: %s", str(e))
            raise ValueError(f"Error initializing and testing token: {str(e)}")

    @property
    def slack_client(self):
        """Get the Slack WebClient instance (for backward compatibility)."""
        logger.warn("Deprecated: Use slack_async_web_client instead of slack_client")
        return self._slack_async_web_client

    @property
    def slack_async_client(self):
        """Get the Slack AsyncWebClient instance (for backward compatibility)."""
        logger.warn("Deprecated: Use slack_async_web_client instead of slack_async_client")
        return self._slack_async_web_client
        
    @property
    def slack_async_web_client(self):
        """Get the Slack AsyncWebClient instance."""
        return self._slack_async_web_client

    @property
    def is_bot_token(self):
        """Check if the token is a bot token."""
        return self._is_bot_token

    @property
    def bot_user_id(self):
        """Get the bot user ID."""
        return self._bot_user_id

    @property
    def bot_user_name(self):
        """Get the bot user name."""
        return self._bot_user_name

    def _cache_user_info(self, user_id, user_info):
        """
        Process and cache user information.
        
        :param user_id: The user's ID
        :param user_info: The user information from Slack API
        :return: Tuple containing (user_id, display_name, email)
        """
        username = user_info.get("real_name", "Unknown User")
        display_name = user_info.get("profile", {}).get("display_name", username)
        email = user_info.get("profile", {}).get("email", "No email found")

        # Cache the complete user info
        self._slack_user_cache[user_id] = {
            "name": display_name or username,
            "email": email,
            "timestamp": datetime.now()
        }
        return user_id, display_name or username, email

    async def _handle_rate_limit(self, func, *args, error_handler=None, log_prefix="", **kwargs):
        """
        Handles rate limiting for Slack API calls with customizable error handling.
        
        :param func: The async function to call
        :param args: Positional arguments for the function
        :param error_handler: Function to handle errors (returns value to return on error)
        :param log_prefix: Prefix for log messages
        :param kwargs: Keyword arguments for the function
        :return: The function's response or error_handler's return value
        """
        while True:  # Loop to handle rate limits
            try:
                response = await func(*args, **kwargs)
                if response["ok"]:
                    return response
                else:
                    error_msg = f"{log_prefix}API call failed: {response.get('error')}"
                    logger.error(error_msg)
                    if error_handler:
                        return error_handler(response)
                    raise ValueError(error_msg)
            except SlackApiError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 30))
                    logger.warn(f"{log_prefix}Rate limited. Retrying in {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    continue
                else:
                    error_msg = f"{log_prefix}API error: {e.response['error']}"
                    logger.error(error_msg, exc_info=True)
                    if error_handler:
                        return error_handler(e)
                    raise
            except Exception as e:
                error_msg = f"{log_prefix}Unexpected error: {str(e)}"
                logger.error(error_msg, exc_info=True)
                if error_handler:
                    return error_handler(e)
                raise

    async def _get_user_by_id(self, user_id):
        """
        Get user information from Slack by user ID.
        Caches the results for future lookups.
        
        :param user_id: Slack user ID to fetch information for
        :return: Tuple containing (user_id, display_name, email) or (None, None, None) if not found
        """
        logger.info(f"Getting user by ID {user_id}")
        # Check cache first
        cached_user = self._slack_user_cache.get(user_id)
        if cached_user:
            logger.debug(f"Using cached user info for {cached_user['name']} ({cached_user['email']})")
            return user_id, cached_user["name"], cached_user["email"]
        
        # If not in cache, get user info
        logger.debug(f"Cached user info was not found for user {user_id}, fetching from Slack API")
        async with self._tier_4_semaphore:  # users_info is Tier 4 (100+ per minute)
            response = await self._handle_rate_limit(
                self._slack_async_web_client.users_info,
                user=user_id,
                error_handler=lambda e: None,
                log_prefix=f"User {user_id}: "
            )
            if response:
                return self._cache_user_info(user_id, response["user"])
            return None, None, None

    async def _get_user_by_email(self, email):
        """
        Get user information from Slack by email.
        Caches the results for future lookups.
        
        :param email: Email address to look up
        :return: Tuple containing (user_id, display_name, email) or (None, None, None) if not found
        """
        logger.info(f"Getting user by email {email}")
        # Check if we have this email in our cache
        for cached_user_id, cached_user in self._slack_user_cache.items():
            if cached_user.get("email") == email:
                logger.debug(f"Found cached user info for email {email}")
                return cached_user_id, cached_user["name"], cached_user["email"]

        # If not in cache, try to find the user
        logger.debug(f"Cached user info was not found for email {email}, fetching from Slack API")
        async with self._tier_3_semaphore:  # users_lookupByEmail is Tier 3 (50+ per minute)
            response = await self._handle_rate_limit(
                self._slack_async_web_client.users_lookupByEmail,
                email=email,
                error_handler=lambda e: None,
                log_prefix=f"Looking up user by email {email}: "
            )
            if response:
                user_info = response["user"]
                user_id = user_info["id"]
                return self._cache_user_info(user_id, user_info)
            return None, None, None

    async def _get_channel_id_by_name(self, channel_name):
        """
        Get a channel's ID from its name.
        
        :param channel_name: The channel name to look up (with or without #)
        :return: The channel's ID or None if not found
        """
        # Remove # if present
        original_name = channel_name
        channel_name = channel_name.lstrip('#')
        logger.debug(f"Looking up channel ID for '{original_name}' (normalized to '{channel_name}')")
        
        # Check cache first
        cached_channel = self._slack_channel_name_cache.get(channel_name)
        if cached_channel:
            logger.info(f"Cache hit for channel '{channel_name}' -> ID: {cached_channel['id']}")
            return cached_channel["id"]
        
        logger.debug(f"Cache miss for channel '{channel_name}', fetching from API")
        # If not in cache, fetch all channels (which will populate the cache)
        channels, _ = await self.fetch_channels(cursor_limit=self.CHANNEL_FETCH_LIMIT)
        for channel in channels:
            if channel["name"] == channel_name:
                logger.info(f"Found channel '{channel_name}' with ID: {channel['id']}")
                return channel["id"]
            
        logger.warn(f"Could not find channel with name '{channel_name}'")
        return None

    async def _send_reaction(self, channel_id, reaction_name, event_timestamp):
        """
        Sends a reaction emoji to a Slack message.

        :param channel_id: ID of the channel where the message was sent.
        :param reaction_name: Name of the reaction emoji.
        :param event_timestamp: Timestamp of the Slack message.
        """
        async with self._tier_3_semaphore:  # reactions_add is Tier 3 (50+ per minute)
            response = await self._handle_rate_limit(
                self._slack_async_web_client.reactions_add,
                channel=channel_id,
                name=reaction_name,
                timestamp=event_timestamp,
                log_prefix=f"Reaction {reaction_name} to message {event_timestamp}: "
            )
            logger.info(f"Successfully sent reaction '{reaction_name}' to message {event_timestamp} in channel {channel_id}")
            return response

    async def fetch_channels(self, include_private_channels=False, total_limit=None, cursor_limit=None):
        """Fetch all channels the Slack app or user has access to.
        
        :param include_private_channels: Whether to include private channels (default: False)
        :param total_limit: Maximum total number of channels to fetch (default: None, fetch all)
        :param cursor_limit: Maximum number of channels to fetch per API call (default: CHANNEL_FETCH_LIMIT)
        :return: Tuple of (all_channels, member_channels)
        """
        logger.info("Fetching all channels from Slack API")
        # Determine which types of channels to fetch
        types = "public_channel"
        if include_private_channels:
            types += ",private_channel"
            logger.info("Including private channels in the fetch")
        
        if cursor_limit is None:
            cursor_limit = self.CHANNEL_FETCH_LIMIT
            
        logger.debug("Using cursor limit: %d, total limit: %s", cursor_limit, total_limit if total_limit is not None else "unlimited")
        all_channels = []
        member_channels = []
        next_cursor = None
        
        # Calculate how many more channels we need to fetch
        remaining_to_fetch = total_limit if total_limit is not None else float('inf')
        
        while remaining_to_fetch > 0:
            # Adjust cursor limit to fetch only what we need
            current_cursor_limit = min(cursor_limit, remaining_to_fetch)
            logger.debug("Fetching up to %d more channels", current_cursor_limit)
            
            async with self._tier_2_semaphore:  # conversations_list is Tier 2 (20+ per minute)
                response = await self._handle_rate_limit(
                    self._slack_async_web_client.conversations_list,
                    types=types,
                    limit=current_cursor_limit,
                    cursor=next_cursor,
                    log_prefix="Fetching channels: "
                )
                
                if response["ok"]:
                    channels = response.get("channels", [])
                    # Filter channels where Slack app or user is a member
                    member_channels_batch = [channel for channel in channels if channel.get("is_member")]
                    non_member_channels = [channel for channel in channels if not channel.get("is_member")]
                    
                    if non_member_channels:
                        logger.warn(f"Found {len(non_member_channels)} channels where Slack app or user is not a member.")
                        logger.debug(f"Non-member channels: {[c['name'] for c in non_member_channels]}")
                    
                    all_channels.extend(channels)  # Add all channels to the list
                    member_channels.extend(member_channels_batch)  # Add only member channels                    
                    # Cache channel names and IDs only for member channels
                    for channel in channels:
                        self._slack_channel_name_cache[channel["name"]] = {
                            "id": channel["id"],
                            "timestamp": datetime.now()
                        }
                    
                    # Update remaining count
                    if total_limit is not None:
                        remaining_to_fetch = total_limit - len(all_channels)
                        logger.debug("Remaining channels to fetch: %d", remaining_to_fetch)
                    
                    logger.info(f"Fetched {len(all_channels)} channels in total, {len(member_channels)} channels where Slack app or user is member")
                    # Check if there are more channels to fetch
                    next_cursor = response.get("response_metadata", {}).get("next_cursor")
                    if not next_cursor:
                        break
        
        logger.info(f"Successfully fetched total of {len(all_channels)} channels and {len(member_channels)} channels where Slack app or user is member")
        logger.debug(f"Cached {len(all_channels)} channel name/ID mappings")
        return all_channels, member_channels
        
    async def fetch_messages(self, channel_id, start_timestamp, channel_name=None, resolve_users=True, total_limit=None, cursor_limit=None):
        """Fetch messages from a specific channel, including thread replies.
        
        :param channel_id: ID of the channel to fetch messages from
        :param start_timestamp: Timestamp to start fetching messages from
        :param channel_name: Name of the channel (optional)
        :param resolve_users: Whether to resolve user IDs to usernames and emails (default: True)
        :param total_limit: Maximum total number of messages to fetch (default: None, fetch all)
        :param cursor_limit: Maximum number of messages to fetch per API call (default: MESSAGE_FETCH_LIMIT)
        :return: List of messages with added metadata
        """
        
        logger.info(f"Fetching messages from channel id: {channel_id}, name: {channel_name}...")
        
        if cursor_limit is None:
            cursor_limit = self.MESSAGE_FETCH_LIMIT
            
        logger.debug("Using cursor limit: %d, total limit: %s", cursor_limit, total_limit if total_limit is not None else "unlimited")
        messages = []
        next_cursor = None
        
        # Calculate how many more messages we need to fetch
        remaining_to_fetch = total_limit if total_limit is not None else float('inf')
        
        while remaining_to_fetch > 0:
            # Adjust cursor limit to fetch only what we need
            current_cursor_limit = min(cursor_limit, remaining_to_fetch)
            logger.debug("Fetching up to %d more messages", current_cursor_limit)
            
            async with self._tier_3_semaphore:  # conversations_history is Tier 3 (50+ per minute)
                response = await self._handle_rate_limit(
                    self._slack_async_web_client.conversations_history,
                    channel=channel_id,
                    oldest=start_timestamp,
                    limit=current_cursor_limit,
                    cursor=next_cursor,
                    error_handler=lambda e: {"ok": True, "messages": []},  # Return empty list on error
                    log_prefix=f"Fetch messages from channel {channel_id} history: "
                )
                
                for message in response.get("messages", []):
                    # Add formatted date and time
                    date, time = self._format_timestamp(message.get("ts"))
                    message["date"] = date
                    message["time"] = time
                    
                    # Inject channel_id and channel_name into each message
                    message["channel_id"] = channel_id
                    message["channel_name"] = channel_name
                    messages.append(message)

                    # If this message has a thread, fetch the replies
                    if message.get("thread_ts"):
                        logger.info(f"Fetching thread replies for message {message.get('ts')} from channel id: {channel_id}, name: {channel_name}...")
                        async with self._tier_3_semaphore:  # conversations_replies is Tier 3 (50+ per minute)
                            thread_response = await self._handle_rate_limit(
                                self._slack_async_web_client.conversations_replies,
                                channel=channel_id,
                                ts=message["thread_ts"],
                                error_handler=lambda e: {"ok": True, "messages": []},  # Return empty list on error
                                log_prefix=f"Fetch thread {message['thread_ts']} replies: "
                            )
                            if thread_response["ok"]:
                                # Skip the first message as it's the parent message we already have
                                for reply in thread_response["messages"][1:]:
                                    # Add formatted date and time
                                    date, time = self._format_timestamp(reply.get("ts"))
                                    reply["date"] = date
                                    reply["time"] = time
                                    
                                    # Inject channel_id and channel_name into each reply
                                    reply["channel_id"] = channel_id
                                    reply["channel_name"] = channel_name
                                    messages.append(reply)

                # Update remaining count
                if total_limit is not None:
                    remaining_to_fetch = total_limit - len(messages)
                    logger.debug("Remaining messages to fetch: %d", remaining_to_fetch)
                
                next_cursor = response.get("response_metadata", {}).get("next_cursor")
                if not next_cursor:
                    break
                logger.info(f"In total {len(messages)} messages have been fetched from channel id: {channel_id}, name: {channel_name}")
        
        # Add user information to all messages if requested
        if resolve_users:
            logger.info("Resolving user IDs to usernames and emails...")
            messages = await self._add_user_info_to_messages(messages)
        else:
            logger.info("Skipping user ID resolution as requested")
        
        return messages

    async def _add_user_info_to_messages(self, messages):
        """
        Add user information (user_name, user_email) to messages based on user IDs.
        For replies, also process reply_users field and add corresponding user info.
        Also resolves user mentions in message text (e.g., <@UF9QWN8RJ>).
        
        :param messages: List of message objects from Slack API
        :return: List of messages with added user information
        """
        logger.info(f"Adding user information to {len(messages)} messages")
        
        # Create a set of all user IDs that need to be resolved
        user_ids_to_resolve = set()
        
        # Regular expression to find user mentions in text
        user_mention_pattern = r'<@([A-Z0-9]+)>'
        
        # Collect all user IDs from messages, replies, and text mentions
        for message in messages:
            # Add sender user ID
            if "user" in message:
                user_ids_to_resolve.add(message["user"])
            
            # Add reply user IDs if present
            if "reply_users" in message:
                user_ids_to_resolve.update(message["reply_users"])
            
            # Find user mentions in message text
            if "text" in message:
                mentions = re.findall(user_mention_pattern, message["text"])
                user_ids_to_resolve.update(mentions)
        
        # Create a mapping of user_id to user info
        user_info_map = {}
        
        # Create tasks for all user IDs to get user information in parallel
        tasks = [self._get_user_by_id(uid) for uid in user_ids_to_resolve]
        results = await asyncio.gather(*tasks)
        
        # Create a mapping from the results
        for i, uid in enumerate(user_ids_to_resolve):
            user_id, user_name, user_email = results[i]
            if user_id:  # Skip None results
                user_info_map[uid] = {
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_email": user_email
                }
        
        # Add user information to each message
        for message in messages:
            # Add sender information
            if "user" in message and message["user"] in user_info_map:
                user_info = user_info_map[message["user"]]
                message["user_name"] = user_info["user_name"]
                message["user_email"] = user_info["user_email"]
            
            # Add parent user information if present
            if "parent_user_id" in message and message["parent_user_id"] in user_info_map:
                parent_user_info = user_info_map[message["parent_user_id"]]
                message["parent_user_name"] = parent_user_info["user_name"]
                message["parent_user_email"] = parent_user_info["user_email"]
            
            # Add reply users information
            if "reply_users" in message:
                message["reply_users_info"] = []
                for reply_user_id in message["reply_users"]:
                    if reply_user_id in user_info_map:
                        message["reply_users_info"].append(user_info_map[reply_user_id])
            
            # Process user mentions in message text
            if "text" in message:
                # Find all user mentions
                mentions = re.findall(user_mention_pattern, message["text"])
                if mentions:
                    # Create a list to store mention information
                    message["mentions"] = []
                    for user_id in mentions:
                        if user_id in user_info_map:
                            message["mentions"].append(user_info_map[user_id])
                    
                    # Replace mentions in text with user names
                    for user_id in mentions:
                        if user_id in user_info_map:
                            user_name = user_info_map[user_id]["user_name"]
                            message["text"] = message["text"].replace(
                                f"<@{user_id}>", 
                                f"@{user_name}"
                            )
        
        logger.info(f"Successfully added user information to {len(messages)} messages")
        return messages

    async def _get_channel_members(self, channel_id):
        """
        Get channel members with caching.
        
        :param channel_id: The channel ID to get members for
        :return: List of member IDs or empty list if error
        """
        logger.info(f"Getting members for channel {channel_id}")
        logger.debug(f"Channel members cache: {self._slack_channel_members_cache}")
        cached_members = self._slack_channel_members_cache.get(channel_id)
        if cached_members:
            logger.info(f"Cache hit for channel {channel_id} members ({len(cached_members['members'])} members)")
            return cached_members["members"]

        logger.debug(f"Cache miss for channel {channel_id} members, fetching from API")
        all_members = []
        cursor = None
        
        while True:
            async with self._tier_4_semaphore:  # conversations_members is Tier 4 (100+ per minute)
                response = await self._handle_rate_limit(
                    self._slack_async_web_client.conversations_members,
                    channel=channel_id,
                    cursor=cursor,
                    limit=100,  # Use default limit
                    error_handler=lambda e: None,
                    log_prefix=f"Channel {channel_id} members: "
                )
                if response:
                    members = response["members"]
                    all_members.extend(members)
                    
                    # Check if there are more pages
                    cursor = response.get("response_metadata", {}).get("next_cursor")
                    if not cursor:
                        break
                else:
                    break
        
        if all_members:
            logger.info(f"Successfully fetched {len(all_members)} members for channel {channel_id}")
            logger.debug(f"Members: {all_members}")
            self._slack_channel_members_cache[channel_id] = {
                "members": all_members,
                "timestamp": datetime.now()
            }
            logger.debug(f"Cached members for channel {channel_id}")
            return all_members
        return []

    async def fetch_messages_from_channels(self, start_timestamp, user_emails=None, channel_names=None, channel_ids=None, include_private_channels=False, resolve_users=True, total_limit=None):
        """Fetch messages from specified channels or all channels.
        
        :param start_timestamp: Timestamp to start fetching messages from
        :param user_emails: List of user emails to filter messages by (optional)
        :param channel_names: List of channel names to fetch messages from (optional)
        :param channel_ids: List of channel IDs to fetch messages from (optional)
        :param include_private_channels: Whether to include private channels (default: False)
        :param resolve_users: Whether to resolve user IDs to usernames and emails (default: True)
        :param total_limit: Maximum total number of messages to fetch (default: None, fetch all)
        :return: List of messages from all channels
        """
        
        user_ids = set()
        channels = []

        # Convert channel names to IDs
        if channel_ids:
            channel_ids = set(channel_ids)
            logger.info(f"Starting message fetch for {len(channel_ids)} channels filtering on channel ids")
           
            for cid in channel_ids:
                response = await self._slack_async_web_client.conversations_info(channel=cid)
                if response['ok']:
                    # Only check for membership if using a bot token
                    if self._is_bot_token and not response['channel'].get('is_member', False):
                        logger.warn(f"Bot is not a member of channel ID {cid}, name: {response['channel'].get('name', 'Unknown')}. Skipping.")
                        continue
                    channels.append(response['channel'])
                else:
                    logger.warn(f"Failed to get info for channel ID {cid}: {response.get('error', 'Unknown error')}")
        elif channel_names:
            logger.info(f"Starting message fetch for {len(channel_names)} channels filtering on channel names")
            _, member_channels = await self.fetch_channels(
                include_private_channels=include_private_channels,
                cursor_limit=self.CHANNEL_FETCH_LIMIT
            )
                    
             # Build a dict for quick lookup
            channel_name_to_obj = {channel['name']: channel for channel in member_channels}
            
            for channel_name in channel_names:
                channel = channel_name_to_obj.get(channel_name)
                if channel:
                    channels.append(channel)
                    logger.debug(f"Found channel for channel name: {channel_name}")
                else:
                    logger.warn(f"Could not find channel for channel name: {channel_name}")
                
                logger.debug(f"Filtered to {len(channels)} channels by given channel names")
            
        else:
            logger.info(f"Starting message fetch for all channels that the Slack app or user has access to ")
            _, channels = await self.fetch_channels(
                include_private_channels=include_private_channels,
                cursor_limit=self.CHANNEL_FETCH_LIMIT
            )
            logger.debug(f"Filtered to {len(channels)} channels that the Slack app or user has access to")

        # Convert user emails to IDs
        if user_emails:
            logger.debug(f"Converting {len(user_emails)} user emails to IDs to filter messages from specific users")
            for email in user_emails:
                user_id, _, _ = await self._get_user_by_email(email)
                if user_id:
                    user_ids.add(user_id)
                    logger.debug(f"Found user ID {user_id} for {email}")
                else:
                    logger.warn(f"Could not find user ID for {email}")

        # Filter channels based on user membership if user_ids are provided
        if user_ids:
            logger.info(f"Filtering channels for {len(user_ids)} users")
            # Get members for all channels in parallel with throttling
            member_tasks = [self._get_channel_members(channel["id"]) for channel in channels]
            channel_members = await asyncio.gather(*member_tasks)
            
            # Create a mapping of channel IDs to their members for better tracking
            channel_members_map = {
                channel["id"]: set(members)
                for channel, members in zip(channels, channel_members)
            }
            
            # Only log detailed channel member information if debug level is enabled
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Channel members mapping:")
                for channel_id, members in channel_members_map.items():
                    channel_name = next((c["name"] for c in channels if c["id"] == channel_id), "unknown")
                    logger.debug(f"Channel {channel_name} ({channel_id}) has {len(members)} members")
            
            # Filter channels that have any of the specified users as members
            filtered_channels = []
            for channel in channels:
                members = channel_members_map[channel["id"]]  # No need for .get() since we know the key exists
                if user_ids & members:
                    filtered_channels.append(channel)
                    logger.debug(f"Channel {channel['name']} ({channel['id']}) has matching users")
                else:
                    logger.debug(f"Channel {channel['name']} ({channel['id']}) has no matching users")
            
            channels = filtered_channels
            logger.info(f"Found {len(channels)} channels with matching users")
            logger.debug(f"Channels: {channels}")

        # Calculate per-channel limit if total limit is provided
        per_channel_limit = None
        if total_limit is not None and channels:
            per_channel_limit = max(1, total_limit // len(channels))
            logger.debug("Using per-channel limit: %d", per_channel_limit)

        # Fetch messages from all channels in parallel with throttling
        logger.info(f"Fetching messages from {len(channels)} channels")
        tasks = [
            self.fetch_messages(
                channel_id=channel["id"],
                start_timestamp=start_timestamp,
                channel_name=channel["name"],
                resolve_users=resolve_users,
                total_limit=per_channel_limit,
                cursor_limit=self.MESSAGE_FETCH_LIMIT
            )
            for channel in channels
        ]
        results = await asyncio.gather(*tasks)
        all_messages = [message for channel_messages in results for message in channel_messages]
        
        # Apply total limit if needed
        if total_limit is not None and len(all_messages) > total_limit:
            logger.info(f"Limiting total messages to {total_limit}")
            all_messages = all_messages[:total_limit]
            
        logger.info(f"Successfully fetched {len(all_messages)} messages from {len(channels)} channels")
        return all_messages

    async def fetch_thread_replies(self, channel_id, thread_ts, resolve_users=True):
        """
        Fetch replies for a specific thread using conversations.replies.
        
        :param channel_id: ID of the channel containing the thread
        :param thread_ts: Timestamp of the parent message
        :param resolve_users: Whether to resolve user IDs to usernames and emails (default: True)
        :return: Tuple of (replies, error) where error is None if successful, or error message if failed
        """
        logger.info(f"Fetching replies for thread {thread_ts} in channel {channel_id}")
        
        async with self._tier_3_semaphore:  # conversations_replies is Tier 3 (50+ per minute)
            response = await self._handle_rate_limit(
                self._slack_async_web_client.conversations_replies,
                channel=channel_id,
                ts=thread_ts,
                error_handler=lambda e: {"ok": False, "error": str(e), "messages": []},  # Return error info
                log_prefix=f"Fetch thread {thread_ts} replies: "
            )
            
            if not response["ok"]:
                error_msg = f"Failed to fetch thread replies: {response.get('error')}"
                logger.error(error_msg)
                return [], error_msg
            
            # Skip the first message as it's the parent message
            replies = response["messages"][1:] if len(response["messages"]) > 1 else []
            
            # Add user information to all replies if requested
            if resolve_users:
                logger.info("Resolving user IDs to usernames and emails...")
                replies = await self._add_user_info_to_messages(replies)
            else:
                logger.info("Skipping user ID resolution as requested")
            
            logger.info(f"Successfully fetched {len(replies)} replies for thread {thread_ts}")
            return replies, None 

    def _format_timestamp(self, timestamp):
        """
        Convert a Slack timestamp to a formatted date and time.
        
        :param timestamp: Slack timestamp (e.g., "1234567890.123456")
        :return: Tuple of (formatted_date, formatted_time) or (None, None) if invalid
        """
        try:
            if not timestamp:
                return None, None
            ts = float(timestamp)
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        except (ValueError, TypeError):
            return None, None

    async def search_messages_with_context(self, query, context_messages=5, limit=100, sort="score", sort_dir="desc"):
        """
        Search messages with context and thread information.
        
        :param query: The search query
        :param context_messages: Number of messages before and after to include (default: 5)
        :param limit: Maximum number of results to return (default: 100, max: 200)
        :param sort: Sort order (score or timestamp, default: score)
        :param sort_dir: Sort direction (asc or desc, default: desc)
        :return: Tuple of (messages, error) where error is None if successful, or error message if failed
        """
        logger.info(f"Searching messages with query: {query}")
        
        try:
            # Search messages using the Slack client
            response = await self._handle_rate_limit(
                self._slack_async_web_client.search_messages,
                query=query,
                count=limit,
                sort=sort,
                sort_dir=sort_dir,
                error_handler=lambda e: {"ok": False, "error": str(e), "messages": {"matches": []}},
                log_prefix="Search messages: "
            )
            
            if not response["ok"]:
                error_msg = f"Failed to search messages: {response.get('error')}"
                logger.error(error_msg)
                return [], error_msg
            
            matches = response.get("messages", {}).get("matches", [])
            logger.info(f"Found {len(matches)} messages matching query: {query}")
            
            # Process each match to get context and thread information
            processed_messages = []
            for match in matches:
                # Get user info for the message
                user_id = match.get("user")
                user_info = {}
                if user_id:
                    try:
                        user_id, display_name, email = await self._get_user_by_id(user_id)
                        if user_id:
                            user_info = {
                                "id": user_id,
                                "name": display_name,
                                "email": email
                            }
                    except Exception as e:
                        logger.warning(f"Could not get user info for {user_id}: {str(e)}")
                
                # Check if this is a thread message
                thread_ts = None
                if "thread_ts" in match:
                    thread_ts = match["thread_ts"]
                elif "permalink" in match and "?thread_ts=" in match["permalink"]:
                    # Extract thread_ts from permalink
                    thread_ts = match["permalink"].split("?thread_ts=")[1]
                
                # Get thread replies if this is a thread message
                thread_replies = []
                if thread_ts:
                    try:
                        replies, error = await self.fetch_thread_replies(
                            channel_id=match["channel"]["id"],
                            thread_ts=thread_ts,
                            resolve_users=True
                        )
                        if not error:
                            # Add formatted dates to replies
                            for reply in replies:
                                date, time = self._format_timestamp(reply.get("ts"))
                                reply["date"] = date
                                reply["time"] = time
                            thread_replies = replies
                    except Exception as e:
                        logger.warning(f"Could not get thread replies for {thread_ts}: {str(e)}")
                
                # Get context messages (messages before and after)
                context_before = []
                context_after = []
                if context_messages > 0:
                    try:
                        # Get messages before
                        before_response = await self._handle_rate_limit(
                            self._slack_async_web_client.conversations_history,
                            channel=match["channel"]["id"],
                            latest=match["ts"],
                            limit=context_messages + 1,  # +1 to exclude the match itself
                            error_handler=lambda e: {"ok": False, "error": str(e), "messages": []},
                            log_prefix=f"Get context before {match['ts']}: "
                        )
                        if before_response["ok"]:
                            # Skip the match itself and take the rest
                            context_before = before_response["messages"][1:][:context_messages]
                            # Add formatted dates to context messages
                            for msg in context_before:
                                date, time = self._format_timestamp(msg.get("ts"))
                                msg["date"] = date
                                msg["time"] = time
                        
                        # Get messages after
                        after_response = await self._handle_rate_limit(
                            self._slack_async_web_client.conversations_history,
                            channel=match["channel"]["id"],
                            oldest=match["ts"],
                            limit=context_messages + 1,  # +1 to exclude the match itself
                            error_handler=lambda e: {"ok": False, "error": str(e), "messages": []},
                            log_prefix=f"Get context after {match['ts']}: "
                        )
                        if after_response["ok"]:
                            # Skip the match itself and take the rest
                            context_after = after_response["messages"][1:][:context_messages]
                            # Add formatted dates to context messages
                            for msg in context_after:
                                date, time = self._format_timestamp(msg.get("ts"))
                                msg["date"] = date
                                msg["time"] = time
                    except Exception as e:
                        logger.warning(f"Could not get context messages for {match['ts']}: {str(e)}")
                
                # Format the message with all its context
                date, time = self._format_timestamp(match.get("ts"))
                processed_message = {
                    "ts": match.get("ts"),
                    "date": date,
                    "time": time,
                    "text": match.get("text", ""),
                    "user": user_info,
                    "channel": {
                        "id": match.get("channel", {}).get("id"),
                        "name": match.get("channel", {}).get("name")
                    },
                    "permalink": match.get("permalink"),
                    "score": match.get("score"),
                    "thread_ts": thread_ts,
                    "thread_replies": thread_replies,
                    "context_before": context_before,
                    "context_after": context_after,
                    "reply_count": match.get("reply_count", 0),
                    "reply_users_count": match.get("reply_users_count", 0),
                    "latest_reply": match.get("latest_reply"),
                    "subtype": match.get("subtype"),
                    "is_starred": match.get("is_starred", False),
                    "reactions": match.get("reactions", [])
                }
                processed_messages.append(processed_message)
            
            return processed_messages, None
            
        except Exception as e:
            error_msg = f"Error searching messages: {str(e)}"
            logger.error(error_msg)
            return [], error_msg 

    async def _get_all_users(self, total_limit=None, cursor_limit=None):
        """
        Get all users from the workspace using pagination.
        
        :param total_limit: Maximum total number of users to fetch (default: None, fetch all)
        :param cursor_limit: Maximum number of users to fetch per API call (default: USER_FETCH_LIMIT)
        :return: List of user objects or empty list if error
        """
        logger.info("Getting all users from workspace")
        logger.debug("Using cursor limit: %d, total limit: %s", cursor_limit, total_limit if total_limit is not None else "unlimited")
        all_users = []
        cursor = None
        
        if cursor_limit is None:
            cursor_limit = self.USER_FETCH_LIMIT
        
        # Calculate how many more users we need to fetch
        remaining_to_fetch = total_limit if total_limit is not None else float('inf')
        
        while remaining_to_fetch > 0:
            # Adjust cursor limit to fetch only what we need
            current_cursor_limit = min(cursor_limit, remaining_to_fetch)
            logger.debug("Fetching up to %d more users", current_cursor_limit)
                
            async with self._tier_4_semaphore:  # users_list is Tier 4 (100+ per minute)
                response = await self._handle_rate_limit(
                    self._slack_async_web_client.users_list,
                    cursor=cursor,
                    limit=current_cursor_limit,
                    error_handler=lambda e: None,
                    log_prefix="Get all users: "
                )
                if response and response["ok"]:
                    users = response["members"]
                    all_users.extend(users)
                    
                    # Update remaining count
                    if total_limit is not None:
                        remaining_to_fetch = total_limit - len(all_users)
                        logger.debug("Remaining users to fetch: %d", remaining_to_fetch)
                    
                    # Check if there are more pages
                    cursor = response.get("response_metadata", {}).get("next_cursor")
                    if not cursor:
                        break
                else:
                    break
        
        if all_users:
            logger.info(f"Successfully fetched {len(all_users)} users from workspace")
            return all_users
        return [] 