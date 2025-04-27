import asyncio
import re
from utils.logging import logger
from slack_sdk import WebClient
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.signature import SignatureVerifier

from cachetools import TTLCache
from datetime import datetime

class SlackChatBot():
    """
    A bot for interacting with Slack, providing functionality for handling messages,
    querying Dataiku Answers, and sending responses or reactions.
    """
    def __init__(self, slack_bot_auth):
        self._slack_token = ""
        self._slack_signing_secret = ""
        self._bot_user_id = ""
        self._bot_user_name = ""
        self.signature_verifier = None
        self._slack_client = None
        self._slack_async_client = None
        self._bot_prefix = None
        self._slack_user_cache = TTLCache(maxsize=100, ttl=86400)  # 24 hours
        self._slack_channel_name_cache = TTLCache(maxsize=100, ttl=86400)  # 24 hours
        self._slack_channel_members_cache = TTLCache(maxsize=100, ttl=86400)  # 24 hours
        self._api_semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent API calls
        self._load_credentials(slack_bot_auth)
        self._initialize_slack_client()

    def _load_credentials(self, slack_bot_auth):
        """
        Loads Slack bot credentials from the Dataiku environment under the webapp's run as user's credntails. 
        The reason why we don't put this under the webapp's setting is 
        because all the parameters of the webapp's setting will be visible in clear text from the page source on the client side.
        R&D (according to Clement) will investigate improvements for the future.
        Ensures required secrets are available, and sets up the signature verifier.
        """
        logger.debug("Loading Slack bot authentication settings from webapp config...")
        self._slack_token = slack_bot_auth.get("slack_token", None)
        self._slack_signing_secret = slack_bot_auth.get("slack_signing_secret", None)
        self._bot_user_id = slack_bot_auth.get("slack_bot_user_id", None)
        self._bot_user_name = slack_bot_auth.get("slack_bot_user_name", None)
        
        if not self._slack_token or not self._slack_signing_secret or not self._bot_user_id or not self._bot_user_name:
            logger.error("Some required Slack bot credentials (slack_token, slack_signing_secret, bot_user_id, bot_user_name) are missing!")
            raise ValueError("Required Slack bot credentials are missing.")
        self.signature_verifier = SignatureVerifier(self._slack_signing_secret)
        self._bot_prefix = f"<@{self._bot_user_id}>"

    def _initialize_slack_client(self):
        """
        Initializes the Slack WebClient using the provided bot token.
        """
        self._slack_client = WebClient(token=self._slack_token)
        self._slack_async_client = AsyncWebClient(token=self._slack_token)
        logger.debug("Slack WebClient initialized successfully.")
        logger.debug("Slack AsyncWebClient initialized successfully.")

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

    async def _get_user_by_id(self, user_id):
        """
        Get user information from Slack by user ID.
        Caches the results for future lookups.
        
        :param user_id: Slack user ID to fetch information for
        :return: Tuple containing (user_id, display_name, email) or (None, None, None) if not found
        """
        # Check cache first
        cached_user = self._slack_user_cache.get(user_id)
        if cached_user:
            logger.info(f"Using cached user info for {cached_user['name']} ({cached_user['email']})")
            return user_id, cached_user["name"], cached_user["email"]
        
        # If not in cache, get user info
        try:
            response = await self._slack_async_client.users_info(user=user_id)
            if response["ok"]:
                return self._cache_user_info(user_id, response["user"])
            else:
                logger.warning(f"Slack API returned an error for user {user_id}: {response['error']}")
        except SlackApiError as e:
            logger.error(f"Slack API Error: {e.response['error']}", exc_info=True)
        return None, None, None

    async def _get_user_by_email(self, email):
        """
        Get user information from Slack by email.
        Caches the results for future lookups.
        
        :param email: Email address to look up
        :return: Tuple containing (user_id, display_name, email) or (None, None, None) if not found
        """
        # Check if we have this email in our cache
        for cached_user_id, cached_user in self._slack_user_cache.items():
            if cached_user.get("email") == email:
                logger.info(f"Found cached user info for email {email}")
                return cached_user_id, cached_user["name"], cached_user["email"]

        # If not in cache, try to find the user
        try:
            response = await self._slack_async_client.users_lookupByEmail(email=email)
            if response["ok"]:
                user_info = response["user"]
                user_id = user_info["id"]
                return self._cache_user_info(user_id, user_info)
            else:
                logger.warning(f"Could not find user with email {email}: {response['error']}")
        except SlackApiError as e:
            logger.error(f"Error looking up user by email {email}: {e.response['error']}")
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
        channels = await self.fetch_channels()
        for channel in channels:
            if channel["name"] == channel_name:
                logger.info(f"Found channel '{channel_name}' with ID: {channel['id']}")
                return channel["id"]
            
        logger.warning(f"Could not find channel with name '{channel_name}'")
        return None

    def _send_reaction(self, channel_id, reaction_name, user_id, event_timestamp):
        """
        Sends a reaction emoji to a Slack message.

        :param channel_id: ID of the channel where the message was sent.
        :param reaction_name: Name of the reaction emoji.
        :param user_id: ID of the user who sent the message.
        :param event_timestamp: Timestamp of the Slack message.
        """
        try:
            self._slack_client.reactions_add(channel=channel_id, name=reaction_name, timestamp=event_timestamp)
            logger.info(f"Successfully sent reaction '{reaction_name}' to user {user_id} in channel {channel_id}")
        except SlackApiError as e:
            logger.error(f"Failed to send reaction: {e}", exc_info=True)

    async def fetch_channels(self):
        """Fetch all channels the bot has access to."""
        logger.info("Fetching all channels from Slack API")
        try:
            response = await self._slack_async_client.conversations_list(types="public_channel,private_channel")
            if response["ok"]:
                channels = response.get("channels", [])
                logger.info(f"Successfully fetched {len(channels)} channels")
                # Cache channel names and IDs
                for channel in channels:
                    self._slack_channel_name_cache[channel["name"]] = {
                        "id": channel["id"],
                        "timestamp": datetime.now()
                    }
                logger.debug(f"Cached {len(channels)} channel name/ID mappings")
                return channels
            else:
                logger.error(f"Failed to fetch channels: {response['error']}")
            return []
        except SlackApiError as e:
            logger.error(f"Error fetching channels: {e.response['error']}", exc_info=True)
            return []

    async def fetch_messages(self, channel_id, start_timestamp, channel_name=None):
        """Fetch messages from a specific channel."""
        try:
            messages = []
            next_cursor = None

            while True:
                async with self._api_semaphore:
                    response = await self._slack_async_client.conversations_history(
                        channel=channel_id,
                        oldest=start_timestamp,
                        limit=200,
                        cursor=next_cursor
                    )
                for message in response.get("messages", []):
                    # Inject channel_id and channel_name into each message
                    message["channel_id"] = channel_id
                    message["channel_name"] = channel_name
                    messages.append(message)

                next_cursor = response.get("response_metadata", {}).get("next_cursor")
                if not next_cursor:
                    break

            return messages
        except SlackApiError as e:
            logger.error(f"Error fetching messages from channel {channel_id}: {e.response['error']}")
            return []

    async def _get_channel_members(self, channel_id):
        """
        Get channel members with caching.
        
        :param channel_id: The channel ID to get members for
        :return: List of member IDs or empty list if error
        """
        logger.debug(f"Getting members for channel {channel_id}")
        cached_members = self._slack_channel_members_cache.get(channel_id)
        if cached_members:
            logger.info(f"Cache hit for channel {channel_id} members ({len(cached_members['members'])} members)")
            return cached_members["members"]

        logger.debug(f"Cache miss for channel {channel_id} members, fetching from API")
        async with self._api_semaphore:
            try:
                response = await self._slack_async_client.conversations_members(channel=channel_id)
                if response["ok"]:
                    members = response["members"]
                    logger.info(f"Successfully fetched {len(members)} members for channel {channel_id}")
                    self._slack_channel_members_cache[channel_id] = {
                        "members": members,
                        "timestamp": datetime.now()
                    }
                    logger.debug(f"Cached members for channel {channel_id}")
                    return members
                else:
                    logger.warning(f"Could not get members for channel {channel_id}: {response['error']}")
            except SlackApiError as e:
                logger.error(f"Error getting channel members for {channel_id}: {e.response['error']}", exc_info=True)
            return []

    async def fetch_messages_from_channels(self, start_timestamp, channel_names=None, user_emails=None):
        """Fetch messages from specified channels or all channels."""
        logger.info(f"Starting message fetch for {len(channel_names) if channel_names else 'all'} channels")
        
        channels = await self.fetch_channels()
        channel_ids = []
        user_ids = []

        # Convert channel names to IDs
        if channel_names:
            logger.debug(f"Converting {len(channel_names)} channel names to IDs")
            for channel_name in channel_names:
                channel_id = await self._get_channel_id_by_name(channel_name)
                if channel_id:
                    channel_ids.append(channel_id)
                    logger.debug(f"Found channel ID {channel_id} for {channel_name}")
                else:
                    logger.warning(f"Could not find channel ID for {channel_name}")
        else:
            channel_ids = [channel["id"] for channel in channels]
            logger.debug(f"Using all {len(channel_ids)} available channels")

        # Convert user emails to IDs
        if user_emails:
            logger.debug(f"Converting {len(user_emails)} user emails to IDs to filter messages from specific users")
            for email in user_emails:
                user_id, _, _ = await self._get_user_by_email(email)
                if user_id:
                    user_ids.append(user_id)
                    logger.debug(f"Found user ID {user_id} for {email}")
                else:
                    logger.warning(f"Could not find user ID for {email}")

        # Filter channels based on user membership if user_ids are provided
        if user_ids:
            logger.info(f"Filtering channels for {len(user_ids)} users")
            # Get members for all channels in parallel with throttling
            member_tasks = [self._get_channel_members(channel["id"]) for channel in channels if channel["id"] in channel_ids]
            channel_members = await asyncio.gather(*member_tasks)
            
            # Filter channels that have any of the specified users as members
            filtered_channels = [
                channel for channel, members in zip(channels, channel_members)
                if channel["id"] in channel_ids and any(user_id in members for user_id in user_ids)
            ]
            channels = filtered_channels
            logger.info(f"Found {len(channels)} channels with matching users")
        else:
            channels = [channel for channel in channels if channel["id"] in channel_ids]
            logger.debug(f"Using {len(channels)} channels without user filtering")

        # Fetch messages from all channels in parallel with throttling
        logger.info(f"Fetching messages from {len(channels)} channels")
        tasks = [
            self.fetch_messages(channel["id"], start_timestamp, channel_name=channel["name"])
            for channel in channels
        ]
        results = await asyncio.gather(*tasks)
        all_messages = [message for channel_messages in results for message in channel_messages]
        logger.info(f"Successfully fetched {len(all_messages)} messages from {len(channels)} channels")
        return all_messages