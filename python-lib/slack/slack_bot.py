
import re
from utils.logging import logger
from base.bot_base import BotBase

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.signature import SignatureVerifier

from cachetools import TTLCache
from datetime import datetime

class SlackChatBot(BotBase):
    """
    A bot for interacting with Slack, providing functionality for handling messages,
    querying Dataiku Answers, and sending responses or reactions.
    """
    def __init__(self, dku_answers_client, slack_bot_auth):
        BotBase.__init__(self, dku_answers_client)
        self._slack_token = ""
        self._slack_signing_secret = ""
        self._bot_user_id = ""
        self._bot_user_name = ""
        self.signature_verifier = None
        self._slack_client = None
        self._bot_prefix = None
        self._slack_user_cache = TTLCache(maxsize=100, ttl=86400)  # 24 hours
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

    def _get_user_info(self, user_id):
        """
        Retrieves user information (username, email) from Slack.

        :param user_id: Slack user ID to fetch information for.
        :return: Tuple containing the display name and email of the user.
        """
        cached_user_info = self._slack_user_cache.get(user_id)
        if cached_user_info:
            display_name = cached_user_info["name"]
            email = cached_user_info["email"]
            logger.info(f"Using cached user info for {display_name} ({email})")
            return display_name, email
        else:
            try:
                logger.info(f"Cache miss for user {user_id}. Fetching data from Slack API...")
                response = self._slack_client.users_info(user=user_id)
                if response["ok"]:
                    user_info = response["user"]
                    username = user_info.get("real_name", "Unknown User")
                    display_name = user_info.get("profile", {}).get("display_name", username)
                    email = user_info.get("profile", {}).get("email", "No email found")

                    # Store the data along with the timestamp
                    logger.info(f"Fetched new data for user {user_id} and cached it.")
                    self._slack_user_cache[user_id] = {
                        "email": email,
                        "name": display_name or username,
                        "timestamp": datetime.now()  # Store the timestamp of cache creation
                    }
                    return display_name or username, email
                else:
                    logger.warning(f"Slack API returned an error for user {user_id}: {response['error']}")
            except SlackApiError as e:
                logger.error(f"Slack API Error while fetching data for user: {e.response['error']}, ", exc_info=True)
            return None, None

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