from utils.logging import logger
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt import App
from slack_sdk import WebClient
import threading
import atexit


class SlackSocketClient:
    """Manages the Slack socket client for real-time communication with Slack."""

    def __init__(self, slack_bot_token: str, slack_app_token: str) -> None:
        """Initialize the Slack Socket Client.
        
        Args:
            slack_bot_token: The bot user OAuth token.
            slack_app_token: The app-level token for Socket Mode.
        """
        logger.debug("Initializing SlackSocketClient...")
        self.slack_bot_token = slack_bot_token
        self.slack_app_token = slack_app_token
        self.app = None
        self.socket_mode_handler = None
        self.client = None
        self.tools = []
        self.bot_id = None
        self.bot_name = None
        self.thread = None
        logger.info("SlackSocketClient initialized successfully")

    def initialize_bot_info(self) -> None:
        """Get the app's ID and other info."""
        try:
            logger.debug("Fetching app authentication info...")
            auth_info = self.client.auth_test()
            self.bot_id = auth_info["user_id"]
            self.bot_name = auth_info["user"]
            logger.info(f"App initialized with User ID: {self.bot_id} and Name: {self.bot_name}")
        except Exception as e:
            logger.error(f"Failed to get app info: {str(e)}", exc_info=True)
            self.bot_id = None

    def handle_mention(self, event, say):
        """Handle mentions of the app in channels."""
        logger.debug(f"Received mention event: {event}")
        self._process_message(event, say)

    def handle_message(self, message, say):
        """Handle direct messages to the app."""
        logger.debug(f"Received message event: {message}")
        # Only process direct messages
        if message.get("channel_type") == "im" and not message.get("subtype"):
            self._process_message(message, say)

    def handle_home_opened(self, event, client):
        """Handle when a user opens the App Home tab."""
        user_id = event["user"]
        logger.debug(f"User {user_id} opened App Home")

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Welcome to MCP Assistant!"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "I'm an AI assistant with access to tools and resources "
                        "through the Model Context Protocol."
                    ),
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Available Tools:*"},
            },
        ]

        # Add tools
        for tool in self.tools:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"• *{tool.name}*: {tool.description}",
                    },
                }
            )

        # Add usage section
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*How to Use:*\n• Send me a direct message\n"
                        "• Mention me in a channel with @MCP Assistant"
                    ),
                },
            }
        )

        try:
            logger.debug(f"Publishing home view for user {user_id}")
            client.views_publish(
                user_id=user_id, view={"type": "home", "blocks": blocks}
            )
            logger.info(f"Successfully published home view for user {user_id}")
        except Exception as e:
            logger.error(f"Error publishing home view: {str(e)}", exc_info=True)

    def _process_message(self, event, say):
        """Process incoming messages and generate responses."""
        channel = event["channel"]
        user_id = event.get("user")
        logger.debug(f"Processing message from user {user_id} in channel {channel}")

        # Skip messages from the bot itself
        if user_id == self.bot_id:
            logger.debug("Skipping message from bot itself")
            return

        # Get text and remove bot mention if present
        text = event.get("text", "")
        if self.bot_id:
            text = text.replace(f"<@{self.bot_id}>", "").strip()

        thread_ts = event.get("thread_ts", event.get("ts"))
        logger.debug(f"Message text after processing: {text}")

        try:
            # Send the response to the user
            logger.debug(f"Sending response to channel {channel}")
            say(text=text, channel=channel, thread_ts=thread_ts)
            logger.info(f"Successfully sent response to channel {channel}")
        except Exception as e:
            error_message = f"I'm sorry, I encountered an error: {str(e)}"
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            say(text=error_message, channel=channel, thread_ts=thread_ts)

    def run_socket_mode(self):
        """Run the socket mode handler in a blocking way."""
        try:
            # Initialize the Slack app and handlers
            self.app = App(token=self.slack_bot_token)
            self.socket_mode_handler = SocketModeHandler(
                self.app, 
                self.slack_app_token
            )
            self.client = WebClient(token=self.slack_bot_token)
            
            # Set up event handlers
            logger.debug("Setting up event handlers...")
            self.app.event("app_mention")(self.handle_mention)
            self.app.message()(self.handle_message)
            self.app.event("app_home_opened")(self.handle_home_opened)
            
            self.initialize_bot_info()
            
            # Start the socket mode handler
            logger.debug("Starting socket mode handler...")
            self.socket_mode_handler.start()
            logger.info("Slack socket client started and waiting for messages")
        except Exception as e:
            logger.error(f"Error in socket mode thread: {str(e)}", exc_info=True)
            raise

    def start(self) -> None:
        """Start the Slack Socket Client in a separate thread."""
        logger.info("Starting Slack Socket Client...")
        self.thread = threading.Thread(target=self.run_socket_mode)
        self.thread.daemon = True  # Make the thread a daemon so it exits when the main program exits
        self.thread.start()
        logger.info("Slack socket client thread started")

    def cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Starting cleanup process...")
        try:
            if self.socket_mode_handler:
                logger.debug("Closing socket mode handler...")
                self.socket_mode_handler.close()
                logger.info("Socket mode handler closed successfully")
        except Exception as e:
            logger.error(f"Error closing socket mode handler: {str(e)}", exc_info=True)

        logger.info("Cleanup process completed")
