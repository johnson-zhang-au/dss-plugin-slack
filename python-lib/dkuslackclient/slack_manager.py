from utils.logging import logger
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.adapter.flask import SlackRequestHandler
import threading
import asyncio
import hmac
import hashlib
import time
from .slack_event_handler import SlackEventHandler
from .dku_slack_client import DKUSlackClient

class SlackManager:
    """
    Manages the Slack connection and routes events to the SlackEventHandler.
    Supports both socket mode and HTTP endpoint mode.
    """
    
    def __init__(self, slack_bot_token, slack_app_token=None, slack_signing_secret=None, settings=None):
        """
        Initialize the SlackManager.
        
        Args:
            slack_bot_token: The bot user OAuth token
            slack_app_token: The app-level token for Socket Mode (required for socket mode)
            slack_signing_secret: The signing secret for HTTP request verification
            settings: Dictionary of required settings to customize behavior (contains llm_id and other configuration)
        """
        logger.debug("Initializing SlackManager...")
        self.slack_bot_token = slack_bot_token
        self.slack_app_token = slack_app_token
        self.slack_signing_secret = slack_signing_secret
        self.mode = "socket" if slack_app_token else "http"
        
        # Create settings dictionary with all configuration parameters
        self.settings = settings or {}
        
        # Initialize the event handler
        self.event_handler = None
        
        # Create a DKUSlackClient instance
        self.slack_client_instance = DKUSlackClient(slack_bot_token)
        
        # Initialize Slack App instance
        self.app = App(token=slack_bot_token, signing_secret=slack_signing_secret)
        self.request_handler = None
        self.socket_mode_handler = None
        self.thread = None
        
        # Fetch bot info
        self._initialize_bot_info()
        
        # Create the event handler with bot info and settings
        self.event_handler = SlackEventHandler(
            self.bot_id,
            self.bot_name,
            slack_client=self.slack_client_instance,
            settings=self.settings
        )
        
        # Set up event handlers
        self._setup_listeners()
        
        logger.info(f"SlackManager initialized in {self.mode} mode with LLM ID: {self.settings.get('llm_id')}")
        
    def _initialize_bot_info(self):
        """Get the bot's ID and name."""
        try:
            logger.debug("Fetching app authentication info...")
            # Use the DKUSlackClient's AsyncWebClient directly
            auth_info = asyncio.run(self.slack_client_instance.slack_async_web_client.auth_test())
            
            self.bot_id = auth_info["user_id"]
            self.bot_name = auth_info["user"]
            logger.info(f"App initialized with User ID: {self.bot_id} and Name: {self.bot_name}")
        except Exception as e:
            logger.error(f"Failed to get app info: {str(e)}", exc_info=True)
            self.bot_id = None
            self.bot_name = None
    
    def _setup_listeners(self):
        """Set up event listeners for the Slack app."""
        logger.debug("Setting up event listeners...")
        
        # Handle message events
        @self.app.message()
        def handle_message(message, say, client):
            logger.debug(f"Received message event: {message}")
            # Delegate handling to the event handler
            self.event_handler.handle_message_event(message, say, client)
        
        # Handle app mention events
        @self.app.event("app_mention")
        def handle_app_mention(event, say, client):
            logger.debug(f"Received app mention event: {event}")
            # Delegate handling to the event handler
            self.event_handler.handle_mention_event(event, say, client)
        
        # Handle app home opened events
        @self.app.event("app_home_opened")
        def handle_app_home_opened(event, client):
            logger.debug(f"Received app home opened event: {event}")
            # Delegate handling to the event handler
            self.event_handler.handle_app_home_event(event, client)
    
    def start(self):
        """Start the Slack integration based on the configured mode."""
        if self.mode == "socket":
            return self._start_socket_mode()
        else:
            return self._prepare_http_mode()
    
    def _start_socket_mode(self):
        """Start the Socket Mode handler in a separate thread."""
        if not self.slack_app_token:
            error_msg = "Cannot start in socket mode without an app token"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        try:
            logger.info("Starting Slack in socket mode...")
            
            # Initialize the Socket Mode handler
            self.socket_mode_handler = SocketModeHandler(self.app, self.slack_app_token)
            
            # Start in a separate thread
            self.thread = threading.Thread(
                target=self._run_socket_handler,
                name="SlackSocketHandler"
            )
            self.thread.daemon = True
            self.thread.start()
            
            logger.info(f"Slack socket mode handler started in a background thread (Thread ID: {self.thread.ident}, Name: {self.thread.name})")
            return True
        except Exception as e:
            logger.error(f"Failed to start socket mode: {str(e)}", exc_info=True)
            raise
    
    def _run_socket_handler(self):
        """Run the socket mode handler (called in a thread)."""
        try:
            # Set thread name in logs for better traceability
            logger.debug(f"Socket mode handler thread starting (Thread ID: {threading.get_ident()}, Name: {threading.current_thread().name})")
            self.socket_mode_handler.start()
        except Exception as e:
            logger.error(f"Error in socket mode thread: {str(e)}", exc_info=True)
            # Re-raise the exception to ensure it's visible in the main process
            raise
    
    def _prepare_http_mode(self):
        """Prepare for HTTP mode by creating a request handler."""
        logger.info("Preparing Slack for HTTP mode...")
        self.request_handler = SlackRequestHandler(self.app)
        logger.info("Slack HTTP request handler initialized")
        return self.request_handler
    
    def cleanup(self):
        """Clean up resources."""
        logger.info("Starting cleanup process...")
        
        try:
            if self.socket_mode_handler and self.mode == "socket":
                logger.debug("Closing socket mode handler...")
                self.socket_mode_handler.close()
                logger.info("Socket mode handler closed")
        except Exception as e:
            logger.error(f"Error closing socket mode handler: {str(e)}", exc_info=True)
        
        logger.info("Cleanup process completed")
    
    def handle_http_request(self, request):
        """
        Handle an HTTP request using the Flask adapter.
        
        Args:
            request: Flask request object
            
        Returns:
            Flask response
        """
        if self.request_handler:
            return self.request_handler.handle(request)
        else:
            logger.error("No request handler available")
            raise ValueError("Slack manager not initialized for HTTP mode") 