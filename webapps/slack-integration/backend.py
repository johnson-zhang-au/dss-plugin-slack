from dataiku.customwebapp import get_webapp_config
from utils.logging import logger
from slackclient.slack_manager import SlackManager
from flask import Flask, request
import atexit


# Global variables
slack_manager = None
flask_app = None  # Will be initialized only in HTTP mode

def setup_logging(logging_level):
    """
    Sets up the logging level using the logger.
    """
    try:
        # Set the logging level dynamically
        logger.set_level(logging_level)
        logger.info(f"Logging initialized with level: {logging_level}")
    except ValueError as e:
        # Handle invalid logging levels
        logger.error(f"Invalid logging level '{logging_level}': {str(e)}")
        raise

def setup_flask_for_http_mode():
    """
    Set up Flask app with routes for HTTP endpoint mode.
    Only called when mode is 'http'.
    """

    
    @app.route('/slack/events', methods=['POST'])
    def slack_events():
        """
        Handle incoming Slack events via HTTP endpoint.
        This uses the Slack Bolt Flask adapter to process requests.
        """
        global slack_manager
        
        # Check if we're initialized
        if not slack_manager:
            logger.error("Slack manager not initialized")
            return "Slack manager not initialized", 500
        
        for header, value in request.headers.items():
            logger.debug(f"Header: {header} -> Value: {value}")
        
        # Process the request using the SlackManager's HTTP request handler
        return slack_manager.handle_http_request(request)
    
    logger.info("Flask app initialized with /slack/events endpoint")

def cleanup():
    """Clean up resources when the application exits."""
    try:
        if slack_manager:
            logger.info("Shutting down Slack manager...")
            slack_manager.cleanup()
    except Exception as e:
        logger.error(f"Failed to clean up: {str(e)}", exc_info=True)

# Register the cleanup function with atexit
atexit.register(cleanup)

# Main initialization
def init():
    """Initialize and run the Slack app."""
    global slack_manager, flask_app
    
    logger.info("Starting Slack integration initialization...")
    config = get_webapp_config()

    # Get the logging level from the configuration, default to INFO
    logging_level = config.get("logging_level", "INFO")
    setup_logging(logging_level)

    # Get the integration mode
    mode = config.get("mode", "socket")
    logger.info(f"Starting in {mode} mode")

    # Get Slack authentication settings
    slack_auth = config.get("slack_auth_settings", {})
    if not slack_auth:
        error_msg = "Slack authentication settings are missing"
        logger.error(error_msg)
        raise ValueError(error_msg)

    slack_bot_token = slack_auth.get("slack_token")
    if not slack_bot_token:
        error_msg = "slack_token is missing from authentication settings"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Initialize based on mode
    if mode == "socket":
        # Socket mode requires an app token
        slack_app_token = slack_auth.get("slack_app_token")
        if not slack_app_token:
            error_msg = "slack_app_token is required for socket mode"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Initialize the SlackManager with app token for socket mode
        logger.debug("Creating SlackManager for socket mode...")
        slack_manager = SlackManager(
            slack_bot_token=slack_bot_token,
            slack_app_token=slack_app_token
        )
        
        # Start the SlackManager in socket mode
        try:
            logger.info("Starting Slack app in socket mode...")
            slack_manager.start()
            logger.info("Slack app is running in socket mode and waiting for messages...")
        except Exception as e:
            logger.error(f"Error occurred while running Slack app in socket mode: {str(e)}", exc_info=True)
            raise

    elif mode == "http":
        # Setup Flask app for HTTP mode
        flask_app = setup_flask_for_http_mode()
        
        # HTTP mode uses the signing secret for request verification
        slack_signing_secret = slack_auth.get("slack_signing_secret")
        if not slack_signing_secret:
            logger.warn("slack_signing_secret is missing. Request verification will be limited.")
        
        # Initialize the SlackManager for HTTP mode
        logger.debug("Creating SlackManager for HTTP endpoint mode...")
        slack_manager = SlackManager(
            slack_bot_token=slack_bot_token,
            slack_signing_secret=slack_signing_secret
        )
        
        # Prepare the HTTP request handler
        logger.info("Preparing Slack app in HTTP endpoint mode...")
        slack_manager.start()
        logger.info("Slack app is running in HTTP endpoint mode")
        logger.info("Waiting for events at /slack/events endpoint")
    
    else:
        error_msg = f"Invalid mode: {mode}. Must be either 'socket' or 'http'"
        logger.error(error_msg)
        raise ValueError(error_msg)

# Run the initialization
init()