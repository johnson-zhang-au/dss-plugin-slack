from dataiku.customwebapp import get_webapp_config
from utils.logging import logger
from slackclient.slack_socket_client import SlackSocketClient
import asyncio

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

        

        
"""Initialize and run the Slack app."""
logger.info("Starting Slack Socket Client initialization...")
config = get_webapp_config()

logger.info(f"config is {config}")

# Get the logging level from the configuration, default to INFO
logging_level = config.get("logging_level", "INFO")

setup_logging(logging_level)

slack_auth = config["slack_auth_settings"]

slack_bot_token = slack_auth["slack_token"]
slack_app_token = slack_auth["slack_app_token"]
if not slack_bot_token or not slack_app_token:
    error_msg = "SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in webapp configuration"
    logger.error(error_msg)
    raise ValueError(error_msg)
logger.debug("Creating SlackSocketClient instance...")
slack_socket_client = SlackSocketClient(
    slack_bot_token,
    slack_app_token
)

# Create and run the event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

try:
    logger.info("Starting Slack app...")
    # Start the socket mode handler in a non-blocking way
    loop.create_task(slack_socket_client.start())
    logger.info("Slack app is running and waiting for messages...")
except Exception as e:
    logger.error(f"Error occurred while running Slack app: {str(e)}", exc_info=True)
    raise