from dataiku.customwebapp import get_webapp_config
from utils.logging import logger
from slackclient.slack_socket_client import SlackSocketClient
import asyncio


async def run_slack_app():
    """Initialize and run the Slack app."""
    logger.info("Starting Slack Socket Client initialization...")

    config = get_webapp_config()
    if not config.get("slack_bot_token") or not config.get("slack_app_token"):
        error_msg = "SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in webapp configuration"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.debug("Creating SlackSocketClient instance...")
    slack_bot = SlackSocketClient(
        config.get("slack_bot_token"),
        config.get("slack_app_token")
    )

    try:
        logger.info("Starting Slack app...")
        await slack_bot.start()
        logger.info("Slack app is running and waiting for messages...")
    except Exception as e:
        logger.error(f"Error occurred while running Slack app: {str(e)}", exc_info=True)
        raise

# Create and run the event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(run_slack_app())