from utils.logging import logger

class SlackEventHandler:
    """
    Handles Slack event processing logic, separate from the transport mechanism.
    This class contains the business logic for processing Slack events.
    """
    
    def __init__(self, bot_id=None, bot_name=None):
        """
        Initialize the SlackEventHandler.
        
        Args:
            bot_id: The bot's user ID (optional)
            bot_name: The bot's name (optional)
        """
        logger.debug("Initializing SlackEventHandler...")
        self.bot_id = bot_id
        self.bot_name = bot_name
        self.tools = []
    
    def process_event(self, event_data):
        """
        Process a Slack event or message.
        
        Args:
            event_data (dict): The event data from Slack
        """
        try:
            logger.debug(f"Processing event: {event_data.get('type', 'unknown')}")
            
            # Handle different event types
            event_type = event_data.get("type")
            
            if event_type == "event_callback":
                # Process the inner event from Events API
                inner_event = event_data.get("event", {})
                inner_event_type = inner_event.get("type")
                
                if inner_event_type == "message":
                    return self.process_message(inner_event)
                elif inner_event_type == "app_mention":
                    return self.process_mention(inner_event)
                else:
                    logger.debug(f"Received unhandled inner event type: {inner_event_type}")
            
            elif event_type == "message":
                # Direct message from socket mode
                return self.process_message(event_data)
            
            elif event_type == "app_mention":
                # App mention from socket mode
                return self.process_mention(event_data)
            
            else:
                logger.debug(f"Received unhandled event type: {event_type}")
            
            return None
        
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}", exc_info=True)
            return {"text": f"I'm sorry, an error occurred: {str(e)}"}
    
    def process_message(self, message_data):
        """
        Process a message event.
        
        Args:
            message_data (dict): The message event data
            
        Returns:
            dict: Response data or None
        """
        user_id = message_data.get("user")
        
        # Skip messages from the bot itself
        if user_id == self.bot_id:
            logger.debug("Skipping message from bot itself")
            return None
            
        # Get text and remove bot mention if present
        text = message_data.get("text", "")
        if self.bot_id:
            text = text.replace(f"<@{self.bot_id}>", "").strip()
        
        logger.info(f"Processing message from user {user_id}")
        logger.debug(f"Message text: {text}")
        
        # Your message processing logic here
        # For example:
        # 1. Process message with your business logic
        # 2. Generate a response
        # 3. Return the response data
        
        # For now, just echo the message back
        response = {
            "text": f"You said: {text}",
            "channel": message_data.get("channel"),
            "thread_ts": message_data.get("thread_ts", message_data.get("ts"))
        }
        
        return response
    
    def process_mention(self, mention_data):
        """
        Process an app mention event.
        
        Args:
            mention_data (dict): The mention event data
            
        Returns:
            dict: Response data or None
        """
        user_id = mention_data.get("user")
        text = mention_data.get("text", "")
        
        # Remove the bot mention from the text
        if self.bot_id:
            text = text.replace(f"<@{self.bot_id}>", "").strip()
        
        logger.info(f"Processing mention from user {user_id}")
        logger.debug(f"Mention text: {text}")
        
        # Your mention processing logic here
        # For now, just respond to the mention
        response = {
            "text": f"You mentioned me and said: {text}",
            "channel": mention_data.get("channel"),
            "thread_ts": mention_data.get("thread_ts", mention_data.get("ts"))
        }
        
        return response
    
    def generate_home_view(self):
        """
        Generate the App Home view content.
        
        Returns:
            dict: The view object for the App Home
        """
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Welcome to Slack Integration!"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "This is a Slack integration for Dataiku DSS.",
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
                        f"• Mention me in a channel with @{self.bot_name or 'Bot'}"
                    ),
                },
            }
        )
        
        return {"type": "home", "blocks": blocks} 