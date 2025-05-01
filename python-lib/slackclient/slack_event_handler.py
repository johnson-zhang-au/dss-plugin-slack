from utils.logging import logger
import time

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
    
    def handle_message_event(self, message, say, client):
        """
        Handle a message event from Slack.
        Sends a "Thinking..." message first, then updates it with the processed response.
        
        Args:
            message: The message event data from Slack
            say: Function to send a message
            client: Slack WebClient for API calls
        """
        logger.debug(f"Handling message event: {message}")
        
        # Skip messages from the bot itself
        user_id = message.get("user")
        if user_id == self.bot_id:
            logger.debug("Skipping message from bot itself")
            return
        
        # Get channel and thread info
        channel = message.get("channel")
        thread_ts = message.get("thread_ts", message.get("ts"))
        
        # Send "Thinking..." message
        thinking_response = say(
            text="Thinking...",
            channel=channel,
            thread_ts=thread_ts
        )
        
        # Get the timestamp of the "Thinking..." message
        thinking_ts = thinking_response.get("ts")
        
        # Process the message
        response = self.process_message(message)
        
        if response:
            try:
                # Update the "Thinking..." message with the actual response
                client.chat_update(
                    channel=channel,
                    ts=thinking_ts,
                    text=response.get("text", ""),
                    blocks=response.get("blocks")
                )
                logger.debug(f"Updated 'Thinking...' message with response in channel {channel}")
            except Exception as e:
                logger.error(f"Error updating message: {str(e)}", exc_info=True)
                # Fallback: post a new message if update fails
                say(**response)
    
    def handle_mention_event(self, event, say, client):
        """
        Handle an app mention event from Slack.
        Sends a "Thinking..." message first, then updates it with the processed response.
        
        Args:
            event: The app_mention event data from Slack
            say: Function to send a message
            client: Slack WebClient for API calls
        """
        logger.debug(f"Handling mention event: {event}")
        
        # Get channel and thread info
        channel = event.get("channel")
        thread_ts = event.get("thread_ts", event.get("ts"))
        
        # Send "Thinking..." message
        thinking_response = say(
            text="Thinking...",
            channel=channel,
            thread_ts=thread_ts
        )
        
        # Get the timestamp of the "Thinking..." message
        thinking_ts = thinking_response.get("ts")
        
        # Process the mention
        response = self.process_mention(event)
        
        if response:
            try:
                # Update the "Thinking..." message with the actual response
                client.chat_update(
                    channel=channel,
                    ts=thinking_ts,
                    text=response.get("text", ""),
                    blocks=response.get("blocks")
                )
                logger.debug(f"Updated 'Thinking...' message with response in channel {channel}")
            except Exception as e:
                logger.error(f"Error updating message: {str(e)}", exc_info=True)
                # Fallback: post a new message if update fails
                say(**response)
    
    def handle_app_home_event(self, event, client):
        """
        Handle an app_home_opened event from Slack.
        
        Args:
            event: The app_home_opened event data from Slack
            client: Slack WebClient for API calls
        """
        logger.debug(f"Handling app home event: {event}")
        user_id = event.get("user")
        view = self.generate_home_view()
        
        try:
            client.views_publish(user_id=user_id, view=view)
            logger.info(f"Published home view for user {user_id}")
        except Exception as e:
            logger.error(f"Error publishing home view: {str(e)}", exc_info=True)
    
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
        
        # Simulate some processing time
        # In a real implementation, this would be your actual message processing logic
        logger.debug("Starting message processing...")
        
        # Simulate a delay (1-3 seconds)
        processing_time = min(1 + len(text) / 20, 3)
        time.sleep(processing_time)
        
        logger.debug(f"Finished processing message in {processing_time:.2f} seconds")
        
        # Your message processing logic here
        # For now, just echo the message back with some formatting
        response = {
            "text": f"You said: {text}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*You said:*\n>{text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Processed in {processing_time:.2f} seconds"
                        }
                    ]
                }
            ],
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
        
        # Simulate some processing time
        # In a real implementation, this would be your actual message processing logic
        logger.debug("Starting mention processing...")
        
        # Simulate a delay (1-3 seconds)
        processing_time = min(1 + len(text) / 20, 3)
        time.sleep(processing_time)
        
        logger.debug(f"Finished processing mention in {processing_time:.2f} seconds")
        
        # Your mention processing logic here
        # For now, just respond to the mention with some formatting
        response = {
            "text": f"You mentioned me and said: {text}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*You mentioned me and said:*\n>{text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Processed in {processing_time:.2f} seconds"
                        }
                    ]
                }
            ],
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