from utils.logging import logger
import time
import dataiku
import asyncio
from collections import defaultdict

class SlackEventHandler:
    """
    Handles Slack event processing logic, separate from the transport mechanism.
    This class contains the business logic for processing Slack events.
    """
    
    # Constants
    MENTION_WITHOUT_TEXT = """
Hi there! You didn't provide a message with your mention.
Mention me again in this thread so that I can help you out!
"""
    DEFAULT_LOADING_TEXT = "Thinking..."
    SYSTEM_PROMPT = """You are a helpful assistant. Your name is {bot_name}.
Respond using Slack markdown.
* Use *bold* for emphasis
* Use _italic_ for subtle emphasis
* Use `code` for code snippets
* Use ```code blocks``` for multi-line code
* Use >quote for quotes
* Use bullet lists with * or - when listing items
* Use numbered lists with 1. 2. etc. when sequence matters
* Format URLs as <url|text> for prettier links
"""
    SLACK_ADDITIONAL_INSTRUCTIONS = "\nRespond using Slack markdown."
    
    def __init__(self, bot_id=None, bot_name=None, llm_id=None, slack_client=None):
        """
        Initialize the SlackEventHandler.
        
        Args:
            bot_id: The bot's user ID (optional)
            bot_name: The bot's name (optional)
            llm_id: The ID of the LLM to use for generating responses
            slack_client: SlackClient instance for API calls
        """
        logger.debug("Initializing SlackEventHandler...")
        self.bot_id = bot_id
        self.bot_name = bot_name
        self.llm_id = llm_id
        self.slack_client = slack_client
        self.tools = []
        
        # LLM info cache
        self._llm_name = None
        self._llm_type = None
        
        # Initialize LLM client if llm_id is provided
        self.llm_client = None
        if self.llm_id:
            try:
                logger.debug(f"Initializing LLM client with ID: {self.llm_id}")
                client = dataiku.api_client()
                project = client.get_default_project()
                self.llm_client = project.get_llm(self.llm_id)
                logger.info(f"LLM client initialized for {self.llm_id}")
            except Exception as e:
                logger.error(f"Failed to initialize LLM client: {str(e)}", exc_info=True)
                self.llm_client = None

    def get_llm_info(self):
        """
        Get LLM information (name and type) from Dataiku API.
        Caches results for subsequent calls.
        
        Returns:
            tuple: A tuple containing (llm_name, llm_type)
        """
        # Return cached values if available
        if self._llm_name is not None and self._llm_type is not None:
            return self._llm_name, self._llm_type
            
        # Default values
        llm_name = "Unknown LLM"
        llm_type = "UNKNOWN"
        
        # Attempt to retrieve LLM info from Dataiku API
        if self.llm_id:
            try:
                client = dataiku.api_client()
                project = client.get_default_project()
                llm_list = project.list_llms()
                
                for llm in llm_list:
                    if llm.get('id') == self.llm_id:
                        llm_name = llm.get('friendlyName', 'Unknown LLM')
                        llm_type = llm.get('type', 'UNKNOWN')
                        break
                
                logger.debug(f"Found LLM name for {self.llm_id}: {llm_name}, type: {llm_type}")
            except Exception as e:
                logger.error(f"Error getting LLM info: {str(e)}", exc_info=True)
        
        # Cache the values
        self._llm_name = llm_name
        self._llm_type = llm_type
        
        return llm_name, llm_type

    def process_rag_response(self, response_text, text):
        """
        Process a RAG response from the LLM.
        
        Args:
            response_text: The raw response text from the LLM
            text: The original user query
            
        Returns:
            tuple: A tuple containing (processed_text, blocks, success)
            where success is a boolean indicating if the response was processed as a RAG response
        """
        # Check if response looks like JSON
        if not (response_text.strip().startswith("{") and response_text.strip().endswith("}")):
            return response_text, None, False
            
        try:
            import json
            parsed_response = json.loads(response_text)
            
            # Check if the response has the expected RAG format
            if "result" not in parsed_response or "sources" not in parsed_response:
                return response_text, None, False
                
            # Extract the result text and sources
            result_text = parsed_response["result"]
            sources = parsed_response["sources"]
            
            # Format sources as markdown links
            source_items = []
            for index, src in enumerate(sources):
                file = src.get("file", "Unknown source")
                url = src.get("url", "")
                
                if url:
                    source_items.append(f"{index + 1}. <{url}|{file.replace('>', ' - ')}>")
                else:
                    source_items.append(f"{index + 1}. {file}")
            
            source_markdown = "\n".join(source_items)
            
            # Create special blocks for RAG response with sources
            formatted_blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"_You asked: {text.replace('_', '')}_"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": result_text
                    }
                }
            ]
            
            # Add sources section if there are sources
            if source_items:
                formatted_blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Sources:*\n{source_markdown}"
                        }
                    }
                )
            
            return result_text, formatted_blocks, True
            
        except Exception as e:
            logger.error(f"Error processing RAG response: {str(e)}", exc_info=True)
            return response_text, None, False

    async def get_conversation_history(self, channel, thread_ts=None):
        """
        Get conversation history from Slack using the SlackClient.
        
        Args:
            channel: The channel ID
            thread_ts: The thread timestamp (optional)
            
        Returns:
            list: The conversation history formatted for LLM
        """
        if not self.slack_client:
            logger.warning("SlackClient not available. Cannot fetch conversation history.")
            return []
        
        try:
            if thread_ts:
                # Fetch thread replies
                replies, error = await self.slack_client.fetch_thread_replies(
                    channel_id=channel,
                    thread_ts=thread_ts,
                    resolve_users=True
                )
                
                if error:
                    logger.error(f"Error fetching thread replies: {error}")
                    return []
                
                # Convert to LLM-compatible format
                conversation = []
                for message in replies:
                    # Skip messages from the bot itself
                    if message.get("user") == self.bot_id:
                        role = "assistant"
                    else:
                        role = "user"
                    
                    conversation.append({
                        "role": role,
                        "content": message.get("text", "")
                    })
                
                return conversation
            else:
                # Get recent messages from channel
                # Use current timestamp as end time
                end_timestamp = str(time.time())
                # Get messages from the last hour
                start_timestamp = str(time.time() - 3600)
                
                messages = await self.slack_client.fetch_messages(
                    channel_id=channel,
                    start_timestamp=start_timestamp,
                    resolve_users=True,
                    total_limit=10  # Limit to recent messages
                )
                
                # Convert to LLM-compatible format
                conversation = []
                for message in messages:
                    # Skip messages from the bot itself
                    if message.get("user") == self.bot_id:
                        role = "assistant"
                    else:
                        role = "user"
                    
                    conversation.append({
                        "role": role,
                        "content": message.get("text", "")
                    })
                
                # Reverse to get chronological order
                conversation.reverse()
                return conversation
                
        except Exception as e:
            logger.error(f"Error getting conversation history: {str(e)}", exc_info=True)
            return []

    def handle_user_input(self, event_data, say, client, is_mention=False):
        """
        Common handler for both messages and mentions.
        
        Args:
            event_data: The event data (message or mention)
            say: Function to send a message
            client: Slack WebClient for API calls
            is_mention: Whether this is a mention event
            
        Returns:
            None
        """
        event_type = "mention" if is_mention else "message"
        logger.debug(f"Handling {event_type} event: {event_data}")
        
        # Skip messages from the bot itself
        user_id = event_data.get("user")
        if user_id == self.bot_id:
            logger.debug(f"Skipping {event_type} from bot itself")
            return
        
        # Get channel and thread info
        channel = event_data.get("channel")
        thread_ts = event_data.get("thread_ts", event_data.get("ts"))
        
        # Get the text of the message
        text = event_data.get("text", "")
        
        # Remove bot mention from text if present
        if self.bot_id:
            text = text.replace(f"<@{self.bot_id}>", "").strip()
            
        # Handle case where user only mentioned the bot without text
        if is_mention and not text:
            logger.info("User mentioned bot without providing text")
            say(
                text=self.MENTION_WITHOUT_TEXT,
                channel=channel,
                thread_ts=thread_ts
            )
            return
        
        # Send "Thinking..." message
        thinking_response = say(
            text=f"_{self.DEFAULT_LOADING_TEXT}_",
            channel=channel,
            thread_ts=thread_ts
        )
        
        # Get the timestamp of the "Thinking..." message
        thinking_ts = thinking_response.get("ts")
        
        # Process the input
        logger.info(f"Processing {event_type} from user {user_id}")
        logger.debug(f"Text: {text}")
        
        # Generate response using LLM
        response = asyncio.run(self.generate_response(channel, thread_ts, text, event_data))
        
        if response:
            try:
                # Update the "Thinking..." message with the actual response
                client.chat_update(
                    channel=channel,
                    ts=thinking_ts,
                    text=response.get("text", ""),
                    blocks=response.get("blocks", [])
                )
                logger.debug(f"Updated '{self.DEFAULT_LOADING_TEXT}' message with response in channel {channel}")
                logger.debug(f"Response text: {response.get('text', '')}")
                logger.debug(f"Response blocks: {response.get('blocks', [])}")
            except Exception as e:
                logger.error(f"Error updating message: {str(e)}", exc_info=True)
                # Fallback: post a new message if update fails
                say(**response)

    def handle_message_event(self, message, say, client):
        """
        Handle a message event from Slack.
        Sends a "Thinking..." message first, then updates it with the processed response.
        
        Args:
            message: The message event data from Slack
            say: Function to send a message
            client: Slack WebClient for API calls
        """
        self.handle_user_input(message, say, client, is_mention=False)
    
    def handle_mention_event(self, event, say, client):
        """
        Handle an app mention event from Slack.
        Sends a "Thinking..." message first, then updates it with the processed response.
        
        Args:
            event: The app_mention event data from Slack
            say: Function to send a message
            client: Slack WebClient for API calls
        """
        self.handle_user_input(event, say, client, is_mention=True)
    
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
    

    async def generate_response(self, channel, thread_ts, text, event_data):
        """
        Generate a response using the LLM.
        
        Args:
            channel: The channel ID
            thread_ts: The thread timestamp
            text: The text to respond to
            event_data: The original event data
            
        Returns:
            dict: Response data including text and blocks
        """
        start_time = time.time()
        
        # Get conversation history from Slack
        conversation = await self.get_conversation_history(channel, thread_ts)
        
        # Add the current message to the conversation history
        conversation.append({
            "role": "user",
            "content": text + self.SLACK_ADDITIONAL_INSTRUCTIONS
        })
        
        # Use LLM to generate response if available
        response_text = None
        custom_blocks = False
        
        if self.llm_client:
            try:
                logger.debug("Generating response using LLM...")
                
                # Create a new completion
                completion = self.llm_client.new_completion()
                
                # Add system message
                system_prompt = self.SYSTEM_PROMPT.format(bot_name=self.bot_name)
                completion.with_message(
                    system_prompt,
                    role="system"
                )
                
                # Add conversation history as separate messages
                for msg in conversation:
                    completion.with_message(
                        msg.get("content", ""),
                        role=msg.get("role")
                    )
                
                # Execute the completion
                llm_response = completion.execute()
                
                # Check if the response is successful
                if llm_response.success:
                    response_text = llm_response.text
                    logger.debug(f"LLM response: {response_text}")
                    
                    # Check if we're using a RAG model
                    _, llm_type = self.get_llm_info()
                    
                    if llm_type == "RETRIEVAL_AUGMENTED":
                        # Process as potential RAG response
                        processed_text, rag_blocks, is_rag = self.process_rag_response(response_text, text)
                        if is_rag:
                            response_text = processed_text
                            response_blocks = rag_blocks
                            custom_blocks = True
                    
                else:
                    error_msg = str(llm_response.errorMessage) if hasattr(llm_response, 'errorMessage') else "Unknown error"
                    logger.error(f"LLM returned an error: {error_msg}")
                    response_text = f"I'm sorry, I couldn't generate a response: {error_msg}"
                
            except Exception as e:
                logger.error(f"Error generating LLM response: {str(e)}", exc_info=True)
                response_text = f"I'm sorry, an error occurred while generating a response: {str(e)}"
        else:
            # Fallback to echo response if LLM is not available
            logger.warning("LLM client not available, using fallback response")
            event_type = "mention" if event_data.get("type") == "app_mention" else "message"
            prefix = "You mentioned me and said: " if event_type == "mention" else "You said: "
            response_text = f"{prefix}{text}"
        
        processing_time = time.time() - start_time
        logger.debug(f"Finished processing in {processing_time:.2f} seconds")
        
        # Format the response if not already formatted by RAG
        if not custom_blocks:
            # Standard message formatting
            response_blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"_You asked: {text.replace('_', '')}_" 
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": response_text
                    }
                }
            ]
        
        # Add the context element with processing time to all responses
        response_blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Processed in {processing_time:.2f} seconds to <@{event_data.get('user')}>'s question"
                    }
                ]
            }
        )
        
        response = {
            "text": response_text,
            "blocks": response_blocks,
            "channel": event_data.get("channel"),
            "thread_ts": event_data.get("thread_ts", event_data.get("ts"))
        }
        
        # Log the response we're returning
        logger.debug(f"Formatted response with text: {response_text[:50]}... and {len(response['blocks'])} blocks")
        
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

        # Add LLM info if available
        if self.llm_id:
            # Get LLM information
            llm_name, llm_type = self.get_llm_info()
            
            # Set header text based on LLM type
            if llm_type == "SAVED_MODEL_AGENT":
                header_text = "Using Agent:"
            elif llm_type == "RETRIEVAL_AUGMENTED":
                header_text = "Using RAG:"
            else:
                header_text = "Using LLM:"
                
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{header_text}* {llm_name} (ID: {self.llm_id})",
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
    
    ## Not used anymore
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
        
        # Get channel and thread info for conversation history
        channel = message_data.get("channel")
        thread_ts = message_data.get("thread_ts", message_data.get("ts"))
        
        # Get text and remove bot mention if present
        text = message_data.get("text", "")
        if self.bot_id:
            text = text.replace(f"<@{self.bot_id}>", "").strip()
        
        # Generate response
        return asyncio.run(self.generate_response(channel, thread_ts, text, message_data))
    
    def process_mention(self, mention_data):
        """
        Process an app mention event.
        
        Args:
            mention_data (dict): The mention event data
            
        Returns:
            dict: Response data or None
        """
        user_id = mention_data.get("user")
        
        # Skip mentions from the bot itself
        if user_id == self.bot_id:
            logger.debug("Skipping mention from bot itself")
            return None
        
        # Get channel and thread info for conversation history
        channel = mention_data.get("channel")
        thread_ts = mention_data.get("thread_ts", mention_data.get("ts"))
        
        # Get text and remove bot mention if present
        text = mention_data.get("text", "")
        if self.bot_id:
            text = text.replace(f"<@{self.bot_id}>", "").strip()
            
        # Handle case where user only mentioned the bot without text
        if not text:
            logger.info("User mentioned bot without providing text in process_mention")
            return {
                "text": self.MENTION_WITHOUT_TEXT,
                "channel": channel,
                "thread_ts": thread_ts
            }
        
        # Generate response
        return asyncio.run(self.generate_response(channel, thread_ts, text, mention_data))
    