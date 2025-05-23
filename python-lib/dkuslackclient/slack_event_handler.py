from utils.logging import logger
import time
import dataiku
import asyncio
from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode
import re

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
    DEFAULT_SYSTEM_PROMPT = """You are a versatile AI assistant. Your name is {bot_name}.
Help users with writing, coding, task management, advice, project management, and any other needs.
Provide concise, relevant assistance tailored to each request.
Note that context is sent in order of the most recent message last.
Do not respond to messages in the context, as they have already been answered.
Be professional and friendly.
Don't ask for clarification unless absolutely necessary.
Don't ask questions in your response.
Don't use user names in your response.
Respond using Slack markdown.
"""
    SLACK_ADDITIONAL_INSTRUCTIONS = ""
    
    # Default constants for conversation history
    DEFAULT_CONVERSATION_HISTORY_SECONDS = 2592000  # 30 days in seconds (1 month)
    DEFAULT_CONVERSATION_CONTEXT_LIMIT = 10  # Default number of messages to fetch
    
    def __init__(self, bot_id=None, bot_name=None, slack_client=None, settings=None):
        """
        Initialize the SlackEventHandler.
        
        Args:
            bot_id: The bot's user ID (optional)
            bot_name: The bot's name (optional)
            slack_client: SlackClient instance for API calls
            settings: Required dictionary of settings that configure behavior.
                      Must contain 'llm_id' and may contain other configuration
                      parameters like 'conversation_context_limit', 'conversation_history_seconds', 
                      and 'custom_system_prompt'.
        """
        logger.debug("Initializing SlackEventHandler...")
        self.bot_id = bot_id
        self.bot_name = bot_name
        self.slack_client = slack_client
        self.settings = settings or {}
        self.tools = []
        
        # LLM info cache
        self._llm_name = None
        self._llm_type = None
        
        # Get LLM ID from settings
        self.llm_id = self.settings.get('llm_id')
        
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

    def convert_to_slack_markdown(self, markdown_text):
        """
        Convert standard markdown to Slack markdown format.
        
        Args:
            text: The text containing markdown to convert
            
        Returns:
            tuple: (converted_text, list_of_image_blocks)
        """
        md = MarkdownIt()
        tree = SyntaxTreeNode(md.parse(markdown_text))
        image_blocks = []

        def node_to_slack(node):
            logger.debug(f"Processing node type: {node.type}")
            
            if node.type == 'text':
                logger.debug(f"Text node content: {node.content}")
                return node.content
            elif node.type == 'strong':
                content = ''.join(node_to_slack(child) for child in node.children)
                logger.debug(f"Strong node content: {content}")
                return f"*{content}*"
            elif node.type == 'em':
                content = ''.join(node_to_slack(child) for child in node.children)
                logger.debug(f"Em node content: {content}")
                return f"_{content}_"
            elif node.type == 's':
                content = ''.join(node_to_slack(child) for child in node.children)
                logger.debug(f"Strikethrough node content: {content}")
                return f"~{content}~"
            elif node.type == 'link':
                href = node.attrs.get('href', '')
                text = ''.join(node_to_slack(child) for child in node.children)
                logger.debug(f"Link node - href: {href}, text: {text}")
                
                # Check if the link is an image URL
                image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
                if any(href.lower().endswith(ext) for ext in image_extensions):
                    image_block = {
                        "type": "image",
                        "image_url": href,
                        "alt_text": text if text != href else "Image from message"
                    }
                    image_blocks.append(image_block)
                    return ""  # Return empty string as we'll handle the image separately
                
                return f"<{href}|{text}>"
            elif node.type == 'code_inline':
                logger.debug(f"Code inline node content: {node.content}")
                return f"`{node.content}`"
            elif node.type == 'code_block' or node.type == 'fence':
                logger.debug(f"Code block/fence node content: {node.content}")
                return f"```{node.content}```"
            elif node.type == 'blockquote':
                lines = ''.join(node_to_slack(child) for child in node.children).splitlines()
                logger.debug(f"Blockquote node lines: {lines}")
                return '\n'.join([f"> {line}" for line in lines])
            elif node.type == 'paragraph':
                content = ''.join(node_to_slack(child) for child in node.children)
                logger.debug(f"Paragraph node content: {content}")
                return content + "\n"
            elif node.type == 'bullet_list':
                content = '\n'.join(node_to_slack(child) for child in node.children)
                logger.debug(f"Bullet list node content: {content}")
                return content + "\n"
            elif node.type == 'list_item':
                content = ''.join(node_to_slack(child) for child in node.children)
                logger.debug(f"List item node content: {content}")
                return f"- {content}"
            elif node.type == 'image':
                src = node.attrs.get('src', '')
                alt = node.attrs.get('alt', '')
                logger.debug(f"Image node - src: {src}, alt: {alt}")
                
                # Create image block
                image_block = {
                    "type": "image",
                    "image_url": src,
                    "alt_text": alt if alt else "Image from message"
                }
                image_blocks.append(image_block)
                return ""  # Return empty string as we'll handle the image separately
            else:
                content = ''.join(node_to_slack(child) for child in node.children or [])
                logger.debug(f"Other node type '{node.type}' content: {content}")
                return content

        slack_text = ''.join(node_to_slack(child) for child in tree.children)
        logger.debug(f"Final converted text: {slack_text}")
        return slack_text.strip(), image_blocks

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

    async def get_conversation_history(self, channel, thread_ts=None, conversation_context_limit=None, conversation_history_seconds=None):
        """
        Get conversation history from Slack using the SlackClient.
        
        Args:
            channel: The channel ID
            thread_ts: The thread timestamp (optional)
            conversation_context_limit: Maximum number of messages to fetch (default: DEFAULT_CONVERSATION_CONTEXT_LIMIT)
            conversation_history_seconds: Maximum history period in seconds (default: DEFAULT_CONVERSATION_HISTORY_SECONDS)
            
        Returns:
            list: The conversation history formatted for LLM
        """
        # Use default constants if parameters are not provided
        if conversation_context_limit is None:
            conversation_context_limit = self.DEFAULT_CONVERSATION_CONTEXT_LIMIT
            
        if conversation_history_seconds is None:
            conversation_history_seconds = self.DEFAULT_CONVERSATION_HISTORY_SECONDS
            
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
                # Get messages from the specified history period
                start_timestamp = str(time.time() - conversation_history_seconds)
                
                logger.debug(f"Fetching messages from {start_timestamp} to {end_timestamp} with limit {conversation_context_limit}")
                
                messages = await self.slack_client.fetch_messages(
                    channel_id=channel,
                    start_timestamp=start_timestamp,
                    resolve_users=True,
                    total_limit=conversation_context_limit
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
            
        # Skip messages from any bot
        if event_data.get("bot_id") is not None:
            logger.debug(f"Skipping {event_type} from another bot")
            return
        
        # Get channel and thread info
        channel = event_data.get("channel")
        
        # Check if the message is from a bot (not our bot)
        is_from_bot = event_data.get("bot_id") is not None and event_data.get("bot_id") != self.bot_id
        
        # For bot messages, reply in the channel directly, not in a thread
        thread_ts = None if is_from_bot else event_data.get("thread_ts", event_data.get("ts"))
        
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
                # Add channel and thread_ts to the response for sending
                fallback_response = response.copy()
                fallback_response["channel"] = channel
                fallback_response["thread_ts"] = thread_ts
                say(**fallback_response)

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
        
        # Get conversation context limit from settings (how many messages to include)
        conversation_context_limit = self.settings.get('conversation_context_limit', self.DEFAULT_CONVERSATION_CONTEXT_LIMIT)
        
        # Get conversation history period from settings (how far back to look)
        conversation_history_seconds = self.settings.get('conversation_history_seconds', self.DEFAULT_CONVERSATION_HISTORY_SECONDS)
        
        # Get conversation history from Slack
        conversation = await self.get_conversation_history(
            channel, 
            thread_ts, 
            conversation_context_limit=conversation_context_limit,
            conversation_history_seconds=conversation_history_seconds
        )
        
        # Add the current message to the conversation history
        conversation.append({
            "role": "user",
            "content": text + self.SLACK_ADDITIONAL_INSTRUCTIONS
        })
        
        # Use LLM to generate response if available
        response_text = None
        custom_blocks = False
        response_blocks = []
        
        if self.llm_client:
            try:
                logger.debug("Generating response using LLM...")
                
                # Create a new completion
                completion = self.llm_client.new_completion()
                _, llm_type = self.get_llm_info()
                # Only add system message for standard models
                #if llm_type != "SAVED_MODEL_AGENT" and llm_type != "RETRIEVAL_AUGMENTED":
                if True:
                    # Get system prompt from settings or use default
                    use_custom_system_prompt = self.settings.get('use_custom_system_prompt', False)
                    logger.info(f"the use_custom_system_prompt is {use_custom_system_prompt}")
                    
                    if use_custom_system_prompt and self.settings.get('custom_system_prompt'):
                        system_prompt = self.settings.get('custom_system_prompt')
                    else:
                        system_prompt = self.DEFAULT_SYSTEM_PROMPT
                        
                    system_prompt = system_prompt.format(bot_name=self.bot_name)
                    
                    # Get user profile information
                    user_id = event_data.get("user")
                    if user_id:
                        try:
                            # Get user info from Slack
                            user_info = await self.slack_client._get_user_by_id(user_id)
                            if user_info:
                                _, _, user_email = user_info
                                if user_email:
                                    # Add user profile to system prompt
                                    user_profile = {
                                        "email": user_email
                                    }
                                    system_prompt += f"\n\nThe user profile is the following: # USER PROFILE: {user_profile} # --- END OF USER PROFILE --- Consider information provided in the USER PROFILE if meaningful, take it into account."
                                    logger.debug(f"Added user profile to system prompt: {user_profile}")
                        except Exception as e:
                            logger.error(f"Error getting user profile: {str(e)}", exc_info=True)
                    
                    logger.debug(f"the formatted additional system prompt is: {system_prompt}")
                    
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
                    
                    # Convert response to Slack markdown format and get image blocks
                    response_text, image_blocks = self.convert_to_slack_markdown(response_text)
                    
                    # Check if we're using a RAG model
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
            # Check if the message is from a bot (not the current bot)
            is_from_bot = event_data.get("bot_id") is not None and event_data.get("bot_id") != self.bot_id
            
            # Standard message formatting
            response_blocks = []
            
            # Only include "You asked" section if it's not from a bot
            if not is_from_bot:
                response_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"_You asked: {text.replace('_', '')}_" 
                    }
                })
            
            # Add the response text section
            response_blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": response_text
                }
            })
            
            # Add any image blocks found during markdown conversion
            if 'image_blocks' in locals():
                response_blocks.extend(image_blocks)
        
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
            "blocks": response_blocks
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
    