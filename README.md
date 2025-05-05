# Dataiku DSS Slack Plugin

This plugin provides comprehensive Slack integration for Dataiku DSS, allowing users to fetch messages, format conversations, build caches, and interact with Slack directly from DSS through both recipes and an agent tool. It also includes a web application that enables deploying AI assistants through Slack using Dataiku's LLM capabilities.

## Table of Contents

- [Recipes](#recipes)
  - [Message Fetcher](#message-fetcher)
  - [Conversation Formatter](#conversation-formatter)
  - [Slack Cache Builder](#slack-cache-builder)
  - [User Resolver](#user-resolver)
- [Visual Agent Tool](#visual-agent-tool)
- [Web Application](#web-application)
- [Authentication Settings](#authentication-settings)

## Recipes

### Message Fetcher

The **Slack Message Fetcher** recipe retrieves messages from Slack channels and saves them to a Dataiku dataset.

#### Parameters

- **Slack Authentication Settings**: Preset with Slack API credentials
- **Message Fetching Options**:
  - **Include Private Channels**: Whether to fetch messages from private channels (requires appropriate permissions)
  - **Resolve User IDs**: Convert user IDs to usernames and emails (improves data quality but increases API usage)
  - **Start Date**: Date to start fetching messages from (default: 1 day ago)
  - **Filter Options**:
    - **Filter By Channel Names**: Specify channel names to fetch from (e.g., "general,random")
    - **Filter By Channel IDs**: Specify channel IDs (alternative to names, more reliable but less user-friendly)
    - **Filter By User Emails**: Fetch messages only from channels where specified users are members
  - **Logging Level**: Detail level for logging

#### Usage Notes

- **Channel Names vs. IDs**:
  - **Channel Names** are more human-readable but might be ambiguous (e.g., renamed channels)
  - **Channel IDs** are unique identifiers that never change, making them more reliable
- **Private Channels** require the Slack app to have `groups:history` and `groups:read` permissions and be added to those channels
- **Rate Limiting**: The recipe implements backoff strategies to handle Slack API rate limits
- **User Resolution**: Adds user metadata (names, emails) to messages but increases API calls

### Conversation Formatter

The **Slack Conversation Formatter** transforms raw Slack messages into structured conversations suitable for analysis or LLM input.

#### Parameters

- **Formatting Options**:
  - **Group By**: How to organize conversations (channel, thread, date, or combinations)
  - **Time Period**: Group messages by day, week, or month when using date grouping
  - **Format Type**: Output format (JSON, text, markdown)
  - **Include Message Metadata**: Whether to include timestamps, user info, etc.
  - **Exclude Message Subtypes**: Filter out specific message types (e.g., channel_join, bot_message)
  - **Max Messages Per Group**: Limit the number of messages in each conversation group
  - **Logging Level**: Detail level for logging

#### Usage Notes

- **Conversation Grouping Logic**:
  - **By Channel**: Groups all messages from the same channel
  - **By Date**: Groups by time periods (day/week/month)
  - **Combined**: Supports combining multiple grouping criteria

### Slack Cache Builder

The **Slack Cache Builder** creates and maintains caches of Slack users and channels for faster lookups in other components.

#### Parameters

- **Slack Authentication Settings**: Preset with Slack API credentials
- **Cache TTL**: Number of hours until cache expiration (default: 24)
- **Logging Level**: Detail level for logging

#### Usage Notes

- This recipe outputs two datasets:
  - **User Cache**: Contains user IDs, names, emails, and other profile information
  - **Channel Cache**: Contains channel IDs, names, topics, and member counts
- **Why Use a Cache**:
  - Slack doesn't provide direct methods to get channel IDs by name
  - Looking up users by email requires API calls
  - The cache significantly improves performance for agent tools
- **Recommended Usage**: Schedule with a DSS Scenario to refresh periodically

### User Resolver

The **User Resolver** enriches datasets containing Slack user IDs with user information.

#### Parameters

- **Slack Authentication Settings**: Preset with Slack API credentials
- **User Resolution Options**:
  - **Columns to Resolve**: Columns containing user IDs to resolve
  - **Resolve User**: Add username and email for the 'user' column
  - **Resolve Reply Users**: Add user info for 'reply_users' column
  - **Output Column Names**: Customize the column names for resolved information
  - **Logging Level**: Detail level for logging

#### Usage Notes

- Useful for post-processing messages fetched without user resolution
- Handles both individual user IDs and arrays of user IDs (e.g., 'reply_users')
- Implements batching and rate limiting for efficient API usage

## Visual Agent Tool

The **Slack Visual Agent Tool** enables Dataiku Agents to interact with Slack workspaces directly.

### Comprehensive Action Reference

Below is a detailed reference for all available actions in the Slack Visual Agent Tool.

#### Channel Operations

| Action | Description | Required Parameters | Optional Parameters | Returns |
|--------|-------------|---------------------|---------------------|---------|
| **slack_list_channels** | List available channels in the workspace | none | `limit` (int, max 200)<br>`include_private_channels` (bool)<br>`cursor` (string) | List of channels with IDs, names, topics, member counts |
| **slack_get_channel_id_by_name** | Convert channel name to ID | `channel_name` (string) | none | Channel ID and normalized name |
| **slack_get_channel_history** | Get recent messages from a channel | `channel_id` (string) | `limit` (int, default 10)<br>`time_range` (string, e.g. "1d", "4h", "1w")<br>`format_type` ("json", "markdown", "text") | Messages with content and metadata |

#### User Operations

| Action | Description | Required Parameters | Optional Parameters | Returns |
|--------|-------------|---------------------|---------------------|---------|
| **slack_get_users** | List users in the workspace | none | `limit` (int, max 200)<br>`cursor` (string) | List of users with profiles |
| **slack_get_user_profile** | Get detailed profile for a user | `user_id` (string) | none | User profile with email, status, and other information |

#### Messaging Operations

| Action | Description | Required Parameters | Optional Parameters | Returns |
|--------|-------------|---------------------|---------------------|---------|
| **slack_post_message** | Send a message to a channel | `channel_id` (string)<br>`text` (string) | none | Message confirmation with timestamp |
| **slack_reply_to_thread** | Reply to a message thread | `channel_id` (string)<br>`thread_ts` (string)<br>`text` (string) | none | Reply confirmation with timestamp |
| **slack_add_reaction** | Add emoji reaction to a message | `channel_id` (string)<br>`timestamp` (string)<br>`reaction` (string) | none | Reaction confirmation |
| **slack_get_thread_replies** | Get all replies in a thread | `channel_id` (string)<br>`thread_ts` (string) | none | List of replies with content and metadata |
| **slack_search_messages** | Search messages by keyword | `query` (string) | `limit` (int, default 100)<br>`sort` ("score", "timestamp")<br>`sort_dir` ("asc", "desc")<br>`context_messages` (int) | Search results with message content and context |

### Detailed Parameters

#### Common Parameters

- **limit**: Maximum number of items to return (default varies by action, max 200)
- **cursor**: Pagination cursor for retrieving additional pages of results
- **format_type**: Output format, one of "json", "markdown", or "text"

#### Channel Parameters

- **channel_id**: Slack channel ID (starts with "C")
- **channel_name**: Human-readable channel name (with or without #)
- **include_private_channels**: Whether to include private channels (requires appropriate permissions: **groups:read** and **groups:history** , and onlye the private channels that the bot or the represented user is a memeber of)
- **time_range**: How far back to fetch messages (e.g., "1d" for 1 day, "40h" for 40 hours, "1w" for 1 week)

#### Message Parameters

- **text**: Message content to send
- **thread_ts**: Thread timestamp for thread-related operations
- **timestamp**: Message timestamp for reaction-related operations
- **reaction**: Emoji name without colons (e.g., "thumbsup" not ":thumbsup:")
- **query**: Search term for message searching
- **sort**: Sort order for search results ("score" or "timestamp")
- **sort_dir**: Sort direction ("asc" or "desc")
- **context_messages**: Number of messages before and after to include in search results

### Usage Examples

#### Searching for Messages and Adding a Reaction

```json
// Search for messages about a specific topic
{
   "input": {
      "action": "slack_search_messages",
      "query": "quarterly report",
      "limit": 5,
      "sort": "timestamp",
      "sort_dir": "desc"
   },
   "context": {}
}

// Add a reaction to the first message from the results
{
   "input": {
      "action": "slack_add_reaction",
      "channel_id": "C01234567",
      "timestamp": "1618324391.123456",
      "reaction": "eyes"
   },
   "context": {}
}
```

#### Getting Channel History and Replying to a Thread

```json
// Get recent messages from a channel
{
   "input": {
      "action": "slack_get_channel_history",
      "channel_id": "C01234567",
      "limit": 10,
      "time_range": "1d"
   },
   "context": {}
}

// Reply to a message thread
{
   "input": {
      "action": "slack_reply_to_thread",
      "channel_id": "C01234567",
      "thread_ts": "1618324391.123456",
      "text": "I've analyzed this and have some insights to share."
   },
   "context": {}
}
```

#### Finding a Channel by Name and Posting a Message

```json
// Get the channel ID from name
{
   "input": {
      "action": "slack_get_channel_id_by_name",
      "channel_name": "general"
   },
   "context": {}
}

// Post a message to the channel
{
   "input": {
      "action": "slack_post_message",
      "channel_id": "C01234567",
      "text": "Hello everyone! I've just completed the analysis."
   },
   "context": {}
}
```

### Important Notes

- The tool supports Dataiku's Agent framework and can be used in Prompt Studio
- Uses intelligent formatting for responses, including Slack markdown
- Implements proper error handling and rate limiting
- Search functionality (`slack_search_messages`) requires a User OAuth Token
- Channel IDs provide more reliable access than channel names
- For emoji reactions, use the name without colons (e.g., "thumbsup" not ":thumbsup:")

## Web Application

The **Slack Integration** webapp enables deploying Dataiku LLMs as Slack bots, responding to messages and mentions.

### Features

- Supports two integration modes:
  - **Socket Mode**: Real-time messaging without public endpoints
  - **HTTP Endpoint**: Webhook-based for environments where Socket Mode isn't available
- Compatible with all Dataiku DSS LLM types:
  - **Standard LLM**: For general question answering
  - **RAG (Retrieval Augmented Generation)**: With source citation
  - **Agent**: With tool use capabilities
- Automatically formats responses based on LLM type:
  - RAG responses include sources with links
  - Appropriate markdown formatting for Slack

### Configuration

- **Integration Mode**: Choose between Socket Mode or HTTP Endpoint
- **LLM Selection**: Choose any LLM configured in your DSS project
- **Authentication Settings**: Slack credentials (see [Authentication Settings](#authentication-settings))

### Usage Notes

- The integration uses threads to maintain conversation context
- The bot responds to:
  - Direct messages
  - Channel mentions (@bot)
  - App Home interactions
- Messages from other bots or the current bot itself are automatically ignored to prevent bot-to-bot interactions

### Choose Your Integration Mode: Socket Mode vs HTTP Endpoint

This plugin supports two integration methods for connecting with Slack: Socket Mode and HTTP Endpoint

| Feature | Socket Mode | HTTP Endpoint |
|---------|-------------|---------------|
| Setup complexity | Simpler - no public URL needed | Requires public URL & request verification |
| Security | Uses pre-authenticated WebSockets | Requires event signature verification | 
| Firewall considerations | Works behind firewalls | Requires publicly accessible endpoint |
| Performance | WebSocket connection maintained | New HTTP connection per event |
| Scalability | Limited by connection capacity | Better for high-volume applications |

**Socket Mode**:
- Creates outbound websocket connections from Dataiku to Slack
- Doesn't require public access to your Dataiku instance
- Simpler to set up
- Requires an App-Level Token with `connections:write` scope

**HTTP Endpoint**:
- requires the Slack app to be configured with Events API 
- Requires Slack to send HTTP requests to your Dataiku instance
- Your Dataiku server must be publicly accessible, and the webapp need to be a public webapp
- Requires proper security measures for your server
- Uses a Signing Secret to verify requests

For detailed information about Socket Mode, refer to [Slack's official Socket Mode documentation](https://api.slack.com/apis/socket-mode).

### Setup Steps

Follow these steps to create the Slack integration using the visual webapp after creating your Slack app (see [Authentication Settings](#authentication-settings) for setup instructions):

#### 1. Configure Slack App Settings:
   
   A. **For Socket Mode** (for environments where exposing a Dataiku webapp to public isn't allowed):
   - Go to [api.slack.com/apps](https://api.slack.com/apps), find your Slack app for this integration
   - In the left sidebar, click "Socket Mode"
   - Toggle "Enable Socket Mode" to ON
   - Click "Generate" to create an app-level token
   - Enter a name for the token and add the `connections:write` scope
   - Copy the App-Level Token (starts with `xapp-`)
   - With Socket Mode enabled, you do NOT need to configure the Events API URL or expose your Dataiku instance

   **Important Note for Socket Mode**: Each app-level token can only be used by one Dataiku Webapp. Using the same token across multiple webapp instances may cause issues like missing messages, as Slack will randomly distribute events among all active socket connections using that token.

   B. **For HTTP Endpoint Mode** :
   - Go to [api.slack.com/apps](https://api.slack.com/apps), find your Slack app for this integration
   - In the left sidebar, click "Basic Information"
   - Scroll down to "App Credentials" and copy the "Signing Secret"
   - Then navigate to "Event Subscriptions" in the sidebar
   - Toggle "Enable Events" to ON
   - In the "Request URL" field, enter your Dataiku instance URL with the webapp endpoint:
     ```
     https://your_dss_base_url/web-apps-backends/PROJECT-ID/WEBAPP-ID/slack/events
     ```
     - Replace `your_dss_base_url` with your DSS server address
     - Replace `PROJECT-ID` with your Dataiku project ID
     - Replace `WEBAPP-ID` with your Dataiku webapp (which will be created later) ID
     - The URL must be publicly accessible, and Slack will send a verification request
     - The Request URL must respond with a 200 OK to Slack's verification request
   - Click "Save Changes"
   - Note: When using HTTP Endpoint mode, your Dataiku server must be accessible from the internet

   **Important Security Considerations For HTTP Endpoint Mode:**
   - Making a webapp public means it can be accessed without DSS authentication
   - Ensure your Slack token security is properly managed
   - Use Slack's signing secret verification to prevent unauthorized access
   - Consider IP restrictions for additional security
   - For more information, see the [DSS documentation on public webapps](https://doc.dataiku.com/dss/13/webapps/public.html)

#### 2. Event Subscriptions for the Slack App (Required for Both Modes):
   - In the left sidebar, click "Event Subscriptions"
   - Under "Subscribe to bot events", click "Add Bot User Event" and add the following events:
   
   | Event Name | Description | Required Scope |
   |------------|-------------|---------------|
   | `app_home_opened` | User clicked into your App Home | none |
   | `app_mention` | Subscribe to only the message events that mention your app or bot | `app_mentions:read` |
   | `message.im` | A message was posted in a direct message channel | `im:history` |

   **These event subscriptions are required for your Slack bot to function properly, regardless of whether you choose Socket Mode or HTTP Endpoint Mode.**

#### 3. Adding Bots to Channels:
   - After installation, your bot will not automatically have access to all channels
   - You must explicitly invite your bot to channels using `/invite @your_bot_name`
   - For private channels, remember that your app needs `groups:history` and `groups:read` permissions
   - Without being added to a channel, your bot won't see messages or be able to respond in that channel

#### 4. Install the Slack App to Workspace:
   - After configuring the bot and permissions, scroll up to "Install App" 
   - Click "(Re)Install to xxx (your Workspace)" to add your app to your Slack workspace
   - Authorize the requested permissions when prompted
   - This is a necessary step to activate your integration
   - Note: This installation is for internal workspace use only

#### 5. Create and Configure the Visual Webapp in Dataiku:
   
   A. **Create the Webapp**:
   - In your Dataiku DSS project, click on "Webapps" in the top navigation
   - Click the "+ New Webapp" button
   - Select "Visual webapp" from the options
   - Find and select "Slack Integration" from the list of webapp types
   - Enter a name for your webapp (e.g., "Slack Assistant")
   - Click "Create" to generate the webapp

   B. **Configure the Integration**:
   - In the webapp settings page, configure the following:
     - **Slack Authentication Settings**: Select or create a Slack auth preset with your tokens
       - To create a new preset, click "Create new..." and enter:
         - Bot User OAuth Token or User OAuth Token
         - App-Level Token (for Socket Mode)
         - Signing Secret (for HTTP Endpoint mode)
     - **Integration Mode**: Choose "Socket Mode" or "HTTP Endpoint"
     - **LLM**: Select the Dataiku LLM to use for generating responses
   - Click "Save" to apply your configurations
   
   C. **Start the Backend**:
   - When you save the webapp, the backend should automatically start
   - You'll see a notification indicating the backend is starting
   - If the backend doesn't start automatically:
     - Go to the "Actions" panel on the right side of the screen
     - Click "Start backend" to manually start it
   - Once the backend is running:
     - For Socket Mode: The connection to Slack will be established immediately
     - For HTTP Endpoint: The endpoint URL becomes active (find it in webapp details)
   - In HTTP Endpoint mode, you may need to make the Slack Integration webapp accessible without DSS authentication so that the HTTP endpoint can be accessed by Slack's Event API.

   D. **Making the Webapp Public** (Only Required for HTTP Endpoint mode):             
   - Go to the DSS Administration panel
   - Navigate to Settings > Security & Audit > Other security settings
   - Find the "Authentication whitelist" section
   - Add your webapp's identifier in the format: `PROJECTKEY.webappId`
     - The `webappId` is the first 8 characters before the underscore in the webapp URL
     - For example, if the webapp URL is `/projects/MYPROJECT/webapps/kUDF1mQ_/view`, use `MYPROJECT.kUDF1mQ`
   
#### 6. **Test the Integration**:
   - In Slack, send a direct message to your bot
   - Or mention the bot in a channel it has joined
   - You should see the bot respond with a message from your configured LLM


## Authentication Settings

The plugin uses a parameter set for Slack authentication that can be defined at the instance or project level.

### Required Settings

- **Slack App OAuth Token**: You can use either a Bot User OAuth Token or a User OAuth Token:
  
  A. **Bot User OAuth Token** (starts with `xoxb-`):
    - Represents a bot user identity
    - Requires Bot Token Scopes in OAuth settings
    - Required scopes:
      - `app_mentions:read`
      - `channels:history`, `channels:read`
      - `chat:write`
      - `im:history`, `im:read`
      - `users:read`, `users:read.email`
      - For private channels: `groups:history`, `groups:read`
      - For reactions: `reactions:write`
  
  B. **User OAuth Token** (starts with `xoxp-`):
    - Represents the user who installed the app
    - Requires User Token Scopes in OAuth settings
    - Provides additional capabilities not available to bots
    - Required scopes:
      - `channels:history`, `channels:read`
      - `chat:write`
      - `groups:history`, `groups:read`
      - `search:read` (if using search functionality)
      - `users:read`, `users:read.email`

- **Slack Message Signing Secret**: For verifying HTTP endpoint requests
  - Required only for HTTP Endpoint mode
  - Found in the "Basic Information" section of your Slack App

- **Slack App App-level Token**: For Socket Mode connections
  - Required only for Socket Mode
  - Must start with `xapp-`
  - Requires the `connections:write` scope

### Bot Token vs. User Token

The choice between using a Bot User OAuth Token or a User OAuth Token significantly affects how the integration works:

#### Bot User OAuth Token

- **Channel Access**: 
  - Bot must be explicitly invited to public and private channels using `/invite @bot_name`
  - Can only access channels it has been invited to
  - Must be a member of a channel to see messages and reply in that channel
  
- **Identity**: 
  - Messages and actions are performed as the bot
  - Bot icon and name appear with messages
  - Clearly identified as an automated system

- **Permissions**: 
  - Limited to bot-specific scopes
  - Some API methods are restricted (like `search.messages`)
  - Cannot act on behalf of users

- **Best for**:
  - Dedicated assistant bots
  - When you want a distinct bot identity
  - Cases where it's acceptable to explicitly add the bot to channels

#### User OAuth Token

- **Channel Access**:
  - Automatically has access to all public channels
  - Can access any private channel the authorizing user is a member of
  - No need to invite to channels separately
  
- **Identity**:
  - Actions are performed on behalf of the authorizing user
  - Messages appear as coming from the user (unless customized)
  - May be confusing to other team members

- **Permissions**:
  - Can use additional functionality like `search.messages`
  - Access matches what the authorizing user can see and do
  - Can execute user-specific actions

- **Best for**:
  - Building personal assistants
  - When broad channel access is needed
  - When search functionality is required
  - Advanced integrations requiring user-level permissions

#### Practical Differences

| Feature | Bot User OAuth Token | User OAuth Token |
|---------|---------------------|------------------|
| Channel access | Only invited channels | All public + user's private channels |
| Installation | Workspace install | Requires user authorization |
| Search capability | Not available | Available with `search:read` scope |
| Identity in messages | Bot identity | User identity (customizable) |
| Permission model | Limited to bot scopes | Can use user scopes |
| Token starts with | `xoxb-` | `xoxp-` |

#### Recommendation

- Use **Bot User OAuth Token** for dedicated assistants, formal integrations, and when a distinct bot identity is preferred
- Use **User OAuth Token** when you need search capabilities, broader channel access, or when building a personal assistant for a specific user

### How to Obtain Credentials

1. **Create a Slack App**:
   - Go to [api.slack.com/apps](https://api.slack.com/apps)
   - Click "Create New App" and choose either:
     - **From scratch**: To manually configure all settings
     - **From a manifest**: To quickly set up using a pre-configured YAML file (see sample manifests below)
   - Enter a name and select your workspace
   - Click "Create App"

   **Sample Manifests for Different Use Cases:**

   **Option 1: Slack App for the Visual Webapp (Socket Mode)**
   ```yaml
   display_information:
     name: <The name of your Slack app>
     description: <A short description of your Slack app>
     background_color: "#121317"
   features:
     app_home:
       home_tab_enabled: true
       messages_tab_enabled: true
       messages_tab_read_only_enabled: false
     bot_user:
       display_name: <Your bot's display name>
       always_online: true
   oauth_config:
     scopes:
       bot:
         - app_mentions:read
         - channels:history
         - channels:read
         - chat:write
         - im:history
         - reactions:write
         - users:read
         - users:read.email
         - groups:read
         - groups:history
   settings:
     event_subscriptions:
       bot_events:
         - app_home_opened
         - app_mention
         - message.im
     org_deploy_enabled: false
     socket_mode_enabled: true
     token_rotation_enabled: false
   ```
   **Note**: When using this manifest, you'll still need to generate an app-level token by going to App Home → Basic Information → App-Level Tokens.

   **Option 2: Slack App for the Visual Webapp (HTTP Endpoint Mode)**
   ```yaml
   display_information:
     name: <The name of your Slack app>
     description: <A short description of your Slack app>
     background_color: "#121317"
   features:
     app_home:
       home_tab_enabled: true
       messages_tab_enabled: true
       messages_tab_read_only_enabled: false
     bot_user:
       display_name: <Your bot's display name>
       always_online: true
   oauth_config:
     scopes:
       bot:
         - app_mentions:read
         - channels:history
         - channels:read
         - chat:write
         - im:history
         - reactions:write
         - users:read
         - users:read.email
         - groups:read
         - groups:history
   settings:
     event_subscriptions:
       request_url: <Your Dataiku webapp URL for receiving events>
       bot_events:
         - app_mention
         - message.im
     org_deploy_enabled: false
     socket_mode_enabled: false
     token_rotation_enabled: false
   ```
   **Note**: Replace the `request_url` with your Dataiku webapp URL in the format: `https://your_dss_base_url/web-apps-backends/PROJECT-ID/WEBAPP-ID/slack/events`

   **Option 3: Slack App for the AI Agent Tool (With Search Capability)**
   ```yaml
   display_information:
     name: <The name of your Slack app>
     description: <A short description of your Slack app>
     background_color: "#121317"
   oauth_config:
     scopes:
       user:
         - channels:history
         - channels:read
         - chat:write
         - groups:history
         - groups:read
         - team:read
         - users:read
         - users:read.email
         - search:read
   settings:
     org_deploy_enabled: false
     socket_mode_enabled: false
     token_rotation_enabled: false
   ```
   **Note**: This manifest uses user token scopes (including search:read) for full search capability.

   **Option 4: Slack App for Recipes or AI Agent Tool (Without Search)**
   ```yaml
   display_information:
     name: <The name of your Slack app>
     description: <A short description of your Slack app>
     background_color: "#121317"
   features:
     app_home:
       home_tab_enabled: false
       messages_tab_enabled: true
       messages_tab_read_only_enabled: false
     bot_user:
       display_name: <Your bot's display name>
       always_online: true
   oauth_config:
     scopes:
       bot:
         - app_mentions:read
         - channels:history
         - channels:read
         - chat:write
         - im:history
         - reactions:write
         - users:read
         - users:read.email
         - groups:history
         - groups:read
   settings:
     org_deploy_enabled: false
     socket_mode_enabled: false
     token_rotation_enabled: false
   ```

2. **Configure Bot User** (if using Bot Token):
   - In the left sidebar, under "Features", click "App Home"
   - Scroll down to "App Display Name" and set your bot's name
   - Toggle on "Always Show My Bot as Online"
   - Under "Show Tabs", enable the "Messages" tab

3. **Set Up OAuth & Permissions**:
   - In the left sidebar, click "OAuth & Permissions"
   
   A. **For Bot User OAuth Token**:
      - Under "Scopes", add the required Bot Token Scopes (listed above)
      - Scroll up to "OAuth Tokens" and click "Install to xxx (your Workspace)"
      - Authorize the app and copy the "Bot User OAuth Token" (starts with `xoxb-`)
   
   B. **For User OAuth Token**:
      - Under "Scopes", add the required User Token Scopes (listed above)
      - Scroll up to "OAuth Tokens" and click "Install to xxx (your Workspace)"
      - Authorize the app and copy the "User OAuth Token" (starts with `xoxp-`)
      - Note: This token represents the user who installed the app

### Additional Notes

- **Adding to Channels**:
  - You must invite your bot to channels it needs to access using `/invite @your_bot_name`
  - Private channels require additional permissions (`groups:history` and `groups:read`)

### Security Considerations

- Store tokens securely using Dataiku's parameter sets
- Consider using IP restrictions in your Slack App settings (under "OAuth & Permissions" > "Restrict API Token Usage")
- For HTTP Endpoint mode:
  - Ensure your DSS instance is properly secured with HTTPS
  - Consider additional network security measures like API gateways or firewalls
  - Regularly audit your server logs for unexpected requests 