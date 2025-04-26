async def fetch_channels():
    """Fetch all channels the bot has access to."""
    try:
        response = await slack_client.conversations_list(types="public_channel,private_channel")
        return response.get("channels", [])
    except SlackApiError as e:
        logger.error(f"Error fetching channels: {e.response['error']}")
        return []

async def fetch_messages(channel_id, start_timestamp, user_ids):
    """Fetch messages from a specific channel."""
    try:
        messages = []
        next_cursor = None

        while True:
            response = await slack_client.conversations_history(
                channel=channel_id,
                oldest=start_timestamp,
                limit=200,
                cursor=next_cursor
            )
            for message in response.get("messages", []):
                if not user_ids or any(user_id in message.get("user", "") for user_id in user_ids):
                    messages.append(message)

            next_cursor = response.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break

        return messages
    except SlackApiError as e:
        logger.error(f"Error fetching messages from channel {channel_id}: {e.response['error']}")
        return []

async def fetch_messages_from_channels(start_timestamp, channel_ids=None, user_ids=None):
    """Fetch messages from specified channels or all channels."""
    channels = await fetch_channels()

    if channel_ids:
        channels = [channel for channel in channels if channel["id"] in channel_ids]

    tasks = []
    for channel in channels:
        tasks.append(fetch_messages(channel["id"], start_timestamp, user_ids))

    results = await asyncio.gather(*tasks)
    all_messages = [message for channel_messages in results for message in channel_messages]
    return all_messages