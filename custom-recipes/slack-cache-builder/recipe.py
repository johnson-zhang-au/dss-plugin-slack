from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
import dataiku
import pandas as pd
import asyncio
from datetime import datetime
from utils.logging import logger
from slackclient.slack_client import SlackClient
import copy

def mask_sensitive_data(config):
    """
    Create a copy of the config with sensitive data masked.
    
    Args:
        config (dict): The configuration dictionary
        
    Returns:
        dict: A copy of the config with sensitive data masked
    """
    masked_config = copy.deepcopy(config)
    if 'slack_auth_settings' in masked_config:
        auth = masked_config['slack_auth_settings']
        if 'slack_token' in auth:
            token = auth['slack_token']
            if token:
                # Keep first 4 and last 4 characters, mask the rest
                masked_token = token[:4] + '*' * (len(token) - 8) + token[-4:]
                auth['slack_token'] = masked_token
    return masked_config

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack Cache Builder recipe.")
logger.debug(f"Recipe configuration: {mask_sensitive_data(config)}")

try:
    # Get output datasets
    user_cache_name = get_output_names_for_role('user_cache')[0]
    channel_cache_name = get_output_names_for_role('channel_cache')[0]
    
    user_cache_output = dataiku.Dataset(user_cache_name)
    channel_cache_output = dataiku.Dataset(channel_cache_name)
    
    logger.info(f"Output datasets configured: user_cache={user_cache_name}, channel_cache={channel_cache_name}")
    
    # Initialize Slack client
    slack_auth = config.get('slack_auth_settings')
    if not slack_auth:
        raise ValueError("Missing required configuration: slack_auth_settings")
    
    slack_client = SlackClient(slack_auth)
    
    # Fetch all channels to build channel cache
    logger.info("Fetching all channels...")
    all_channels, accessible_channels = asyncio.run(slack_client.fetch_channels(include_private_channels=True))
    
    # Build channel cache with members
    channel_cache_data = []
    for channel in accessible_channels:
        logger.info(f"Fetching members for channel {channel['name']}...")
        members = asyncio.run(slack_client._get_channel_members(channel['id']))
        channel_cache_data.append({
            'channel_name': channel['name'],
            'channel_id': channel['id'],
            'is_private': channel.get('is_private', False),
            'num_members': channel.get('num_members', 0),
            'members': ','.join(members) if members else '',  # Store as comma-separated string
            'member_count': len(members) if members else 0,
            'timestamp': datetime.now()
        })
    
    # Build user cache
    logger.info("Fetching all users...")
    try:
        response = slack_client._slack_client.users_list()
        if not response["ok"]:
            error_msg = f"Failed to fetch users: {response.get('error')}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        users = response["members"]
        logger.info(f"Successfully fetched {len(users)} users")
        
        user_cache_data = []
        for user in users:
            # Skip bots and deleted users
            if user.get("is_bot", False) or user.get("deleted", False):
                continue
            
            profile = user.get("profile", {})
            user_cache_data.append({
                'user_id': user['id'],
                'name': user.get("real_name", ""),
                'display_name': profile.get("display_name", ""),
                'email': profile.get("email", ""),
                'timestamp': datetime.now()
            })
    except Exception as e:
        error_msg = f"Error fetching users: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Write caches to output datasets
    logger.info("Writing caches to output datasets...")
    
    # Write channel cache
    channel_df = pd.DataFrame(channel_cache_data)
    channel_cache_output.write_with_schema(channel_df)
    logger.info(f"Wrote {len(channel_df)} channel cache entries")
    
    # Write user cache
    user_df = pd.DataFrame(user_cache_data)
    user_cache_output.write_with_schema(user_df)
    logger.info(f"Wrote {len(user_df)} user cache entries")
    
    logger.info("Slack Cache Builder recipe completed successfully.")

except Exception as e:
    logger.error(f"Recipe failed: {str(e)}", exc_info=True)
    raise 