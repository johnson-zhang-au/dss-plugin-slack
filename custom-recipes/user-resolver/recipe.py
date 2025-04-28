from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
import dataiku
import pandas as pd
import asyncio
import time
import json
from datetime import datetime
from utils.logging import logger
from slackchat.slack_bot import SlackChatBot

# Start timing for performance tracking
start_time = time.time()

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack User Resolver recipe.")
logger.debug(f"Recipe configuration: {config}")

try:
    # Get bot authentication settings
    slack_bot_auth = config.get("bot_auth_settings", {})
    if not slack_bot_auth:
        logger.error("Bot authentication settings are missing or empty")
        raise ValueError("Bot authentication settings are required")
    
    logger.debug("Initializing SlackChatBot")
    # Initialize the SlackChatBot
    slack_chat_bot = SlackChatBot(slack_bot_auth)
    
    # Get parameters from the recipe configuration
    columns_to_resolve = config.get('columns_to_resolve', ['user', 'reply_users'])
    
    # Build the resolve mapping from the individual parameters
    resolve_mapping = {}
    
    # Add user mapping if enabled
    if config.get('resolve_user', True) and 'user' in columns_to_resolve:
        user_name_column = config.get('user_name_column', 'user_name')
        user_email_column = config.get('user_email_column', 'user_email')
        resolve_mapping['user'] = [user_name_column, user_email_column]
    
    # Add reply_users mapping if enabled
    if config.get('resolve_reply_users', True) and 'reply_users' in columns_to_resolve:
        reply_users_info_column = config.get('reply_users_info_column', 'reply_users_info')
        resolve_mapping['reply_users'] = [reply_users_info_column]
    
    # Add any other selected columns from columns_to_resolve
    for column in columns_to_resolve:
        if column not in resolve_mapping and column != 'user' and column != 'reply_users':
            # For other columns, use a default naming pattern
            resolve_mapping[column] = [f"{column}_name", f"{column}_email"]
    
    logger.info(f"Columns to resolve: {columns_to_resolve}")
    logger.info(f"Resolution mapping: {resolve_mapping}")
    
    # Get input and output datasets
    input_name = get_input_names_for_role('input_dataset')[0]
    output_name = get_output_names_for_role('output_dataset')[0]
    
    input_dataset = dataiku.Dataset(input_name)
    output_dataset = dataiku.Dataset(output_name)
    
    # Read the input dataset
    logger.info(f"Reading messages from input dataset: {input_name}")
    df = input_dataset.get_dataframe()
    
    logger.info(f"Read {len(df)} messages from input dataset")
    
    # Check if we have any messages to process
    if len(df) == 0:
        logger.warn("No messages to process. Creating empty output dataset.")
        output_dataset.write_with_schema(df)
        logger.info("Recipe completed successfully with empty output.")
        exit(0)
    
    # Collect all unique user IDs that need to be resolved
    async def resolve_users():
        user_ids_to_resolve = set()
        
        # Process each column to be resolved
        for column in columns_to_resolve:
            if column not in df.columns:
                logger.warn(f"Column {column} not found in dataset, skipping")
                continue
                
            logger.info(f"Processing column: {column}")
            
            if column == 'reply_users' or column.endswith('_users'):
                # This is a list column, need to extract user IDs from each list
                for idx, value in df[column].items():
                    if pd.isna(value) or value == '':
                        continue
                    
                    # Handle various formats of reply_users:
                    # - JSON string list: '["U123", "U456"]'
                    # - Literal Python string representation: "['U123', 'U456']"
                    # - Already a list: ['U123', 'U456']
                    try:
                        if isinstance(value, str):
                            # Try to parse as JSON
                            try:
                                user_list = json.loads(value)
                            except json.JSONDecodeError:
                                # If that fails, try to eval if it looks like a Python list
                                if value.startswith('[') and value.endswith(']'):
                                    # WARNING: Using eval with controlled input from database
                                    user_list = eval(value)  # Convert string representation to actual list
                                else:
                                    # Not a list, so just add the value itself if not empty
                                    user_list = [value] if value else []
                        else:
                            # Already a list or other iterable
                            user_list = value if hasattr(value, '__iter__') and not isinstance(value, str) else [value]
                        
                        # Add all user IDs from the list
                        for user_id in user_list:
                            if user_id and user_id.strip():
                                user_ids_to_resolve.add(user_id.strip())
                    except Exception as e:
                        logger.warn(f"Error processing reply_users value '{value}': {str(e)}")
            else:
                # Regular column with single user ID
                for user_id in df[column].dropna().unique():
                    if user_id and not pd.isna(user_id) and user_id.strip():
                        user_ids_to_resolve.add(user_id.strip())
        
        logger.info(f"Collected {len(user_ids_to_resolve)} unique user IDs to resolve")
        
        # Create a mapping of user_id to user info
        user_info_map = {}
        
        # Break the resolution into batches to avoid overwhelming the Slack API
        batch_size = 100
        user_ids_list = list(user_ids_to_resolve)
        
        for i in range(0, len(user_ids_list), batch_size):
            batch = user_ids_list[i:i+batch_size]
            logger.info(f"Resolving batch of {len(batch)} user IDs ({i+1} to {min(i+batch_size, len(user_ids_list))})")
            
            # Create tasks for all user IDs to get user information in parallel
            tasks = [slack_chat_bot._get_user_by_id(uid) for uid in batch]
            results = await asyncio.gather(*tasks)
            
            # Create a mapping from the results
            for j, uid in enumerate(batch):
                user_id, user_name, user_email = results[j]
                if user_id:  # Skip None results
                    user_info_map[uid] = {
                        "user_id": user_id,
                        "user_name": user_name,
                        "user_email": user_email
                    }
                else:
                    logger.warn(f"Could not resolve user ID: {uid}")
        
        logger.info(f"Successfully resolved {len(user_info_map)} out of {len(user_ids_to_resolve)} user IDs")
        return user_info_map
    
    # Run the async function to resolve users
    user_info_map = asyncio.run(resolve_users())
    
    # Now apply the user info mapping to the DataFrame
    for source_col, target_cols in resolve_mapping.items():
        if source_col not in df.columns:
            logger.warn(f"Source column {source_col} not found in dataset, skipping")
            continue
            
        logger.info(f"Applying user info for source column {source_col} to target columns {target_cols}")
        
        if source_col == 'reply_users' or source_col.endswith('_users'):
            # Handle list columns - create a new column with the resolved info
            df['_temp_reply_info'] = df[source_col].apply(
                lambda x: _process_reply_users(x, user_info_map) if not pd.isna(x) and x != '' else []
            )
            
            # Map to the target column for reply_users_info
            for target_col in target_cols:
                df[target_col] = df['_temp_reply_info']
            
            # Clean up temporary column
            df = df.drop('_temp_reply_info', axis=1)
        else:
            # Handle single user ID columns
            if len(target_cols) >= 1:
                name_col = target_cols[0]  # First target column is for username
                df[name_col] = df[source_col].apply(
                    lambda x: user_info_map.get(x, {}).get('user_name', x) if not pd.isna(x) and x != '' else ''
                )
                
            if len(target_cols) >= 2:
                email_col = target_cols[1]  # Second target column is for email
                df[email_col] = df[source_col].apply(
                    lambda x: user_info_map.get(x, {}).get('user_email', '') if not pd.isna(x) and x != '' else ''
                )
    
    # Write the output dataset
    logger.info(f"Writing {len(df)} messages with resolved users to output dataset")
    output_dataset.write_with_schema(df)
    
    # Log overall performance
    total_duration = time.time() - start_time
    logger.info(f"Recipe completed successfully in {total_duration:.2f} seconds")

except Exception as e:
    logger.error(f"Recipe failed: {str(e)}", exc_info=True)
    raise

logger.info("Slack User Resolver recipe completed.")

# Helper function to process reply_users values
def _process_reply_users(value, user_info_map):
    try:
        # Convert the value to a list of user IDs
        if isinstance(value, str):
            # Try to parse as JSON
            try:
                user_list = json.loads(value)
            except json.JSONDecodeError:
                # If that fails, try to eval if it looks like a Python list
                if value.startswith('[') and value.endswith(']'):
                    # WARNING: Using eval with controlled input from database
                    user_list = eval(value)  # Convert string representation to actual list
                else:
                    # Not a list, so just use the value itself if not empty
                    user_list = [value] if value else []
        else:
            # Already a list or other iterable
            user_list = value if hasattr(value, '__iter__') and not isinstance(value, str) else [value]
        
        # Map each user ID to its info
        result = []
        for user_id in user_list:
            if user_id and user_id.strip() and user_id.strip() in user_info_map:
                result.append(user_info_map[user_id.strip()])
        
        return result
    except Exception as e:
        logger.warn(f"Error processing reply_users value '{value}': {str(e)}")
        return [] 