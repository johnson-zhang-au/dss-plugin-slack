from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
import dataiku
import pandas as pd
import asyncio
import time
import json
from datetime import datetime
from utils.logging import logger
from dkuslackclient.dku_slack_client import DKUSlackClient
import logging

# Start timing for performance tracking
start_time = time.time()

# Get the recipe configuration
config = get_recipe_config()

# Set logging level from the configuration
logging_level = config.get('logging_level', "INFO")
logger.set_level(logging_level)

logger.info("Starting the Slack User Resolver recipe.")
logger.debug(f"Recipe configuration: {config}")

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

# Async function to resolve user IDs to user info
async def resolve_users(df, columns_to_resolve, slack_client):
    user_ids_to_resolve = set()
    
    # Process each column to be resolved
    for column in columns_to_resolve:
        if column not in df.columns:
            logger.warn(f"Column '{column}' not found in dataset, skipping")
            continue
            
        logger.info(f"Processing column: '{column}'")
        
        if column == 'reply_users' or column.endswith('_users'):
            # This is a list column, need to extract user IDs from each list
            valid_values = 0
            invalid_values = 0
            extracted_ids = 0
            
            logger.debug(f"Processing '{column}' as a list column")
            for idx, value in df[column].items():
                if pd.isna(value) or value == '':
                    invalid_values += 1
                    continue
                
                # Handle various formats of reply_users:
                # - JSON string list: '["U123", "U456"]'
                # - Literal Python string representation: "['U123', 'U456']"
                # - Already a list: ['U123', 'U456']
                try:
                    valid_values += 1
                    if isinstance(value, str):
                        # Try to parse as JSON
                        try:
                            user_list = json.loads(value)
                            logger.debug(f"Parsed JSON list at idx {idx}: {user_list}")
                        except json.JSONDecodeError:
                            # If that fails, try to eval if it looks like a Python list
                            if value.startswith('[') and value.endswith(']'):
                                # WARNING: Using eval with controlled input from database
                                user_list = eval(value)  # Convert string representation to actual list
                                logger.debug(f"Parsed Python list representation at idx {idx}: {user_list}")
                            else:
                                # Not a list, so just add the value itself if not empty
                                user_list = [value] if value else []
                                logger.debug(f"Using single value as list at idx {idx}: {user_list}")
                    else:
                        # Already a list or other iterable
                        user_list = value if hasattr(value, '__iter__') and not isinstance(value, str) else [value]
                        logger.debug(f"Using existing iterable at idx {idx}: {user_list[:5]}{'...' if len(user_list) > 5 else ''}")
                    
                    # Add all user IDs from the list
                    for user_id in user_list:
                        if user_id and user_id.strip():
                            user_ids_to_resolve.add(user_id.strip())
                            extracted_ids += 1
                except Exception as e:
                    invalid_values += 1
                    logger.warn(f"Error processing '{column}' value '{value}' at idx {idx}: {str(e)}")
            
            logger.info(f"Processed {valid_values} valid values and {invalid_values} invalid values from '{column}', extracted {extracted_ids} unique user IDs")
        else:
            # Regular column with single user ID
            logger.debug(f"Processing '{column}' as a regular column with single user IDs")
            unique_ids = df[column].dropna().unique()
            logger.debug(f"Found {len(unique_ids)} unique values in column '{column}'")
            
            for user_id in unique_ids:
                if user_id and not pd.isna(user_id) and user_id.strip():
                    user_ids_to_resolve.add(user_id.strip())
            
            logger.info(f"Added {len(unique_ids)} unique user IDs from column '{column}'")
    
    logger.info(f"Collected {len(user_ids_to_resolve)} unique user IDs to resolve")
    if logger.isEnabledFor(logging.DEBUG) and len(user_ids_to_resolve) < 20:
        logger.debug(f"User IDs to resolve: {sorted(list(user_ids_to_resolve))}")
    
    # Create a mapping of user_id to user info
    user_info_map = {}
    
    # Break the resolution into batches to avoid overwhelming the Slack API
    batch_size = 100
    user_ids_list = list(user_ids_to_resolve)
    
    total_resolved = 0
    total_failed = 0
    
    for i in range(0, len(user_ids_list), batch_size):
        batch = user_ids_list[i:i+batch_size]
        batch_end = min(i+batch_size, len(user_ids_list))
        logger.info(f"Resolving batch {i//batch_size + 1}: {len(batch)} user IDs (indices {i} to {batch_end-1})")
        
        # Create tasks for all user IDs to get user information in parallel
        tasks = [slack_client._get_user_by_id(uid) for uid in batch]
        logger.debug(f"Created {len(tasks)} async tasks for user resolution")
        
        batch_start_time = time.time()
        results = await asyncio.gather(*tasks)
        batch_duration = time.time() - batch_start_time
        
        batch_resolved = 0
        batch_failed = 0
        
        # Create a mapping from the results
        for j, uid in enumerate(batch):
            user_id, user_name, user_email = results[j]
            if user_id:  # Skip None results
                user_info_map[uid] = {
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_email": user_email
                }
                batch_resolved += 1
            else:
                batch_failed += 1
                logger.warn(f"Could not resolve user ID: '{uid}'")
        
        total_resolved += batch_resolved
        total_failed += batch_failed
        
        logger.info(f"Batch {i//batch_size + 1} completed in {batch_duration:.2f}s: {batch_resolved} resolved, {batch_failed} failed")
        logger.debug(f"Average time per user: {batch_duration/len(batch):.4f}s")
    
    logger.info(f"Successfully resolved {total_resolved} out of {len(user_ids_to_resolve)} user IDs ({total_failed} failed)")
    return user_info_map

try:
    # Get Slack authentication settings
    slack_auth = config.get("slack_auth_settings", {})
    if not slack_auth:
        logger.error("Slack authentication settings are missing or empty")
        raise ValueError("Slack authentication settings are required")
    
    slack_token = slack_auth.get("slack_token")
    if not slack_token:
        logger.error("Slack token is missing from authentication settings")
        raise ValueError("Slack token is required")
    
    logger.debug("Initializing SlackClient with authentication settings")
    # Initialize the Slack client
    slack_client = DKUSlackClient(slack_token)
    logger.debug("SlackClient initialized successfully")
    
    # Get parameters from the recipe configuration
    columns_to_resolve = config.get('columns_to_resolve', ['user', 'reply_users'])
    logger.debug(f"Selected columns to resolve: {columns_to_resolve}")
    
    # Build the resolve mapping from the individual parameters
    resolve_mapping = {}
    
    # Add user mapping if enabled
    if config.get('resolve_user', True) and 'user' in columns_to_resolve:
        user_name_column = config.get('user_name_column', 'user_name')
        user_email_column = config.get('user_email_column', 'user_email')
        resolve_mapping['user'] = [user_name_column, user_email_column]
        logger.debug(f"Adding 'user' column mapping: {user_name_column}, {user_email_column}")
    else:
        logger.debug("Skipping 'user' column mapping (disabled or not selected)")
    
    # Add reply_users mapping if enabled
    if config.get('resolve_reply_users', True) and 'reply_users' in columns_to_resolve:
        reply_users_info_column = config.get('reply_users_info_column', 'reply_users_info')
        resolve_mapping['reply_users'] = [reply_users_info_column]
        logger.debug(f"Adding 'reply_users' column mapping: {reply_users_info_column}")
    else:
        logger.debug("Skipping 'reply_users' column mapping (disabled or not selected)")
    
    # Add any other selected columns from columns_to_resolve
    for column in columns_to_resolve:
        if column not in resolve_mapping and column != 'user' and column != 'reply_users':
            # For other columns, use a default naming pattern
            target_cols = [f"{column}_name", f"{column}_email"]
            resolve_mapping[column] = target_cols
            logger.debug(f"Adding mapping for additional column '{column}': {target_cols}")
    
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
    
    # Log dataset details
    logger.info(f"Read {len(df)} messages from input dataset")
    logger.debug(f"Input dataset columns: {df.columns.tolist()}")
    logger.debug(f"First few rows sample: {df.head(2)}")
    
    # Check if we have any messages to process
    if len(df) == 0:
        logger.warn("No messages to process. Creating empty output dataset.")
        output_dataset.write_with_schema(df)
        logger.info("Recipe completed successfully with empty output.")
        exit(0)
    
    # Run the async function to resolve users
    resolution_start_time = time.time()
    user_info_map = asyncio.run(resolve_users(df, columns_to_resolve, slack_client))
    resolution_duration = time.time() - resolution_start_time
    logger.info(f"User resolution completed in {resolution_duration:.2f} seconds")
    
    # Log a sample of the resolved users
    if logger.isEnabledFor(logging.DEBUG) and user_info_map:
        sample_size = min(5, len(user_info_map))
        sample_keys = list(user_info_map.keys())[:sample_size]
        logger.debug(f"Sample of resolved users:")
        for key in sample_keys:
            logger.debug(f"  {key} -> {user_info_map[key]}")
    
    # Now apply the user info mapping to the DataFrame
    df_update_start_time = time.time()
    columns_added = []
    
    for source_col, target_cols in resolve_mapping.items():
        if source_col not in df.columns:
            logger.warn(f"Source column '{source_col}' not found in dataset, skipping")
            continue
            
        logger.info(f"Applying user info for source column '{source_col}' to target columns {target_cols}")
        
        if source_col == 'reply_users' or source_col.endswith('_users'):
            # Handle list columns - create a new column with the resolved info
            logger.debug(f"Processing '{source_col}' as a list column")
            
            temp_col_name = f"_temp_{source_col}_info"
            logger.debug(f"Creating temporary column '{temp_col_name}'")
            
            df[temp_col_name] = df[source_col].apply(
                lambda x: _process_reply_users(x, user_info_map) if not pd.isna(x) and x != '' else []
            )
            
            # Map to the target column for reply_users_info
            for target_col in target_cols:
                logger.debug(f"Copying temporary column to target column '{target_col}'")
                df[target_col] = df[temp_col_name]
                columns_added.append(target_col)
            
            # Clean up temporary column
            logger.debug(f"Dropping temporary column '{temp_col_name}'")
            df = df.drop(temp_col_name, axis=1)
        else:
            # Handle single user ID columns
            logger.debug(f"Processing '{source_col}' as a regular column")
            
            if len(target_cols) >= 1:
                name_col = target_cols[0]  # First target column is for username
                logger.debug(f"Adding username column '{name_col}'")
                df[name_col] = df[source_col].apply(
                    lambda x: user_info_map.get(x, {}).get('user_name', x) if not pd.isna(x) and x != '' else ''
                )
                columns_added.append(name_col)
                
            if len(target_cols) >= 2:
                email_col = target_cols[1]  # Second target column is for email
                logger.debug(f"Adding email column '{email_col}'")
                df[email_col] = df[source_col].apply(
                    lambda x: user_info_map.get(x, {}).get('user_email', '') if not pd.isna(x) and x != '' else ''
                )
                columns_added.append(email_col)
    
    df_update_duration = time.time() - df_update_start_time
    logger.info(f"DataFrame update completed in {df_update_duration:.2f} seconds, added {len(columns_added)} columns: {columns_added}")
    
    # Write the output dataset
    write_start_time = time.time()
    logger.info(f"Writing {len(df)} messages with resolved users to output dataset")
    output_dataset.write_with_schema(df)
    write_duration = time.time() - write_start_time
    logger.info(f"Dataset write completed in {write_duration:.2f} seconds")
    
    # Log overall performance
    total_duration = time.time() - start_time
    logger.info(f"Recipe completed successfully in {total_duration:.2f} seconds")
    logger.debug(f"Performance breakdown: Resolution: {resolution_duration:.2f}s ({resolution_duration/total_duration*100:.1f}%), " + 
                 f"DataFrame update: {df_update_duration:.2f}s ({df_update_duration/total_duration*100:.1f}%), " +
                 f"Dataset write: {write_duration:.2f}s ({write_duration/total_duration*100:.1f}%)")

except Exception as e:
    logger.error(f"Recipe failed: {str(e)}", exc_info=True)
    raise

logger.info("Slack User Resolver recipe completed.")

