"""
Miner Manager Core Module

Handles miner state management, registration/deregistration,
miner information updates, and miner table operations.
"""

import asyncio
import time
import traceback
from typing import Dict, Set, Optional, List
from datetime import datetime, timezone
from collections import defaultdict

from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class MinerManager:
    """
    Handles miner state management, registration/deregistration,
    and miner table operations for the validator.
    """
    
    def __init__(self, database_manager):
        """Initialize the miner manager."""
        self.database_manager = database_manager
        self.nodes = {}  # UID -> node info mapping
        self.miner_table_lock = asyncio.Lock()
        
    async def cleanup_stale_history_on_startup(self, metagraph, fetch_nodes_func):
        """
        Compares historical prediction table hotkeys against the current metagraph
        and cleans up data for UIDs where hotkeys have changed or the UID is gone.
        """
        logger.info("Starting cleanup of stale miner history based on current metagraph...")
        try:
            if not metagraph:
                logger.warning("Metagraph not initialized, cannot perform stale history cleanup.")
                return
            if not self.database_manager:
                logger.warning("Database manager not initialized, cannot perform stale history cleanup.")
                return

            # Fetch current metagraph state
            logger.info("Syncing metagraph for stale history check...")
            
            try:
                active_nodes_list = await fetch_nodes_func(metagraph.netuid)
                if active_nodes_list is None:
                    active_nodes_list = []
                    logger.warning("Node fetching returned None, proceeding with empty list for stale history check.")
            except Exception as e_fetch_nodes:
                logger.error(f"Failed to fetch nodes for stale history check: {e_fetch_nodes}", exc_info=True)
                active_nodes_list = []

            # Build current_nodes_info mapping node_id (UID) to Node object
            current_nodes_info = {node.node_id: node for node in active_nodes_list}
            logger.info(f"Built current_nodes_info with {len(current_nodes_info)} active UIDs for stale history check.")

            # Fetch historical data (Distinct uid, miner_hotkey pairs)
            geo_history_query = "SELECT DISTINCT miner_uid, miner_hotkey FROM geomagnetic_history WHERE miner_hotkey IS NOT NULL;"
            soil_history_query = "SELECT DISTINCT miner_uid, miner_hotkey FROM soil_moisture_history WHERE miner_hotkey IS NOT NULL;"
            
            all_historical_pairs = set()
            try:
                geo_results = await self.database_manager.fetch_all(geo_history_query)
                all_historical_pairs.update((row['miner_uid'], row['miner_hotkey']) for row in geo_results)
                logger.info(f"Found {len(geo_results)} distinct (miner_uid, hotkey) pairs in geomagnetic_history.")
            except Exception as e:
                logger.warning(f"Could not query geomagnetic_history (may not exist yet): {e}")

            try:
                soil_results = await self.database_manager.fetch_all(soil_history_query)
                all_historical_pairs.update((row['miner_uid'], row['miner_hotkey']) for row in soil_results)
                logger.info(f"Found {len(soil_results)} distinct (miner_uid, hotkey) pairs in soil_moisture_history.")
            except Exception as e:
                logger.warning(f"Could not query soil_moisture_history (may not exist yet): {e}")

            if not all_historical_pairs:
                logger.info("No historical data found to check. Skipping stale history cleanup.")
                return

            logger.info(f"Found {len(all_historical_pairs)} total distinct historical (miner_uid, hotkey) pairs to check.")

            # Identify mismatches
            uids_to_cleanup = defaultdict(set)  # uid -> {stale_historical_hotkey1, ...}
            
            for hist_uid_str, hist_hotkey in all_historical_pairs:
                try:
                    hist_uid = int(hist_uid_str)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert historical UID '{hist_uid_str}' to int. Skipping.")
                    continue
                    
                current_node = current_nodes_info.get(hist_uid)
                if current_node is None:
                     logger.warning(f"Found historical entry for miner_uid {hist_uid} (Hotkey: {hist_hotkey}), but node is unexpectedly None in current metagraph sync. Skipping direct cleanup based on this.")
                     continue

                if current_node.hotkey != hist_hotkey:
                    logger.warning(f"Mismatch found: miner_uid {hist_uid} historical hotkey {hist_hotkey} != current metagraph hotkey {current_node.hotkey}. Marking for cleanup.")
                    uids_to_cleanup[hist_uid].add(hist_hotkey)

            if not uids_to_cleanup:
                logger.info("No stale historical entries found requiring cleanup.")
                return

            logger.info(f"Identified {len(uids_to_cleanup)} UIDs with stale history/scores.")

            # Perform cleanup
            distinct_task_names_rows = await self.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
            all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
            tasks_for_score_cleanup = [
                name for name in all_task_names_in_scores 
                if name == 'geomagnetic' or name.startswith('soil_moisture')
            ]
            if not tasks_for_score_cleanup:
                 logger.warning("No relevant task names (geomagnetic, soil_moisture*) found in score_table for cleanup.")

            async with self.miner_table_lock:
                for uid, stale_hotkeys in uids_to_cleanup.items():
                    logger.info(f"Cleaning up miner_uid {uid} associated with stale historical hotkeys: {stale_hotkeys}")
                    current_node = current_nodes_info.get(uid)

                    # Process each stale hotkey individually
                    for stale_hk in stale_hotkeys:
                        logger.info(f"Processing stale hotkey {stale_hk} for miner_uid {uid}.")
                        
                        # Determine time window for the stale hotkey
                        min_ts: Optional[datetime] = None
                        max_ts: Optional[datetime] = None
                        timestamps_found = False
                        
                        history_tables_and_ts_cols = {
                            "geomagnetic_history": "scored_at",
                            "soil_moisture_history": "scored_at"
                        }
                        
                        all_min_ts = []
                        all_max_ts = []
                        
                        for table, ts_col in history_tables_and_ts_cols.items():
                            try:
                                ts_query = f"""
                                    SELECT MIN({ts_col}) as min_ts, MAX({ts_col}) as max_ts 
                                    FROM {table} 
                                    WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk
                                """
                                result = await self.database_manager.fetch_one(ts_query, {"uid_str": str(uid), "stale_hk": stale_hk})
                                
                                if result and result['min_ts'] is not None and result['max_ts'] is not None:
                                    all_min_ts.append(result['min_ts'])
                                    all_max_ts.append(result['max_ts'])
                                    timestamps_found = True
                                    logger.info(f"  Found time range in {table} for ({uid}, {stale_hk}): {result['min_ts']} -> {result['max_ts']}")
                                    
                            except Exception as e_ts:
                                logger.warning(f"Could not query timestamps from {table} for miner_uid {uid}, Hotkey {stale_hk}: {e_ts}")
                        
                        # Determine overall min/max across tables
                        if all_min_ts:
                             min_ts = min(all_min_ts)
                        if all_max_ts:
                             max_ts = max(all_max_ts)

                        # Delete historical predictions
                        logger.info(f"  Deleting history entries for ({uid}, {stale_hk})")
                        try:
                            await self.database_manager.execute(
                                "DELETE FROM geomagnetic_history WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk",
                                {"uid_str": str(uid), "stale_hk": stale_hk}
                            )
                        except Exception as e_del_geo:
                             logger.warning(f"  Could not delete from geomagnetic_history for miner_uid {uid}, Hotkey {stale_hk}: {e_del_geo}")
                        try:
                            await self.database_manager.execute(
                                "DELETE FROM soil_moisture_history WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk",
                                {"uid_str": str(uid), "stale_hk": stale_hk}
                            )
                        except Exception as e_del_soil:
                            logger.warning(f"  Could not delete from soil_moisture_history for miner_uid {uid}, Hotkey {stale_hk}: {e_del_soil}")
                        
                        # Zero out scores in score_table for the determined time window
                        if tasks_for_score_cleanup and timestamps_found and min_ts and max_ts:
                            logger.info(f"  Zeroing scores for miner_uid {uid} in tasks {tasks_for_score_cleanup} within window {min_ts} -> {max_ts}")
                            await self.database_manager.remove_miner_from_score_tables(
                                uids=[uid],
                                task_names=tasks_for_score_cleanup,
                                filter_start_time=min_ts,
                                filter_end_time=max_ts
                            )
                        elif not timestamps_found:
                             logger.warning(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} - could not determine time window from history tables.")

                    # Update node_table (done once per UID after processing all its stale hotkeys)
                    current_node_for_update = current_nodes_info.get(uid)
                    if current_node_for_update:
                        logger.info(f"Updating node_table info for miner_uid {uid} to match current metagraph hotkey {current_node_for_update.hotkey}.")
                        try:
                            await self.database_manager.update_miner_info(
                                index=uid, hotkey=current_node_for_update.hotkey, coldkey=current_node_for_update.coldkey,
                                ip=current_node_for_update.ip, ip_type=str(current_node_for_update.ip_type), port=current_node_for_update.port,
                                incentive=float(current_node_for_update.incentive), stake=float(current_node_for_update.stake),
                                trust=float(current_node_for_update.trust), vtrust=float(current_node_for_update.vtrust),
                                protocol=str(current_node_for_update.protocol)
                            )
                        except Exception as e_update:
                             logger.error(f"Failed to update node_table for miner_uid {uid}: {e_update}")

            logger.info("Completed cleanup of stale miner history.")

        except Exception as e:
            logger.error(f"Error during stale history cleanup: {e}")
            logger.error(traceback.format_exc())

    async def handle_miner_deregistration_loop(self, sync_metagraph_func, fetch_nodes_func):
        """
        Handle miner state synchronization loop (hotkey changes, new miners, info updates).
        """
        logger.info("Starting miner state synchronization loop")
        
        while True:
            processed_uids = set()
            try:
                logger.info("Performing miner hotkey change check and info update...")
                
                # Get current UIDs and hotkeys from the chain's metagraph
                try:
                    active_nodes_list = await fetch_nodes_func()
                    if active_nodes_list is None:
                        active_nodes_list = []
                        logger.warning("Node fetching returned None in handle_miner_deregistration_loop.")
                except Exception as e_fetch_nodes_dereg:
                    logger.error(f"Failed to fetch nodes in handle_miner_deregistration_loop: {e_fetch_nodes_dereg}", exc_info=True)
                    active_nodes_list = []
                
                # Build chain_nodes_info mapping node_id (UID) to Node object
                chain_nodes_info = {node.node_id: node for node in active_nodes_list}

                # Get UIDs and hotkeys from our local database
                db_miner_query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL;"
                db_miners_rows = await self.database_manager.fetch_all(db_miner_query)
                db_miners_info = {row["uid"]: row["hotkey"] for row in db_miners_rows}

                uids_to_clear_and_update = {}  # uid: new_chain_node
                uids_to_update_info = {}  # uid: new_chain_node

                # Check existing DB miners against the chain
                for db_uid, db_hotkey in db_miners_info.items():
                    processed_uids.add(db_uid)
                    chain_node_for_uid = chain_nodes_info.get(db_uid)

                    if chain_node_for_uid is None:
                        logger.warning(f"UID {db_uid} (DB hotkey: {db_hotkey}) not found in current metagraph sync. Potential deregistration missed? Skipping.")
                        continue 
                        
                    if chain_node_for_uid.hotkey != db_hotkey:
                        logger.info(f"UID {db_uid} hotkey changed. DB: {db_hotkey}, Chain: {chain_node_for_uid.hotkey}. Marking for data cleanup and update.")
                        uids_to_clear_and_update[db_uid] = chain_node_for_uid
                    else:
                        uids_to_update_info[db_uid] = chain_node_for_uid

                # Process UIDs with changed hotkeys
                if uids_to_clear_and_update:
                    await self._process_hotkey_changes(uids_to_clear_and_update, db_miners_info)

                # Update info for existing miners where hotkey didn't change
                if uids_to_update_info:
                    await self._update_existing_miner_info(uids_to_update_info, uids_to_clear_and_update)

                # Handle new miners
                await self._handle_new_miners(chain_nodes_info, processed_uids)

                logger.info("Miner state synchronization cycle completed.")

            except asyncio.CancelledError:
                logger.info("Miner state synchronization loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in miner state synchronization loop: {e}", exc_info=True)
            
            await asyncio.sleep(600)  # Run every 10 minutes

    async def _process_hotkey_changes(self, uids_to_clear_and_update: Dict, db_miners_info: Dict):
        """Process UIDs with changed hotkeys."""
        logger.info(f"Cleaning old data and updating hotkeys for UIDs: {list(uids_to_clear_and_update.keys())}")
        
        async with self.miner_table_lock:
            for uid_to_process, new_chain_node_data in uids_to_clear_and_update.items():
                original_hotkey = db_miners_info.get(uid_to_process)
                if not original_hotkey:
                    logger.warning(f"Could not find original hotkey in DB cache for UID {uid_to_process}. Skipping cleanup for this UID.")
                    continue
                    
                logger.info(f"Processing hotkey change for UID {uid_to_process}: Old={original_hotkey}, New={new_chain_node_data.hotkey}")
                try:
                    # Delete from prediction tables by UID
                    prediction_tables_by_uid = ["geomagnetic_predictions", "soil_moisture_predictions"]
                    for table_name in prediction_tables_by_uid:
                        try:
                            table_exists_res = await self.database_manager.fetch_one(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                            if table_exists_res and table_exists_res['exists']:
                                await self.database_manager.execute(f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", {"hotkey": original_hotkey})
                                logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                        except Exception as e_pred_del:
                            logger.warning(f"  Could not clear {table_name} for UID {uid_to_process}: {e_pred_del}")

                    # Delete from history tables by OLD hotkey
                    history_tables_by_hotkey = ["geomagnetic_history", "soil_moisture_history"]
                    for table_name in history_tables_by_hotkey:
                        try:
                            table_exists_res = await self.database_manager.fetch_one(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                            if table_exists_res and table_exists_res['exists']:
                                await self.database_manager.execute(f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", {"hotkey": original_hotkey})
                                logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                        except Exception as e_hist_del:
                             logger.warning(f"  Could not clear {table_name} for old hotkey {original_hotkey}: {e_hist_del}")

                    # Zero out ALL scores for the UID in score_table
                    distinct_task_names_rows = await self.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
                    all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
                    if all_task_names_in_scores:
                        logger.info(f"  Zeroing all scores in score_table for UID {uid_to_process} across tasks: {all_task_names_in_scores}")
                        await self.database_manager.remove_miner_from_score_tables(
                            uids=[uid_to_process],
                            task_names=all_task_names_in_scores,
                            filter_start_time=None, filter_end_time=None
                        )

                    # Update node_table with NEW info
                    logger.info(f"  Updating node_table info for UID {uid_to_process} with new hotkey {new_chain_node_data.hotkey}.")
                    await self.database_manager.update_miner_info(
                        index=uid_to_process, hotkey=new_chain_node_data.hotkey, coldkey=new_chain_node_data.coldkey,
                        ip=new_chain_node_data.ip, ip_type=str(new_chain_node_data.ip_type), port=new_chain_node_data.port,
                        incentive=float(new_chain_node_data.incentive), stake=float(new_chain_node_data.stake),
                        trust=float(new_chain_node_data.trust), vtrust=float(new_chain_node_data.vtrust),
                        protocol=str(new_chain_node_data.protocol)
                    )
                    
                    # Update in-memory state
                    if uid_to_process in self.nodes:
                         del self.nodes[uid_to_process]
                    self.nodes[uid_to_process] = {"hotkey": new_chain_node_data.hotkey, "uid": uid_to_process}
                    logger.info(f"Successfully processed hotkey change for UID {uid_to_process}.")
                    
                except Exception as e:
                    logger.error(f"Error processing hotkey change for UID {uid_to_process}: {str(e)}", exc_info=True)

    async def _update_existing_miner_info(self, uids_to_update_info: Dict, uids_to_clear_and_update: Dict):
        """Update info for existing miners where hotkey didn't change."""
        logger.info(f"Updating potentially changed info (stake, IP, etc.) for {len(uids_to_update_info)} existing UIDs...")
        
        batch_updates = []
        for uid_to_update, chain_node_data in uids_to_update_info.items():
            if uid_to_update in uids_to_clear_and_update: 
                continue
                
            batch_updates.append({
                "index": uid_to_update,
                "hotkey": chain_node_data.hotkey,
                "coldkey": chain_node_data.coldkey,
                "ip": chain_node_data.ip,
                "ip_type": str(chain_node_data.ip_type),
                "port": chain_node_data.port,
                "incentive": float(chain_node_data.incentive),
                "stake": float(chain_node_data.stake),
                "trust": float(chain_node_data.trust),
                "vtrust": float(chain_node_data.vtrust),
                "protocol": str(chain_node_data.protocol)
            })
            
            self.nodes[uid_to_update] = {"hotkey": chain_node_data.hotkey, "uid": uid_to_update}
        
        if batch_updates:
            try:
                await self.database_manager.batch_update_miners(batch_updates)
                logger.info(f"Successfully batch updated {len(batch_updates)} existing miners")
            except Exception as e:
                logger.error(f"Error in batch update of existing miners: {str(e)}")
                for update_data in batch_updates:
                    try:
                        uid = update_data["index"]
                        await self.database_manager.update_miner_info(**update_data)
                        logger.debug(f"Successfully updated info for existing UID {uid} (fallback)")
                    except Exception as individual_e:
                        logger.error(f"Error updating info for existing UID {uid}: {str(individual_e)}", exc_info=True)

    async def _handle_new_miners(self, chain_nodes_info: Dict, processed_uids: Set):
        """Handle new miners detected on the chain."""
        new_miners_detected = 0
        new_miner_updates = []
        
        for chain_uid, chain_node in chain_nodes_info.items():
            if chain_uid not in processed_uids:
                logger.info(f"New miner detected on chain: UID {chain_uid}, Hotkey {chain_node.hotkey}. Adding to DB.")
                
                new_miner_updates.append({
                    "index": chain_uid,
                    "hotkey": chain_node.hotkey,
                    "coldkey": chain_node.coldkey,
                    "ip": chain_node.ip,
                    "ip_type": str(chain_node.ip_type),
                    "port": chain_node.port,
                    "incentive": float(chain_node.incentive),
                    "stake": float(chain_node.stake),
                    "trust": float(chain_node.trust),
                    "vtrust": float(chain_node.vtrust),
                    "protocol": str(chain_node.protocol)
                })
                
                self.nodes[chain_uid] = {"hotkey": chain_node.hotkey, "uid": chain_uid}
                new_miners_detected += 1
                processed_uids.add(chain_uid)
        
        if new_miner_updates:
            try:
                await self.database_manager.batch_update_miners(new_miner_updates)
                logger.info(f"Successfully batch added {new_miners_detected} new miners to the database")
            except Exception as e:
                logger.error(f"Error in batch update of new miners: {str(e)}")
                successful_adds = 0
                for update_data in new_miner_updates:
                    try:
                        uid = update_data["index"]
                        await self.database_manager.update_miner_info(**update_data)
                        successful_adds += 1
                    except Exception as individual_e:
                        logger.error(f"Error adding new miner UID {uid} (Hotkey: {update_data['hotkey']}): {str(individual_e)}", exc_info=True)
                if successful_adds > 0:
                    logger.info(f"Added {successful_adds} new miners to the database (fallback)")

    async def cleanup(self):
        """Clean up miner manager resources."""
        try:
            logger.info("Miner manager cleanup completed")
        except Exception as e:
            logger.error(f"Error during miner manager cleanup: {e}")