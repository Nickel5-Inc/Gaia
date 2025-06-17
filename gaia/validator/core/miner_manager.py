import asyncio
from typing import Dict, Set, List, Any, Optional
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class MinerManager:
    """
    Handles all miner lifecycle management operations including:
    - Hotkey change detection and cleanup
    - New miner registration
    - Miner state synchronization
    - Database cleanup for deregistered miners
    """
    
    def __init__(self, validator):
        self.validator = validator
        
    async def handle_miner_deregistration_loop(self) -> None:
        """
        Main loop for miner state synchronization.
        Handles hotkey changes, new miners, and info updates.
        """
        logger.info("Starting miner state synchronization loop (handles hotkey changes, new miners, and info updates)")
        
        while True:
            processed_uids = set()  # Keep track of UIDs processed in this cycle
            try:
                # Ensure metagraph is up to date
                if not self.validator.metagraph:
                    logger.warning("Metagraph object not initialized, cannot sync miner state.")
                    await asyncio.sleep(600)  # Sleep before retrying
                    continue  # Wait for metagraph to be initialized
                
                logger.info("Syncing metagraph for miner state update...")
                await self.validator.neuron.sync_metagraph(self.validator.metagraph_sync_interval)
                self.validator.metagraph = self.validator.neuron.metagraph  # Ensure validator's metagraph is updated
                if not self.validator.metagraph.nodes:
                    logger.warning("Metagraph empty after sync, skipping miner state update.")
                    await asyncio.sleep(600)  # Sleep before retrying
                    continue

                async with self.validator.miner_table_lock:
                    logger.info("Performing miner hotkey change check and info update...")
                    
                    # Get current UIDs and hotkeys from the chain's metagraph using managed fetching
                    try:
                        active_nodes_list = await self.validator.neuron._fetch_nodes_managed(self.validator.metagraph.netuid)
                        if active_nodes_list is None:
                            active_nodes_list = []  # Ensure it's an iterable
                            logger.warning("Managed node fetching returned None in handle_miner_deregistration_loop.")
                    except Exception as e_fetch_nodes_dereg:
                        logger.error(f"Failed to fetch nodes in handle_miner_deregistration_loop: {e_fetch_nodes_dereg}", exc_info=True)
                        active_nodes_list = []
                    
                    # Build chain_nodes_info mapping node_id (UID) to Node object
                    chain_nodes_info = {node.node_id: node for node in active_nodes_list}

                    # Get UIDs and hotkeys from our local database
                    db_miner_query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL;"
                    db_miners_rows = await self.validator.database_manager.fetch_all(db_miner_query)
                    db_miners_info = {row["uid"]: row["hotkey"] for row in db_miners_rows}

                    # Analyze differences and plan updates
                    uids_to_clear_and_update, uids_to_update_info = self._analyze_miner_changes(
                        db_miners_info, chain_nodes_info, processed_uids
                    )
                    
                    # Process hotkey changes
                    if uids_to_clear_and_update:
                        await self._process_hotkey_changes(uids_to_clear_and_update, db_miners_info)
                    
                    # Update existing miners' info
                    if uids_to_update_info:
                        await self._update_existing_miners_info(uids_to_update_info, uids_to_clear_and_update)
                    
                    # Add new miners
                    new_miners_detected = await self._add_new_miners(chain_nodes_info, processed_uids)
                    
                    logger.info("Miner state synchronization cycle completed.")

            except asyncio.CancelledError:
                logger.info("Miner state synchronization loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in miner state synchronization loop: {e}", exc_info=True)
            
            # Sleep at the end of the loop - runs immediately on first iteration, then every 10 minutes
            await asyncio.sleep(600)  # Run every 10 minutes

    def _analyze_miner_changes(self, db_miners_info: Dict, chain_nodes_info: Dict, processed_uids: Set) -> tuple:
        """
        Analyze differences between database and chain state.
        Returns tuples of UIDs that need different types of updates.
        """
        uids_to_clear_and_update = {}  # uid: new_chain_node
        uids_to_update_info = {}  # uid: new_chain_node (for existing miners with potentially changed info)

        # Check existing DB miners against the chain
        for db_uid, db_hotkey in db_miners_info.items():
            processed_uids.add(db_uid)  # Mark as processed
            chain_node_for_uid = chain_nodes_info.get(db_uid)

            if chain_node_for_uid is None:
                # This case *shouldn't* happen if metagraph always fills slots,
                # but handle defensively. Might indicate UID truly removed.
                logger.warning(f"UID {db_uid} (DB hotkey: {db_hotkey}) not found in current metagraph sync. Potential deregistration missed? Skipping.")
                continue 
                
            if chain_node_for_uid.hotkey != db_hotkey:
                # Hotkey for this UID has changed!
                logger.info(f"UID {db_uid} hotkey changed. DB: {db_hotkey}, Chain: {chain_node_for_uid.hotkey}. Marking for data cleanup and update.")
                uids_to_clear_and_update[db_uid] = chain_node_for_uid
            else:
                # Hotkey matches, but other info might have changed. Mark for potential update.
                uids_to_update_info[db_uid] = chain_node_for_uid

        return uids_to_clear_and_update, uids_to_update_info

    async def _process_hotkey_changes(self, uids_to_clear_and_update: Dict, db_miners_info: Dict):
        """Process UIDs where hotkeys have changed - cleanup old data and update."""
        logger.info(f"Cleaning old data and updating hotkeys for UIDs: {list(uids_to_clear_and_update.keys())}")
        
        for uid_to_process, new_chain_node_data in uids_to_clear_and_update.items():
            original_hotkey = db_miners_info.get(uid_to_process)  # Get the old hotkey from DB cache
            if not original_hotkey:
                logger.warning(f"Could not find original hotkey in DB cache for UID {uid_to_process}. Skipping cleanup for this UID.")
                continue
                
            logger.info(f"Processing hotkey change for UID {uid_to_process}: Old={original_hotkey}, New={new_chain_node_data.hotkey}")
            try:
                # 1. Delete from prediction tables by UID
                await self._cleanup_prediction_tables(uid_to_process, original_hotkey)
                
                # 2. Delete from history tables by OLD hotkey
                await self._cleanup_history_tables(uid_to_process, original_hotkey)
                
                # 3. Zero out ALL scores for the UID in score_table
                await self._cleanup_score_tables(uid_to_process)
                
                # 4. Update node_table with NEW info
                await self._update_node_table(uid_to_process, new_chain_node_data)
                
                logger.info(f"Successfully processed hotkey change for UID {uid_to_process}.")
                
            except Exception as e:
                logger.error(f"Error processing hotkey change for UID {uid_to_process}: {str(e)}", exc_info=True)

    async def _cleanup_prediction_tables(self, uid_to_process: int, original_hotkey: str):
        """Clean up prediction tables for a UID with changed hotkey."""
        prediction_tables_by_uid = ["geomagnetic_predictions", "soil_moisture_predictions"]
        for table_name in prediction_tables_by_uid:
            try:
                table_exists_res = await self.validator.database_manager.fetch_one(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
                )
                if table_exists_res and table_exists_res['exists']:
                    # Delete using the original hotkey associated with the UID
                    await self.validator.database_manager.execute(
                        f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", 
                        {"hotkey": original_hotkey}
                    )
                    logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                # No else needed, if table doesn't exist, we just skip
            except Exception as e_pred_del:
                logger.warning(f"  Could not clear {table_name} for UID {uid_to_process}: {e_pred_del}")

    async def _cleanup_history_tables(self, uid_to_process: int, original_hotkey: str):
        """Clean up history tables for a UID with changed hotkey."""
        history_tables_by_hotkey = ["geomagnetic_history", "soil_moisture_history"]
        for table_name in history_tables_by_hotkey:
            try:
                table_exists_res = await self.validator.database_manager.fetch_one(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
                )
                if table_exists_res and table_exists_res['exists']:
                    await self.validator.database_manager.execute(
                        f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", 
                        {"hotkey": original_hotkey}
                    )
                    logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
            except Exception as e_hist_del:
                logger.warning(f"  Could not clear {table_name} for old hotkey {original_hotkey}: {e_hist_del}")

    async def _cleanup_score_tables(self, uid_to_process: int):
        """Zero out all scores for a UID in score_table."""
        distinct_task_names_rows = await self.validator.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
        all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
        if all_task_names_in_scores:
            logger.info(f"  Zeroing all scores in score_table for UID {uid_to_process} across tasks: {all_task_names_in_scores}")
            await self.validator.database_manager.remove_miner_from_score_tables(
                uids=[uid_to_process],
                task_names=all_task_names_in_scores,
                filter_start_time=None, 
                filter_end_time=None  # Affect all history
            )
        else:
            logger.info(f"  No task names found in score_table to zero-out for UID {uid_to_process}.")

    async def _update_node_table(self, uid_to_process: int, new_chain_node_data: Any):
        """Update node_table with new chain data."""
        logger.info(f"  Updating node_table info for UID {uid_to_process} with new hotkey {new_chain_node_data.hotkey}.")
        await self.validator.database_manager.update_miner_info(
            index=uid_to_process, 
            hotkey=new_chain_node_data.hotkey, 
            coldkey=new_chain_node_data.coldkey,
            ip=new_chain_node_data.ip, 
            ip_type=str(new_chain_node_data.ip_type), 
            port=new_chain_node_data.port,
            incentive=float(new_chain_node_data.incentive), 
            stake=float(new_chain_node_data.stake),
            trust=float(new_chain_node_data.trust), 
            vtrust=float(new_chain_node_data.vtrust),
            protocol=str(new_chain_node_data.protocol)
        )
        
        # Update in-memory state as well
        if uid_to_process in self.validator.nodes:
            del self.validator.nodes[uid_to_process]  # Remove old entry if exists
        self.validator.nodes[uid_to_process] = {"hotkey": new_chain_node_data.hotkey, "uid": uid_to_process}

    async def _update_existing_miners_info(self, uids_to_update_info: Dict, uids_to_clear_and_update: Dict):
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
            
            self.validator.nodes[uid_to_update] = {"hotkey": chain_node_data.hotkey, "uid": uid_to_update}
        
        if batch_updates:
            try:
                await self.validator.database_manager.batch_update_miners(batch_updates)
                logger.info(f"Successfully batch updated {len(batch_updates)} existing miners")
            except Exception as e:
                logger.error(f"Error in batch update of existing miners: {str(e)}")
                # Fallback to individual updates
                for update_data in batch_updates:
                    try:
                        uid = update_data["index"]
                        await self.validator.database_manager.update_miner_info(**update_data)
                        logger.debug(f"Successfully updated info for existing UID {uid} (fallback)")
                    except Exception as individual_e:
                        logger.error(f"Error updating info for existing UID {uid}: {str(individual_e)}", exc_info=True)

    async def _add_new_miners(self, chain_nodes_info: Dict, processed_uids: Set) -> int:
        """Add new miners detected on the chain."""
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
                
                self.validator.nodes[chain_uid] = {"hotkey": chain_node.hotkey, "uid": chain_uid}
                new_miners_detected += 1
                processed_uids.add(chain_uid)
        
        if new_miner_updates:
            try:
                await self.validator.database_manager.batch_update_miners(new_miner_updates)
                logger.info(f"Successfully batch added {new_miners_detected} new miners to the database")
            except Exception as e:
                logger.error(f"Error in batch update of new miners: {str(e)}")
                # Fallback to individual updates
                successful_adds = 0
                for update_data in new_miner_updates:
                    try:
                        uid = update_data["index"]
                        await self.validator.database_manager.update_miner_info(**update_data)
                        successful_adds += 1
                    except Exception as individual_e:
                        logger.error(f"Error adding new miner UID {uid} (Hotkey: {update_data['hotkey']}): {str(individual_e)}", exc_info=True)
                if successful_adds > 0:
                    logger.info(f"Added {successful_adds} new miners to the database (fallback)")
        
        return new_miners_detected 