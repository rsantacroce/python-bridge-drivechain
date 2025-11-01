#!/usr/bin/env python3
"""
Bridge for Bitcoin node instance 1 to Bitcoin node instance 2

This script fetches blocks from Bitcoin node instance 1 via JSON-RPC,
skips coinbase transactions, and relays remaining transactions to 
Bitcoin node instance 2. It enforces block processing rules: it will
not proceed to the next block until relayed transactions appear in
instance 2's mempool.

Examples:
- Process blocks from height 850000 to 850100:
  ./main.py --from-height 850000 --to-height 850100

- Start at height 850000 and follow new blocks:
  ./main.py --from-height 850000 --follow

- Process a single block:
  ./main.py --block-height 850000
"""
import argparse
import os
import time
import logging
import sqlite3
from datetime import datetime
from bitcoinrpc.authproxy import AuthServiceProxy
from urllib.parse import urlparse

# Configuration (can be overridden via CLI/env)
NODE1_RPC_DEFAULT = os.environ.get("NODE1_RPC", "http://user:password@127.0.0.1:8332")
NODE2_RPC_DEFAULT = os.environ.get("NODE2_RPC", "http://user:password@127.0.0.1:18332")
DB_PATH_DEFAULT = os.environ.get("DB_PATH", "tx_bridge.db")
TX_DELAY_DEFAULT = float(os.environ.get("TX_DELAY", "0.1"))  # Delay in seconds between sending transactions
BLOCK_DELAY_DEFAULT = float(os.environ.get("BLOCK_DELAY", "1.0"))  # Delay in seconds before processing next block

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s'
)
logger = logging.getLogger("bridge")


# Database functions
def init_database(db_path: str = DB_PATH_DEFAULT) -> sqlite3.Connection:
    """Initialize SQLite database and create tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            source_txid TEXT NOT NULL,
            dest_txid TEXT,
            block_hash TEXT,
            block_height INTEGER,
            tx_index INTEGER,
            raw_hex TEXT,
            status TEXT NOT NULL,
            message TEXT,
            error_message TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_source_txid ON transactions(source_txid)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_dest_txid ON transactions(dest_txid)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_block_height ON transactions(block_height)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)
    """)
    conn.commit()
    logger.info(f"Database initialized at {db_path}")
    return conn


def check_transaction_sent(conn: sqlite3.Connection, source_txid: str) -> dict | None:
    """Check if a transaction has been successfully sent before.
    
    Returns the most recent successful transaction record if found, None otherwise.
    Only considers transactions with status 'success' or 'already_in_mempool'.
    """
    cursor = conn.execute("""
        SELECT id, timestamp, source_txid, dest_txid, block_hash, block_height, 
               tx_index, status, message
        FROM transactions
        WHERE source_txid = ? 
          AND status IN ('success', 'already_in_mempool')
        ORDER BY timestamp DESC
        LIMIT 1
    """, (source_txid,))
    
    row = cursor.fetchone()
    if row:
        return {
            "id": row[0],
            "timestamp": row[1],
            "source_txid": row[2],
            "dest_txid": row[3],
            "block_hash": row[4],
            "block_height": row[5],
            "tx_index": row[6],
            "status": row[7],
            "message": row[8],
        }
    return None


def save_transaction(conn: sqlite3.Connection, source_txid: str, dest_txid: str | None,
                    block_hash: str | None, block_height: int | None, tx_index: int,
                    raw_hex: str, status: str, message: str | None, error_message: str | None) -> None:
    """Save a transaction to the database with its metadata and messages."""
    timestamp = datetime.utcnow().isoformat()
    conn.execute("""
        INSERT INTO transactions 
        (timestamp, source_txid, dest_txid, block_hash, block_height, tx_index, 
         raw_hex, status, message, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (timestamp, source_txid, dest_txid, block_hash, block_height, tx_index,
          raw_hex, status, message, error_message))
    conn.commit()


def _build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Bitcoin Block Bridge (Node 1 -> Node 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
The script enforces block processing rules: it will not proceed to the next block
until at least one relayed transaction from the current block appears in node 2's mempool.

If --strict is enabled, the script will stop if transactions don't appear in mempool
within the timeout. Otherwise, it will log a warning and continue.
        """
    )
    
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--block-height", type=int, help="Process a specific block height")
    mode.add_argument("--block-hash", type=str, help="Process a specific block hash")
    
    parser.add_argument("--from-height", type=int, help="Start height for sequential block processing")
    parser.add_argument("--to-height", type=int, help="End height for sequential block processing (inclusive)")
    parser.add_argument("--follow", action="store_true", help="Continue processing new blocks indefinitely")
    
    parser.add_argument("--wait-timeout", type=int, default=int(os.environ.get("WAIT_TIMEOUT", 600)), 
                       help="Seconds to wait for node 2 mempool to receive relayed txs before next block (default: 600)")
    parser.add_argument("--poll-interval", type=int, default=int(os.environ.get("POLL_INTERVAL", 5)), 
                       help="Seconds between mempool checks while waiting (default: 5)")
    parser.add_argument("--tx-delay", type=float, default=TX_DELAY_DEFAULT,
                       help=f"Seconds to wait between sending each transaction (default: {TX_DELAY_DEFAULT})")
    parser.add_argument("--block-delay", type=float, default=BLOCK_DELAY_DEFAULT,
                       help=f"Seconds to wait before processing the next block (default: {BLOCK_DELAY_DEFAULT})")
    parser.add_argument("--strict", action="store_true", 
                       help="Stop processing if transactions don't appear in mempool within timeout (default: continue with warning)")
    
    parser.add_argument("--node1-rpc", default=NODE1_RPC_DEFAULT, help="Node 1 (source) RPC URL")
    parser.add_argument("--node2-rpc", default=NODE2_RPC_DEFAULT, help="Node 2 (destination) RPC URL")
    parser.add_argument("--db-path", default=DB_PATH_DEFAULT, help=f"SQLite database path (default: {DB_PATH_DEFAULT})")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"), 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
    return parser


def _validate_rpc_url(rpc_url: str, which: str) -> None:
    """Ensure the RPC URL has an http/https scheme.

    Raises ValueError if invalid.
    """
    parsed = urlparse(rpc_url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"{which} must be an http(s) URL, got '{rpc_url}'")
    if not parsed.hostname:
        raise ValueError(f"{which} URL is missing hostname: '{rpc_url}'")


def test_connections(node1_rpc_url: str, node2_rpc_url: str) -> bool:
    """Test that both RPC connections are accessible."""
    print("Testing connections...")
    
    # Validate RPC URLs early
    try:
        _validate_rpc_url(node1_rpc_url, "Node 1 RPC")
        _validate_rpc_url(node2_rpc_url, "Node 2 RPC")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        return False
    
    # Test node 1 RPC
    try:
        node1 = AuthServiceProxy(node1_rpc_url)
        height = node1.getblockcount()
        print(f"✓ Node 1 RPC working - Height: {height}")
    except Exception as e:
        print(f"✗ Node 1 RPC failed: {e}")
        return False
    
    # Test node 2 RPC
    try:
        node2 = AuthServiceProxy(node2_rpc_url)
        height = node2.getblockcount()
        print(f"✓ Node 2 RPC working - Height: {height}")
    except Exception as e:
        print(f"✗ Node 2 RPC failed: {e}")
        return False
    
    print("\n✓ All connections successful!")
    return True


def relay_block_transactions(node1_rpc_url: str, node2_rpc_url: str, *, block_height: int | None = None, block_hash: str | None = None, db_conn: sqlite3.Connection | None = None, tx_delay: float = 0.0) -> dict:
    """Fetch a block from node 1, skip coinbase, and relay txs to node 2.

    Returns stats dict with relayed_txids.
    """
    if not block_height and not block_hash:
        raise ValueError("Either block_height or block_hash must be provided")
    
    node1 = AuthServiceProxy(node1_rpc_url)
    node2 = AuthServiceProxy(node2_rpc_url)
    
    if block_height is not None:
        logger.info(f"Resolving block hash for height {block_height}")
        block_hash = node1.getblockhash(block_height)
    
    logger.info(f"Fetching block {block_hash}")
    block = node1.getblock(block_hash, 1)  # verbosity=1 returns txids
    txids_raw = block.get("tx", [])
    
    # Ensure txids is a list
    if not isinstance(txids_raw, list):
        logger.error(f"Block 'tx' field is not a list: {type(txids_raw)}. Got: {txids_raw}")
        txids = []
    else:
        txids = txids_raw
    
    # Get the actual transaction count from block metadata
    block_tx_count = block.get("nTx", None)
    total_tx_count = len(txids) if txids else 0
    
    # Verify transaction count matches
    if block_tx_count is not None and block_tx_count != total_tx_count:
        logger.error(f"Transaction count mismatch! Block reports nTx={block_tx_count} but extracted {total_tx_count} txids. This may indicate a problem with block data extraction.")
    
    logger.info(f"Block contains {total_tx_count} total transaction(s)" + (f" (block reports nTx={block_tx_count})" if block_tx_count else ""))
    
    if not txids:
        logger.warning("Block has no transactions")
        return {"total": 0, "relayed": 0, "failed": 0, "skipped_coinbase": 0, "already_in_mempool": 0, "already_sent": 0, "relayed_txids": []}
    
    if total_tx_count == 1:
        if block_tx_count and block_tx_count > 1:
            logger.error(f"CRITICAL: Block {block_hash} reports {block_tx_count} transactions but only 1 txid was extracted! Transactions are being lost!")
        else:
            logger.info(f"Block {block_hash} only contains coinbase transaction - skipping block")
            return {"total": 0, "relayed": 0, "failed": 0, "skipped_coinbase": 1, "already_in_mempool": 0, "already_sent": 0, "relayed_txids": [], "block_hash": block_hash, "block_height": block_height, "coinbase_only": True}
    else:
        logger.info(f"Will process {total_tx_count - 1} non-coinbase transaction(s)")
    
    # Get node2 mempool once at the start
    try:
        node2_mempool = set(node2.getrawmempool())
        logger.debug(f"Node 2 mempool contains {len(node2_mempool)} transactions")
    except Exception as e:
        logger.warning(f"Failed to get node 2 mempool: {e}, will check each transaction individually")
        node2_mempool = set()
    
    # Skip coinbase (first tx in block)
    relayed = 0
    failed = 0
    skipped_coinbase = 0
    already_in_mempool = 0
    already_sent = 0
    total_candidates = 0
    relayed_txids: list[str] = []
    
    for idx, txid in enumerate(txids):
        if idx == 0:
            skipped_coinbase += 1
            logger.debug(f"Skipping coinbase tx {txid}")
            continue
        
        total_candidates += 1
        
        # Check if transaction was already sent successfully (from database)
        if db_conn:
            prev_record = check_transaction_sent(db_conn, txid)
            if prev_record:
                already_sent += 1
                prev_status = prev_record.get("status", "unknown")
                prev_timestamp = prev_record.get("timestamp", "unknown")
                prev_block = prev_record.get("block_height", "unknown")
                message = f"Tx {txid} already sent before (status: {prev_status}, block: {prev_block}, time: {prev_timestamp}), skipping send"
                logger.info(message)
                relayed_txids.append(prev_record.get("dest_txid") or txid)
                
                # Optionally save a new record referencing the previous send
                # This allows tracking that we saw this tx again in a new block
                try:
                    raw_hex = node1.getrawtransaction(txid, False, block_hash)
                except:
                    raw_hex = ""
                
                save_transaction(
                    db_conn,
                    source_txid=txid,
                    dest_txid=prev_record.get("dest_txid") or txid,
                    block_hash=block_hash,
                    block_height=block_height,
                    tx_index=idx,
                    raw_hex=raw_hex,
                    status="already_sent",
                    message=message,
                    error_message=None
                )
                continue
        
        # Check if transaction is already in node2's mempool
        if txid in node2_mempool:
            already_in_mempool += 1
            relayed_txids.append(txid)
            message = f"Tx {txid} already in node 2 mempool, skipping send"
            logger.info(message)
            
            # Save to database if connection provided
            if db_conn:
                try:
                    raw_hex = node1.getrawtransaction(txid, False, block_hash)
                except:
                    raw_hex = ""  # If we can't get raw hex, store empty string
                
                save_transaction(
                    db_conn,
                    source_txid=txid,
                    dest_txid=txid,
                    block_hash=block_hash,
                    block_height=block_height,
                    tx_index=idx,
                    raw_hex=raw_hex,
                    status="already_in_mempool",
                    message=message,
                    error_message=None
                )
            continue
        
        try:
            # Obtain raw hex; include block hash for pruned nodes
            raw_hex = node1.getrawtransaction(txid, False, block_hash)
            sent_txid = node2.sendrawtransaction(raw_hex)
            relayed += 1
            actual_dest_txid = sent_txid if isinstance(sent_txid, str) else txid
            relayed_txids.append(actual_dest_txid)
            message = f"Relayed tx {txid} -> node 2 txid {actual_dest_txid}"
            logger.info(message)
            
            # Save to database if connection provided
            if db_conn:
                save_transaction(
                    db_conn,
                    source_txid=txid,
                    dest_txid=actual_dest_txid,
                    block_hash=block_hash,
                    block_height=block_height,
                    tx_index=idx,
                    raw_hex=raw_hex,
                    status="success",
                    message=message,
                    error_message=None
                )
            
            # Wait before sending next transaction
            if tx_delay > 0:
                time.sleep(tx_delay)
        except Exception as e:
            failed += 1
            error_msg = f"Failed to relay tx {txid}: {e}"
            logger.error(error_msg)
            
            # Save failed transaction to database if connection provided
            if db_conn:
                try:
                    # Try to get raw hex even if send failed
                    raw_hex = node1.getrawtransaction(txid, False, block_hash)
                except:
                    raw_hex = ""  # If we can't get raw hex, store empty string
                
                save_transaction(
                    db_conn,
                    source_txid=txid,
                    dest_txid=None,
                    block_hash=block_hash,
                    block_height=block_height,
                    tx_index=idx,
                    raw_hex=raw_hex,
                    status="failed",
                    message=None,
                    error_message=str(e)
                )
            
            # Wait before processing next transaction even on failure
            if tx_delay > 0:
                time.sleep(tx_delay)
    
    stats = {
        "total": total_candidates,
        "relayed": relayed,
        "failed": failed,
        "skipped_coinbase": skipped_coinbase,
        "already_in_mempool": already_in_mempool,
        "already_sent": already_sent,
        "block_hash": block_hash,
        "block_height": block_height,
        "relayed_txids": relayed_txids,
    }
    logger.info(f"Block relay complete: {relayed} relayed, {already_sent} already sent (from DB), {already_in_mempool} already in mempool, {failed} failed, {skipped_coinbase} skipped (coinbase)")
    return stats


def wait_for_block_creation(node2_rpc_url: str, block_hash: str, *, timeout_seconds: int, poll_interval_seconds: int) -> dict:
    """Poll node 2 until the block hash exists in its blockchain, or timeout.

    Returns a dict with success status and elapsed time.
    """
    node2 = AuthServiceProxy(node2_rpc_url)
    start_ts = time.time()
    
    logger.info(f"Waiting for block {block_hash} to be created on node 2...")
    
    while True:
        try:
            # Try to get the block - if it exists, this will succeed
            block_info = node2.getblock(block_hash, 1)
            if block_info:
                elapsed = int(time.time() - start_ts)
                block_height = block_info.get("height", "unknown")
                logger.info(f"✓ Block {block_hash} found on node 2 at height {block_height} after {elapsed}s")
                return {"success": True, "elapsed": elapsed, "block_height": block_height}
        except Exception as e:
            # Block doesn't exist yet or other error
            error_msg = str(e).lower()
            if "block not found" in error_msg or "not found" in error_msg:
                # Block doesn't exist yet, continue waiting
                pass
            else:
                logger.warning(f"Error checking block on node 2: {e}")
        
        elapsed = time.time() - start_ts
        if elapsed >= timeout_seconds:
            elapsed_int = int(elapsed)
            logger.warning(f"✗ Timeout {timeout_seconds}s waiting for block {block_hash} to be created on node 2")
            return {"success": False, "elapsed": elapsed_int}
        
        time.sleep(poll_interval_seconds)


def wait_for_mempool_presence(node2_rpc_url: str, candidate_txids: list[str], *, timeout_seconds: int, poll_interval_seconds: int) -> dict:
    """Poll node 2 mempool until at least one candidate txid is present, or timeout.

    Returns a dict with found txids and elapsed time.
    """
    if not candidate_txids:
        logger.warning("No candidate transactions to wait for")
        return {"found": [], "elapsed": 0, "success": True}
    
    node2 = AuthServiceProxy(node2_rpc_url)
    start_ts = time.time()
    found: list[str] = []
    
    logger.info(f"Waiting for {len(candidate_txids)} transaction(s) to appear in node 2 mempool...")
    
    # Check if mempool is empty at the start
    try:
        initial_mempool = set(node2.getrawmempool())
        if len(initial_mempool) == 0:
            logger.info(f"Node 2 mempool is empty - continuing to send more transactions from next block")
            return {"found": [], "elapsed": 0, "success": False, "mempool_empty": True}
    except Exception as e:
        logger.debug(f"Initial mempool check failed: {e}")
    
    while True:
        try:
            mempool_txids = set(node2.getrawmempool())
            mempool_size = len(mempool_txids)
        except Exception as e:
            logger.warning(f"Failed to query node 2 mempool: {e}")
            mempool_txids = set()
            mempool_size = 0
        
        # If mempool is empty, return immediately to allow sending more transactions
        if mempool_size == 0:
            elapsed = int(time.time() - start_ts)
            logger.info(f"Node 2 mempool is empty - continuing to send more transactions from next block (waited {elapsed}s)")
            return {"found": [], "elapsed": elapsed, "success": False, "mempool_empty": True}
        
        found = [txid for txid in candidate_txids if txid in mempool_txids]
        if found:
            elapsed = int(time.time() - start_ts)
            logger.info(f"✓ Found {len(found)}/{len(candidate_txids)} transaction(s) in mempool after {elapsed}s")
            return {"found": found, "elapsed": elapsed, "success": True, "mempool_empty": False}
        
        elapsed = time.time() - start_ts
        if elapsed >= timeout_seconds:
            elapsed_int = int(elapsed)
            logger.warning(f"✗ Timeout {timeout_seconds}s waiting for transactions to appear in mempool (found 0/{len(candidate_txids)})")
            return {"found": [], "elapsed": elapsed_int, "success": False, "mempool_empty": False}
        
        time.sleep(poll_interval_seconds)


def process_blocks_sequential(node1_rpc_url: str, node2_rpc_url: str, *, start_height: int, end_height: int | None, 
                             follow: bool, wait_timeout: int, poll_interval: int, strict: bool, db_conn: sqlite3.Connection | None = None, tx_delay: float = 0.0, block_delay: float = 0.0) -> None:
    """Process blocks sequentially, enforcing that relayed transactions appear in node 2 mempool before proceeding to the next block."""
    node1 = AuthServiceProxy(node1_rpc_url)
    
    current = start_height
    blocks_processed = 0
    blocks_failed = 0
    
    logger.info(f"Starting sequential block processing from height {start_height}")
    if end_height is not None:
        logger.info(f"Will process up to height {end_height} (inclusive)")
    if follow:
        logger.info("Will follow new blocks indefinitely")
    
    while True:
        chain_height = node1.getblockcount()
        
        if end_height is not None and current > end_height:
            logger.info(f"Reached end height {end_height}; stopping")
            break
        
        if current > chain_height:
            if follow:
                logger.info(f"Current height {current} > chain height {chain_height}, waiting for new blocks...")
                time.sleep(max(1, poll_interval))
                continue
            else:
                logger.info(f"Current height {current} > chain height {chain_height}; stopping")
                break
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing block at height {current}")
        logger.info(f"{'='*60}")
        
        try:
            stats = relay_block_transactions(node1_rpc_url, node2_rpc_url, block_height=current, db_conn=db_conn, tx_delay=tx_delay)
            block_hash = stats.get("block_hash")
            relayed_txids = stats.get("relayed_txids", [])
            blocks_processed += 1
            
            # Skip blocks that only contain coinbase
            if stats.get("coinbase_only", False):
                logger.info(f"Skipping block {current} - only contains coinbase, moving to next block")
                current += 1
                continue
            
            if not relayed_txids:
                logger.info("No transactions were relayed (block may only contain coinbase or all were already sent)")
                # Wait before processing next block even if no transactions were relayed
                if block_delay > 0:
                    logger.debug(f"Waiting {block_delay}s before processing next block...")
                    time.sleep(block_delay)
                current += 1
                continue
            
            # Wait for transactions to appear in mempool before proceeding to next block
            wait_info = wait_for_mempool_presence(
                node2_rpc_url,
                relayed_txids,
                timeout_seconds=wait_timeout,
                poll_interval_seconds=poll_interval,
            )
            
            # If mempool is empty, continue immediately to send more transactions
            if wait_info.get("mempool_empty", False):
                logger.info(f"Mempool empty - continuing to next block to send more transactions")
                # Don't increment blocks_failed, this is expected behavior
            elif not wait_info["success"]:
                if strict:
                    logger.error(f"Strict mode enabled: stopping due to mempool timeout at block {current} ({block_hash})")
                    logger.error(f"Processed {blocks_processed} blocks successfully, {blocks_failed} blocks failed")
                    return
                else:
                    logger.warning(f"Continuing despite mempool timeout (strict mode disabled)")
                    blocks_failed += 1
            else:
                found_count = len(wait_info.get("found", []))
                total_count = len(relayed_txids)
                logger.info(f"✓ Block {current} ({block_hash}): {found_count}/{total_count} transactions confirmed in mempool")
            
        except Exception as e:
            logger.error(f"Error processing block {current}: {e}")
            if strict:
                logger.error(f"Strict mode enabled: stopping due to error")
                return
            blocks_failed += 1
        
        # Wait before processing next block
        if block_delay > 0:
            logger.debug(f"Waiting {block_delay}s before processing next block...")
            time.sleep(block_delay)
        
        current += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Sequential processing complete")
    logger.info(f"Blocks processed: {blocks_processed}")
    logger.info(f"Blocks failed: {blocks_failed}")
    logger.info(f"{'='*60}")


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()
    
    # Configure logging level early
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Tame extremely verbose bitcoinrpc debug logs which can include raw bytes
    bitcoinrpc_log = logging.getLogger("BitcoinRPC")
    if os.environ.get("BITCOINRPC_LOG", "").upper() == "DEBUG":
        bitcoinrpc_log.setLevel(logging.DEBUG)
    else:
        bitcoinrpc_log.setLevel(logging.INFO)
    
    print("=" * 60)
    print("Bitcoin Block Bridge (Node 1 -> Node 2)")
    print("=" * 60)
    
    # Initialize database
    db_conn = init_database(args.db_path)
    
    # Test connections
    if not test_connections(args.node1_rpc, args.node2_rpc):
        print("\nPlease fix connection issues before starting bridge")
        db_conn.close()
        return
    
    # Single block mode
    if args.block_height is not None or args.block_hash is not None:
        logger.info("Starting single block relay mode")
        stats = relay_block_transactions(
            args.node1_rpc,
            args.node2_rpc,
            block_height=args.block_height,
            block_hash=args.block_hash,
            db_conn=db_conn,
            tx_delay=args.tx_delay,
        )
        relayed_txids = stats.get("relayed_txids", [])
        
        if stats.get("coinbase_only", False):
            logger.info("Block only contains coinbase transaction - nothing to relay")
        elif relayed_txids:
            wait_info = wait_for_mempool_presence(
                args.node2_rpc,
                relayed_txids,
                timeout_seconds=args.wait_timeout,
                poll_interval_seconds=args.poll_interval,
            )
            if wait_info["success"]:
                found_count = len(wait_info.get("found", []))
                total_count = len(relayed_txids)
                logger.info(f"✓ Block processing validated: {found_count}/{total_count} transactions in mempool")
            else:
                logger.warning("✗ Block processing incomplete (transactions not in mempool)")
        else:
            logger.info("No transactions were relayed (block may only contain coinbase or all were already sent)")
        
        print(f"\nBlock relay stats: {stats}")
        db_conn.close()
        return
    
    # Sequential blocks mode (default/primary mode)
    if args.from_height is not None:
        logger.info("Starting sequential blocks mode")
        try:
            process_blocks_sequential(
                args.node1_rpc,
                args.node2_rpc,
                start_height=args.from_height,
                end_height=args.to_height,
                follow=bool(args.follow),
                wait_timeout=args.wait_timeout,
                poll_interval=args.poll_interval,
                strict=bool(args.strict),
                db_conn=db_conn,
                tx_delay=args.tx_delay,
                block_delay=args.block_delay,
            )
        finally:
            db_conn.close()
        return
    
    # Close database if we reach here
    db_conn.close()
    
    # No mode specified
    parser.print_help()
    print("\nError: Please specify --from-height for sequential processing or --block-height/--block-hash for single block")


if __name__ == "__main__":
    main()
