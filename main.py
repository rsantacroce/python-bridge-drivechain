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
import sys
import time
import logging
import sqlite3
import requests
from datetime import datetime
from bitcoinrpc.authproxy import AuthServiceProxy
from urllib.parse import urlparse
from dotenv import load_dotenv
from http.client import RemoteDisconnected

# Load environment variables from .env file if it exists
load_dotenv()

# Configuration (can be overridden via CLI/env)
NODE1_RPC_DEFAULT = os.environ.get("NODE1_RPC", "http://user:password@127.0.0.1:8332")
NODE2_RPC_DEFAULT = os.environ.get("NODE2_RPC", "http://user:password@127.0.0.1:18301")
DB_PATH_DEFAULT = os.environ.get("DB_PATH", "tx_bridge.db")
TX_DELAY_DEFAULT = float(os.environ.get("TX_DELAY", "0.1"))  # Delay in seconds between sending transactions
BLOCK_DELAY_DEFAULT = float(os.environ.get("BLOCK_DELAY", "1.0"))  # Delay in seconds before processing next block
ELECTRUM_SERVER_URL_DEFAULT = os.environ.get("ELECTRUM_SERVER_URL", "http://localhost:3000")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s'
)
logger = logging.getLogger("bridge")


def rpc_call_with_retry(rpc_url: str, method: str, *args, max_retries: int = 3, retry_delay: float = 1.0, **kwargs):
    """Make an RPC call with retry logic for connection errors.
    
    This function handles connection errors (like RemoteDisconnected) by retrying
    with exponential backoff. On each retry, a new connection is created.
    
    Args:
        rpc_url: The RPC URL to connect to
        method: The RPC method name (e.g., 'getblockcount')
        *args: Positional arguments for the RPC method
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Initial delay between retries in seconds (default: 1.0)
        **kwargs: Keyword arguments for the RPC method
    
    Returns:
        The result of the RPC call
    
    Raises:
        Exception: If all retries are exhausted or a non-connection error occurs
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            # Create a new connection for each attempt to avoid stale connections
            proxy = AuthServiceProxy(rpc_url)
            # Call the method dynamically
            func = getattr(proxy, method)
            return func(*args, **kwargs)
        except (RemoteDisconnected, ConnectionError, OSError) as e:
            last_exception = e
            if attempt < max_retries:
                delay = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"RPC connection error ({e.__class__.__name__}) on attempt {attempt + 1}/{max_retries + 1} for {method}, retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                logger.error(f"RPC call {method} failed after {max_retries + 1} attempts: {e}")
                raise
        except Exception as e:
            # For non-connection errors, don't retry
            logger.debug(f"RPC call {method} failed with non-connection error: {e}")
            raise
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception


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
            error_message TEXT,
            confirmations INTEGER
        )
    """)
    # Add confirmations column if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE transactions ADD COLUMN confirmations INTEGER")
    except sqlite3.OperationalError:
        # Column already exists, ignore
        pass
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


def check_transaction_on_node2(node2_rpc_url: str, txid: str, electrum_server_url: str | None = None) -> dict | None:
    """Check if a transaction exists on node2 via electrum server.
    
    Uses the electrum server to check if a transaction exists in the blockchain.
    The electrum server URL can be configured via ELECTRUM_SERVER_URL environment variable.
    
    Returns dict with transaction info if found, None otherwise.
    """
    if electrum_server_url is None:
        electrum_server_url = ELECTRUM_SERVER_URL_DEFAULT
    
    electrum_url = f"{electrum_server_url.rstrip('/')}/tx/{txid}"
    
    try:
        response = requests.get(electrum_url, timeout=10)
        
        # If transaction not found (404), return None
        if response.status_code == 404:
            return None
        
        # Raise for other HTTP errors
        response.raise_for_status()
        
        # Parse JSON response
        tx_data = response.json()
        
        # Extract status information
        status = tx_data.get("status", {})
        confirmed = status.get("confirmed", False)
        block_height = status.get("block_height")
        block_hash = status.get("block_hash")
        
        # Calculate confirmations - if confirmed, set to at least 1
        # (In a full implementation, we'd need current block height to calculate exact confirmations)
        confirmations = 1 if confirmed else 0
        
        return {
            "txid": tx_data.get("txid", txid),
            "on_node2": True,
            "in_mempool": not confirmed,  # If not confirmed, assume in mempool
            "confirmed": confirmed,
            "confirmations": confirmations,
            "block_hash": block_hash,
            "block_height": block_height,
            "tx_index": None  # Electrum API doesn't provide tx_index
        }
        
    except requests.exceptions.RequestException as e:
        # Network errors, timeout, etc.
        logger.debug(f"Error checking transaction {txid} on electrum server: {e}")
        return None
    except (ValueError, KeyError) as e:
        # JSON parsing errors or missing expected fields
        logger.warning(f"Error parsing response for transaction {txid} from electrum server: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error checking transaction {txid} on electrum server: {e}")
        return None


def check_transaction_sent(conn: sqlite3.Connection, source_txid: str, node2_rpc_url: str | None = None) -> dict | None:
    """Check if a transaction has been successfully sent before.
    
    Performs two checks:
    1. Checks if the transaction is on node2 blockchain/mempool (CRITICAL - prevents duplicate sends)
    2. Checks if the transaction exists in the database and updates it with blockchain data
    
    Returns the most recent successful transaction record if found, None otherwise.
    Only considers transactions with status 'success' or 'already_in_mempool'.
    """
    # FIRST: Check if transaction is on node2 (most important check)
    if node2_rpc_url:
        node2_info = check_transaction_on_node2(node2_rpc_url, source_txid)
        if node2_info:
            # Transaction exists on node2 - don't send!
            logger.info(f"Transaction {source_txid} already exists on node2 (confirmed: {node2_info.get('confirmed', False)}, confirmations: {node2_info.get('confirmations', 0)})")
            
            # Check database for existing record
            cursor = conn.execute("""
                SELECT id, timestamp, source_txid, dest_txid, block_hash, block_height, 
                       tx_index, status, message, confirmations
                FROM transactions
                WHERE source_txid = ? 
                  AND status IN ('success', 'already_in_mempool', 'already_sent')
                ORDER BY timestamp DESC
                LIMIT 1
            """, (source_txid,))
            
            row = cursor.fetchone()
            if row:
                # Update existing record with latest node2 info
                record_id = row[0]
                update_data = []
                update_fields = []
                
                confirmations = node2_info.get("confirmations", 0)
                block_hash = node2_info.get("block_hash")
                block_height = node2_info.get("block_height")
                tx_index = node2_info.get("tx_index")
                
                if confirmations is not None:
                    update_fields.append("confirmations = ?")
                    update_data.append(confirmations)
                
                if block_hash:
                    update_fields.append("block_hash = ?")
                    update_data.append(block_hash)
                
                if block_height is not None:
                    update_fields.append("block_height = ?")
                    update_data.append(block_height)
                
                if tx_index is not None:
                    update_fields.append("tx_index = ?")
                    update_data.append(tx_index)
                
                if update_fields:
                    update_data.append(record_id)
                    conn.execute(f"""
                        UPDATE transactions
                        SET {', '.join(update_fields)}
                        WHERE id = ?
                    """, update_data)
                    conn.commit()
                
                # Return updated record
                return {
                    "id": row[0],
                    "timestamp": row[1],
                    "source_txid": row[2],
                    "dest_txid": row[3],
                    "block_hash": block_hash or row[4],
                    "block_height": block_height if block_height is not None else row[5],
                    "tx_index": tx_index if tx_index is not None else row[6],
                    "status": row[7],
                    "message": row[8],
                    "confirmations": confirmations if confirmations is not None else row[9],
                    "on_node2": True,
                }
            else:
                # Not in database but exists on node2 - return a synthetic record
                return {
                    "id": None,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source_txid": source_txid,
                    "dest_txid": source_txid,
                    "block_hash": node2_info.get("block_hash"),
                    "block_height": node2_info.get("block_height"),
                    "tx_index": node2_info.get("tx_index"),
                    "status": "already_on_node2",
                    "message": f"Transaction exists on node2 (not in our database yet)",
                    "confirmations": node2_info.get("confirmations", 0),
                    "on_node2": True,
                }
    
    # SECOND: Check database for historical record
    cursor = conn.execute("""
        SELECT id, timestamp, source_txid, dest_txid, block_hash, block_height, 
               tx_index, status, message, confirmations
        FROM transactions
        WHERE source_txid = ? 
          AND status IN ('success', 'already_in_mempool')
        ORDER BY timestamp DESC
        LIMIT 1
    """, (source_txid,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    # Build the record from database
    record = {
        "id": row[0],
        "timestamp": row[1],
        "source_txid": row[2],
        "dest_txid": row[3],
        "block_hash": row[4],
        "block_height": row[5],
        "tx_index": row[6],
        "status": row[7],
        "message": row[8],
        "confirmations": row[9],
    }
    
    # Update database with node2 blockchain info if available
    if node2_rpc_url:
        node2_info = check_transaction_on_node2(node2_rpc_url, record.get("dest_txid") or source_txid)
        if node2_info:
            update_data = []
            update_fields = []
            
            confirmations = node2_info.get("confirmations", 0)
            block_hash = node2_info.get("block_hash")
            block_height = node2_info.get("block_height")
            tx_index = node2_info.get("tx_index")
            
            if confirmations is not None:
                update_fields.append("confirmations = ?")
                update_data.append(confirmations)
            
            if block_hash:
                update_fields.append("block_hash = ?")
                update_data.append(block_hash)
            
            if block_height is not None:
                update_fields.append("block_height = ?")
                update_data.append(block_height)
            
            if tx_index is not None:
                update_fields.append("tx_index = ?")
                update_data.append(tx_index)
            
            if update_fields:
                update_data.append(record["id"])
                conn.execute(f"""
                    UPDATE transactions
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                """, update_data)
                conn.commit()
                
                # Update the record dict
                record["confirmations"] = confirmations
                if block_hash:
                    record["block_hash"] = block_hash
                if block_height is not None:
                    record["block_height"] = block_height
                if tx_index is not None:
                    record["tx_index"] = tx_index
                
                logger.debug(f"Updated transaction {source_txid} in database: confirmations={confirmations}, block_height={block_height}")
    
    return record


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
    
    # Helper function to parse boolean from environment variable
    def _get_bool_env(var_name: str, default: bool = False) -> bool:
        value = os.environ.get(var_name)
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes', 'on')
    
    # Helper function to get optional int from environment
    def _get_int_env(var_name: str) -> int | None:
        value = os.environ.get(var_name)
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None
    
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--block-height", type=int, 
                     default=_get_int_env("BLOCK_HEIGHT"),
                     help="Process a specific block height")
    mode.add_argument("--block-hash", type=str, 
                     default=os.environ.get("BLOCK_HASH"),
                     help="Process a specific block hash")
    
    parser.add_argument("--from-height", type=int, 
                       default=_get_int_env("FROM_HEIGHT"),
                       help="Start height for sequential block processing")
    parser.add_argument("--to-height", type=int, 
                       default=_get_int_env("TO_HEIGHT"),
                       help="End height for sequential block processing (inclusive)")
    parser.add_argument("--follow", action="store_true", 
                       help="Continue processing new blocks indefinitely")
    
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
        return {"total": 0, "relayed": 0, "failed": 0, "skipped_coinbase": 0, "already_on_node2": 0, "already_sent": 0, "confirmed_on_node2": 0, "in_mempool_on_node2": 0, "relayed_txids": []}
    
    if total_tx_count == 1:
        if block_tx_count and block_tx_count > 1:
            logger.error(f"CRITICAL: Block {block_hash} reports {block_tx_count} transactions but only 1 txid was extracted! Transactions are being lost!")
        else:
            logger.info(f"Block {block_hash} only contains coinbase transaction - skipping block")
            return {"total": 0, "relayed": 0, "failed": 0, "skipped_coinbase": 1, "already_on_node2": 0, "already_sent": 0, "confirmed_on_node2": 0, "in_mempool_on_node2": 0, "relayed_txids": [], "block_hash": block_hash, "block_height": block_height, "coinbase_only": True}
    else:
        logger.info(f"Will process {total_tx_count - 1} non-coinbase transaction(s)")
    
    # Skip coinbase (first tx in block)
    relayed = 0
    failed = 0
    skipped_coinbase = 0
    already_on_node2 = 0
    already_sent = 0
    total_candidates = 0
    relayed_txids: list[str] = []  # All transaction IDs (for tracking)
    newly_relayed_txids: list[str] = []  # Only transactions we actually sent (for mempool waiting)
    
    # Count how many transactions are already on node2
    confirmed_on_node2 = 0
    in_mempool_on_node2 = 0
    
    for idx, txid in enumerate(txids):
        if idx == 0:
            skipped_coinbase += 1
            logger.debug(f"Skipping coinbase tx {txid}")
            continue
        
        total_candidates += 1
        
        # Check if transaction exists on node2 blockchain/mempool OR in our database
        # This is the critical check - we MUST check node2 first to avoid duplicate sends
        prev_record = None
        if db_conn:
            prev_record = check_transaction_sent(db_conn, txid, node2_rpc_url)
        
        if prev_record:
            # Transaction exists on node2 or was previously sent
            already_on_node2 += 1
            
            # Check if it's confirmed on node2
            if prev_record.get("on_node2"):
                if prev_record.get("confirmations", 0) > 0:
                    confirmed_on_node2 += 1
                else:
                    in_mempool_on_node2 += 1
            
            prev_status = prev_record.get("status", "unknown")
            prev_confirmations = prev_record.get("confirmations", "unknown")
            prev_block = prev_record.get("block_height", "unknown")
            
            if prev_record.get("on_node2"):
                is_confirmed = prev_record.get("confirmations", 0) > 0
                message = f"Tx {txid} already on node2 (confirmed: {is_confirmed}, confirmations: {prev_confirmations}, block: {prev_block}), skipping send"
            else:
                message = f"Tx {txid} already sent before (status: {prev_status}, block: {prev_block}), skipping send"
            
            logger.info(message)
            # Add to relayed_txids for tracking, but NOT to newly_relayed_txids (already on node2)
            relayed_txids.append(prev_record.get("dest_txid") or txid)
            
            # Save a new record if we saw this tx again (allows tracking)
            if db_conn:
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
    
    # Log summary of transactions already on node2
    if already_on_node2 > 0:
        logger.info(f"Transactions already on node2: {already_on_node2} total ({confirmed_on_node2} confirmed, {in_mempool_on_node2} in mempool)")
    
    stats = {
        "total": total_candidates,
        "relayed": relayed,
        "failed": failed,
        "skipped_coinbase": skipped_coinbase,
        "already_on_node2": already_on_node2,
        "already_sent": already_sent,
        "confirmed_on_node2": confirmed_on_node2,
        "in_mempool_on_node2": in_mempool_on_node2,
        "block_hash": block_hash,
        "block_height": block_height,
        "relayed_txids": relayed_txids,
    }
    logger.info(f"Block relay complete: {relayed} relayed, {already_on_node2} already on node2 ({confirmed_on_node2} confirmed, {in_mempool_on_node2} in mempool), {failed} failed, {skipped_coinbase} skipped (coinbase)")
    return stats


def wait_for_block_creation(node2_rpc_url: str, block_hash: str, *, timeout_seconds: int, poll_interval_seconds: int) -> dict:
    """Poll node 2 until the block hash exists in its blockchain, or timeout.

    Returns a dict with success status and elapsed time.
    """
    start_ts = time.time()
    
    logger.info(f"Waiting for block {block_hash} to be created on node 2...")
    
    while True:
        try:
            # Try to get the block - if it exists, this will succeed
            # Use retry logic for connection errors, but let "block not found" errors pass through
            try:
                block_info = rpc_call_with_retry(node2_rpc_url, "getblock", block_hash, 1)
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
                    # Re-raise connection errors that weren't handled by retry logic
                    if isinstance(e, (RemoteDisconnected, ConnectionError, OSError)):
                        raise
                    logger.warning(f"Error checking block on node 2: {e}")
        except (RemoteDisconnected, ConnectionError, OSError) as e:
            # Connection error that retry logic couldn't handle - log and continue polling
            logger.warning(f"Connection error while checking block (will retry): {e}")
        
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
    
    start_ts = time.time()
    found: list[str] = []
    
    logger.info(f"Waiting for {len(candidate_txids)} transaction(s) to appear in node 2 mempool...")
    
    # Check if mempool is empty at the start
    try:
        initial_mempool = set(rpc_call_with_retry(node2_rpc_url, "getrawmempool"))
        if len(initial_mempool) == 0:
            logger.info(f"Node 2 mempool is empty - continuing to send more transactions from next block")
            return {"found": [], "elapsed": 0, "success": False, "mempool_empty": True}
    except Exception as e:
        logger.debug(f"Initial mempool check failed: {e}")
    
    while True:
        try:
            mempool_txids = set(rpc_call_with_retry(node2_rpc_url, "getrawmempool"))
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
    
    current = start_height
    blocks_processed = 0
    blocks_failed = 0
    
    logger.info(f"Starting sequential block processing from height {start_height}")
    if end_height is not None:
        logger.info(f"Will process up to height {end_height} (inclusive)")
    if follow:
        logger.info("Will follow new blocks indefinitely")
    
    while True:
        chain_height = rpc_call_with_retry(node1_rpc_url, "getblockcount")
        
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
    
    # Apply environment variable defaults for boolean flags if not set via CLI
    # For store_true actions, if the flag wasn't provided (False), check env vars
    # Helper function to parse boolean from environment variable
    def _get_bool_env(var_name: str) -> bool | None:
        value = os.environ.get(var_name)
        if value is None:
            return None
        return value.lower() in ('true', '1', 'yes', 'on')
    
    # Check environment variables for boolean flags
    # Note: With store_true, if the flag is True, it was set via CLI and we respect that.
    # If it's False, it might be default, so we check env vars.
    # We use sys.argv to check if the flag was explicitly provided (better than guessing)
    follow_provided = any('--follow' in arg for arg in sys.argv)
    strict_provided = any('--strict' in arg for arg in sys.argv)
    
    if not follow_provided:
        env_follow = _get_bool_env("FOLLOW")
        if env_follow is not None:
            args.follow = env_follow
    if not strict_provided:
        env_strict = _get_bool_env("STRICT")
        if env_strict is not None:
            args.strict = env_strict
    
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
