#!/usr/bin/env python3
"""
Bridge for Drivechain mainnet to fork

This script supports two modes:
- Stream mode (default): subscribe to ZMQ and relay incoming raw transactions to the fork
- Block mode: fetch a specific block via RPC, skip coinbase, and relay its transactions to the fork

Examples:
- Stream: ./main.py
- Block by height: ./main.py --block-height 850000
- Block by hash: ./main.py --block-hash 0000000000000000000abcdef...
"""
import argparse
import os
import sys
import time
import zmq
import logging
from bitcoinrpc.authproxy import AuthServiceProxy

# Your mainnet configuration (can be overridden via CLI/env)
MAINNET_ZMQ_DEFAULT = os.environ.get("MAINNET_ZMQ", "tcp://127.0.0.1:29000")
MAINNET_RPC_DEFAULT = os.environ.get("MAINNET_RPC", "http://user:passwordDC@127.0.0.1:8332")  # Default Bitcoin RPC port

# Your fork configuration (update when ready) (can be overridden via CLI/env)
FORK_RPC_DEFAULT = os.environ.get("FORK_RPC", "http://user:forkpassword@127.0.0.1:18332")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s'
)
logger = logging.getLogger("bridge")


def _build_arg_parser():
    parser = argparse.ArgumentParser(description="Drivechain to Fork Bridge")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--block-height", type=int, help="Process a specific block height from mainnet RPC")
    mode.add_argument("--block-hash", type=str, help="Process a specific block hash from mainnet RPC")

    # Sequential block processing
    parser.add_argument("--from-height", type=int, help="Start height for sequential block processing")
    parser.add_argument("--to-height", type=int, help="End height for sequential block processing (inclusive)")
    parser.add_argument("--follow", action="store_true", help="Continue processing new blocks indefinitely")
    parser.add_argument("--wait-timeout", type=int, default=int(os.environ.get("WAIT_TIMEOUT", 600)), help="Seconds to wait for fork mempool to receive relayed txs before next block")
    parser.add_argument("--poll-interval", type=int, default=int(os.environ.get("POLL_INTERVAL", 5)), help="Seconds between mempool checks while waiting")

    parser.add_argument("--mainnet-rpc", default=MAINNET_RPC_DEFAULT, help="Mainnet RPC URL")
    parser.add_argument("--mainnet-zmq", default=MAINNET_ZMQ_DEFAULT, help="Mainnet ZMQ endpoint")
    parser.add_argument("--fork-rpc", default=FORK_RPC_DEFAULT, help="Fork RPC URL")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"), choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
    return parser

def test_connections(mainnet_rpc_url: str, fork_rpc_url: str, mainnet_zmq_url: str) -> bool:
    """Test that everything is accessible"""
    print("Testing connections...")
    
    # Test mainnet RPC
    try:
        mainnet = AuthServiceProxy(mainnet_rpc_url)
        height = mainnet.getblockcount()
        print(f"✓ Mainnet RPC working - Height: {height}")
    except Exception as e:
        print(f"✗ Mainnet RPC failed: {e}")
        return False
    
    # Test fork RPC (will fail if not running yet)
    try:
        fork = AuthServiceProxy(fork_rpc_url)
        fork_height = fork.getblockcount()
        print(f"✓ Fork RPC working - Height: {fork_height}")
    except Exception as e:
        print(f"⚠ Fork RPC not available yet: {e}")
    
    # Test ZMQ
    try:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(mainnet_zmq_url)
        socket.setsockopt(zmq.SUBSCRIBE, b'')
        socket.setsockopt(zmq.RCVTIMEO, 2000)
        print(f"✓ ZMQ connected to {mainnet_zmq_url}")
        
        try:
            msg = socket.recv_multipart()
            print(f"✓ ZMQ receiving messages: {msg[0].decode()}")
        except zmq.error.Again:
            print("⚠ ZMQ connected but no messages (blockchain idle)")
        
        socket.close()
        context.term()
        
    except Exception as e:
        print(f"✗ ZMQ connection failed: {e}")
        return False
    
    print("\n✓ All connections successful!")
    return True


def relay_block_transactions(mainnet_rpc_url: str, fork_rpc_url: str, *, block_height: int | None = None, block_hash: str | None = None) -> dict:
    """Fetch a block from mainnet RPC, skip coinbase, and relay txs to the fork.

    Returns stats dict.
    """
    if not block_height and not block_hash:
        raise ValueError("Either block_height or block_hash must be provided")

    mainnet = AuthServiceProxy(mainnet_rpc_url)
    fork = AuthServiceProxy(fork_rpc_url)

    if block_height is not None:
        logger.info(f"Resolving block hash for height {block_height}")
        block_hash = mainnet.getblockhash(block_height)

    logger.info(f"Fetching block {block_hash}")
    block = mainnet.getblock(block_hash, 1)  # verbosity=1 returns txids
    txids = block.get("tx", [])
    if not txids:
        logger.warning("Block has no transactions")
        return {"total": 0, "relayed": 0, "failed": 0, "skipped_coinbase": 0}

    # Skip coinbase (first tx in block)
    relayed = 0
    failed = 0
    skipped_coinbase = 0
    total_candidates = 0
    relayed_txids: list[str] = []

    for idx, txid in enumerate(txids):
        if idx == 0:
            skipped_coinbase += 1
            logger.debug(f"Skipping coinbase tx {txid}")
            continue
        total_candidates += 1
        try:
            # Obtain raw hex; include block hash for pruned nodes
            raw_hex = mainnet.getrawtransaction(txid, False, block_hash)
            sent_txid = fork.sendrawtransaction(raw_hex)
            relayed += 1
            relayed_txids.append(sent_txid if isinstance(sent_txid, str) else txid)
            logger.info(f"Relayed tx {txid} -> fork txid {sent_txid}")
        except Exception as e:
            failed += 1
            logger.error(f"Failed to relay tx {txid}: {e}")

    stats = {
        "total": total_candidates,
        "relayed": relayed,
        "failed": failed,
        "skipped_coinbase": skipped_coinbase,
        "block_hash": block_hash,
        "relayed_txids": relayed_txids,
    }
    logger.info(f"Block relay complete: {stats}")
    return stats


def wait_for_mempool_presence(fork_rpc_url: str, candidate_txids: list[str], *, timeout_seconds: int, poll_interval_seconds: int) -> dict:
    """Poll the fork mempool until at least one candidate txid is present, or timeout.

    Returns a dict with result info.
    """
    if not candidate_txids:
        return {"found": [], "elapsed": 0}

    fork = AuthServiceProxy(fork_rpc_url)
    start_ts = time.time()
    found: list[str] = []

    while True:
        try:
            mempool_txids = set(fork.getrawmempool())
        except Exception as e:
            logger.warning(f"Failed to query fork mempool: {e}")
            mempool_txids = set()

        found = [txid for txid in candidate_txids if txid in mempool_txids]
        if found:
            elapsed = int(time.time() - start_ts)
            logger.info(f"Mempool contains {len(found)} relayed tx(s) after {elapsed}s")
            return {"found": found, "elapsed": elapsed}

        elapsed = time.time() - start_ts
        if elapsed >= timeout_seconds:
            logger.warning(f"Timeout {timeout_seconds}s waiting for any relayed tx to appear in mempool")
            return {"found": [], "elapsed": int(elapsed)}

        time.sleep(poll_interval_seconds)


def process_blocks_sequential(mainnet_rpc_url: str, fork_rpc_url: str, *, start_height: int, end_height: int | None, follow: bool, wait_timeout: int, poll_interval: int) -> None:
    mainnet = AuthServiceProxy(mainnet_rpc_url)

    current = start_height
    while True:
        chain_height = mainnet.getblockcount()
        if end_height is not None and current > end_height:
            logger.info("Reached end height; stopping")
            break

        if current > chain_height:
            if follow:
                logger.info("Waiting for new blocks...")
                time.sleep(max(1, poll_interval))
                continue
            else:
                logger.info("Start height beyond current chain tip; stopping")
                break

        logger.info(f"Processing block at height {current}")
        stats = relay_block_transactions(mainnet_rpc_url, fork_rpc_url, block_height=current)
        relayed_txids = stats.get("relayed_txids", [])

        # Wait for mempool signal before next block
        wait_info = wait_for_mempool_presence(
            fork_rpc_url,
            relayed_txids,
            timeout_seconds=wait_timeout,
            poll_interval_seconds=poll_interval,
        )
        logger.info(f"Post-block mempool check: {wait_info}")

        current += 1
        if end_height is None and not follow and current > start_height:
            # Processed a single block in sequential mode without follow/endpoint
            break

def main():
    parser = _build_arg_parser()
    args = parser.parse_args()

    # Configure logging level early
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    print("=" * 60)
    print("Drivechain to Fork Bridge")
    print("=" * 60)

    if not test_connections(args.mainnet_rpc, args.fork_rpc, args.mainnet_zmq):
        print("\nPlease fix connection issues before starting bridge")
        return

    # Single block mode
    if args.block_height is not None or args.block_hash is not None:
        logger.info("Starting block relay mode")
        stats = relay_block_transactions(
            args.mainnet_rpc,
            args.fork_rpc,
            block_height=args.block_height,
            block_hash=args.block_hash,
        )
        # Post-block mempool wait even for single block mode
        relayed_txids = stats.get("relayed_txids", [])
        wait_info = wait_for_mempool_presence(
            args.fork_rpc,
            relayed_txids,
            timeout_seconds=args.wait_timeout,
            poll_interval_seconds=args.poll_interval,
        )
        print(f"Block relay stats: {stats}")
        print(f"Mempool wait result: {wait_info}")
        return

    # Sequential blocks mode
    if args.from_height is not None:
        logger.info("Starting sequential blocks mode")
        process_blocks_sequential(
            args.mainnet_rpc,
            args.fork_rpc,
            start_height=args.from_height,
            end_height=args.to_height,
            follow=bool(args.follow),
            wait_timeout=args.wait_timeout,
            poll_interval=args.poll_interval,
        )
        return

    # Stream/ZMQ mode (default)
    logger.info("Starting stream mode (ZMQ -> fork)")
    fork = AuthServiceProxy(args.fork_rpc)
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(args.mainnet_zmq)

    # Subscribe to all topics to see what's available
    socket.setsockopt(zmq.SUBSCRIBE, b'')

    print("\nListening for all ZMQ topics...")
    print("Press Ctrl+C to stop\n")

    stats = {'total': 0, 'rawtx': 0, 'relayed': 0, 'failed': 0}

    try:
        while True:
            msg = socket.recv_multipart()
            topic = msg[0].decode()
            body = msg[1]

            stats['total'] += 1

            logger.info(f"[{topic}] Received ({len(body)} bytes)")

            if topic == "rawtx":
                stats['rawtx'] += 1
                raw_tx = body.hex()

                try:
                    txid = fork.sendrawtransaction(raw_tx)
                    stats['relayed'] += 1
                    logger.info(f"Relayed TX: {txid}")
                except Exception as e:
                    stats['failed'] += 1
                    logger.debug(f"Relay failed: {str(e)[:200]}")

            # Print stats every 10 messages
            if stats['total'] % 10 == 0:
                logger.info(f"Stats: {stats}")

    except KeyboardInterrupt:
        print("\n\nShutting down...")
        print(f"Final stats: {stats}")

    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
