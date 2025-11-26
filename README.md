# Python TX Bridge (Bitcoin Node 1 -> Node 2)

Relays Bitcoin transactions from one Bitcoin node instance to another using JSON-RPC.

This script fetches blocks from Bitcoin node instance 1, skips coinbase transactions, and relays the remaining transactions to Bitcoin node instance 2. It enforces strict block processing rules: **it will not proceed to the next block until at least one relayed transaction appears in node 2's mempool**.

## Prerequisites
- Python 3.9+
- Bitcoin Core node instance 1 (source) with RPC enabled
- Bitcoin Core node instance 2 (destination) with RPC enabled

Example bitcoin.conf for both nodes:
```
server=1
rpcuser=user
rpcpassword=password
```

## Install
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configuration
Configure via CLI or environment variables.

Environment variables (optional):
- `NODE1_RPC` (default: `http://user:password@127.0.0.1:8332`) - Source Bitcoin node
- `NODE2_RPC` (default: `http://user:password@127.0.0.1:18332`) - Destination Bitcoin node
- `LOG_LEVEL` (default: `INFO`)
- `WAIT_TIMEOUT` (default: 600 seconds) - Time to wait for transactions to appear in mempool
- `POLL_INTERVAL` (default: 5 seconds) - How often to check mempool

CLI flags override environment variables:
- `--node1-rpc URL` - Source node RPC URL
- `--node2-rpc URL` - Destination node RPC URL
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`
- `--wait-timeout SECONDS` - Mempool wait timeout
- `--poll-interval SECONDS` - Mempool polling interval
- `--strict` - Stop processing if transactions don't appear in mempool (default: continue with warning)

## Usage
All commands assume you are in the project root and the venv is activated.

### Sequential blocks mode (primary mode)
Process blocks sequentially with mempool validation. The script will not proceed to the next block until relayed transactions appear in node 2's mempool.

```
# Process a range of blocks
./main.py --from-height 850000 --to-height 850100

# Start at a height and follow new blocks indefinitely
./main.py --from-height 850000 --follow

# Tune wait timeout and polling interval
./main.py --from-height 850000 --wait-timeout 300 --poll-interval 3

# Enable strict mode (stop if transactions don't appear in mempool)
./main.py --from-height 850000 --strict
```

### Single block mode
Process a single block and validate that transactions appear in mempool:

```
# By height
./main.py --block-height 850000

# By hash
./main.py --block-hash 0000000000000000000abcdef0123456789...
```

## Block Processing Rules

The script enforces the following rules:

1. **Coinbase transactions are skipped** (first transaction in each block)
2. **Mempool validation**: Before proceeding to the next block, the script waits (up to `--wait-timeout` seconds) for at least one relayed transaction to appear in node 2's mempool
3. **Strict mode**: If `--strict` is enabled and transactions don't appear in mempool within the timeout, processing stops. Otherwise, a warning is logged and processing continues

## Notes
- Coinbase transactions are always ignored
- The script polls node 2's mempool to ensure transactions were successfully relayed
- If no transactions were relayed from a block (e.g., only coinbase), the script proceeds immediately to the next block
- Use `--log-level DEBUG` for detailed trace logging
- ZMQ functionality will be added in a future version
