# Python TX Bridge (Drivechain to Fork)

Relays Bitcoin mainnet (Drivechain) transactions to a fork chain.

- Stream mode: subscribe to ZMQ and forward incoming rawtx to the fork
- Block mode: fetch a specific block, skip coinbase, relay remaining txs
- Sequential mode: process blocks from a starting height (optional end height or follow tip), waiting for relayed txs to appear in the fork mempool before continuing

## Prerequisites
- Python 3.9+
- Bitcoin Core (source/mainnet side) with RPC and ZMQ enabled
- Fork node with RPC enabled

Example bitcoin.conf (source/mainnet side):
```
server=1
rpcuser=user
rpcpassword=passwordDC
# ZMQ publisher sockets
zmqpubrawtx=tcp://127.0.0.1:29000
```

Fork node should have RPC enabled similarly:
```
server=1
rpcuser=user
rpcpassword=forkpassword
```

## Install
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration
Configure via CLI or environment variables.

Environment variables (optional):
- MAINNET_RPC (default: http://user:passwordDC@127.0.0.1:8332)
- MAINNET_ZMQ (default: tcp://127.0.0.1:29000)
- FORK_RPC (default: http://user:forkpassword@127.0.0.1:18332)
- LOG_LEVEL (default: INFO)
- WAIT_TIMEOUT (default: 600 seconds)
- POLL_INTERVAL (default: 5 seconds)

CLI flags override envs:
- --mainnet-rpc URL
- --mainnet-zmq ADDR
- --fork-rpc URL
- --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}

## Usage
All commands assume you are in the project root and the venv is activated.

### Stream mode (default)
Relays incoming raw transactions from ZMQ to the fork.
```
./main.py
```

### Single block mode
Fetch a block from mainnet RPC, skip coinbase, relay remaining txs. Then wait (up to timeout) for at least one relayed tx to appear in the fork mempool.
```
# By height
./main.py --block-height 850000

# By hash
./main.py --block-hash 0000000000000000000abcdef0123456789...
```

### Sequential blocks mode
Process a range or follow new blocks indefinitely. After each block, the tool polls the fork mempool until any relayed tx appears or the timeout elapses before proceeding.
```
# Fixed range
./main.py --from-height 850000 --to-height 850010

# Start and follow new blocks forever
./main.py --from-height 850000 --follow

# Tune wait window and polling cadence
./main.py --from-height 850000 --wait-timeout 600 --poll-interval 5
```

## Notes
- Coinbase transactions are ignored.
- If no relayed tx is observed in the fork mempool within the timeout window, processing continues to the next block with a warning.
- Use --log-level DEBUG for detailed trace logging.

