CREATE TYPE IF NOT EXISTS blockchains.ethereum_block_tx (
    fee decimal,
    amount decimal,
    hash text,
    "from" text,
    "to" text,
);


CREATE TABLE IF NOT EXISTS blockchains.ethereum_block (
     number bigint PRIMARY KEY,
     hash text,
     parent_hash text,
     timestamp timestamp,
     sha3_uncles text,
     logs_bloom text,
     transactions_root text,
     state_root text,
     receipts_root text,
     miner text,
     difficulty varint,
     total_difficulty varint,
     extra_data text,
     size bigint,
     gas_limit bigint,
     gas_used bigint,
     tx_number int,
     transactions FROZEN<list<ethereum_block_tx>>,
     uncles FROZEN<list<text>>
);


CREATE TABLE IF NOT EXISTS blockchains.ethereum_tx (
     hash text PRIMARY KEY,
     nonce bigint,
     block_hash text,
     timestamp timestamp,
     block_number bigint,
     transaction_index bigint,
     "from" text,
     "to" text,
     value text,
     gas_price decimal,
     gas_limit bigint,
     gas_used bigint,
     fee text,
     input text,
     creates text
);