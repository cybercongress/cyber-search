
CREATE TABLE IF NOT exists ethereum_classic.block (
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
    tx_fees text,
    block_reward text,
    uncles FROZEN < list < text > >
);


CREATE TABLE IF NOT exists ethereum_classic.tx (
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

CREATE TABLE IF NOT exists ethereum_classic.tx_preview_by_address (
    address text,
    fee text,
    block_time timestamp,
    block_number bigint,
    hash text,
    "from" text,
    "to" text,
    value text,
    PRIMARY KEY (address, block_time, hash)
);

CREATE TABLE IF NOT exists ethereum_classic.tx_preview_by_block (
    block_number bigint,
    fee decimal,
    amount decimal,
    hash text,
    "from" text,
    "to" text,
    creates_contract boolean,
    PRIMARY KEY (block_number, hash)
);

CREATE TABLE IF NOT exists ethereum_classic.address (
    id text PRIMARY KEY,
    balance text,
    contract_address boolean,
    total_received text,
    last_transaction_block bigint,
    tx_number int
);