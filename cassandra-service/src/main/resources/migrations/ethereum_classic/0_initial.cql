
CREATE TABLE IF NOT exists ethereum_classic.block (
    number bigint PRIMARY KEY,
    hash text,
    parent_hash text,
    nonce bigint,
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
    uncles_reward text,
    uncles FROZEN < list < text > >
);


CREATE TABLE IF NOT exists ethereum_classic.tx (
    hash text PRIMARY KEY,
    nonce bigint,
    block_hash text,
    block_time timestamp,
    block_number bigint,
    "from" text,
    "to" text,
    value text,
    gas_price decimal,
    gas_limit bigint,
    gas_used bigint,
    fee text,
    input text,
    created_contract text
);

CREATE TABLE IF NOT exists ethereum_classic.tx_preview_by_address (
    address text,
    hash text,
    block_time timestamp,
    fee decimal,
    block_number bigint,
    "from" text,
    "to" text,
    value text,
    PRIMARY KEY (address, block_time, hash)
)  WITH CLUSTERING ORDER BY ( block_time DESC );

CREATE TABLE IF NOT exists ethereum_classic.tx_preview_by_block (
    block_number bigint,
    position_in_block int,
    fee decimal,
    value decimal,
    hash text,
    "from" text,
    "to" text,
    creates_contract boolean,
    PRIMARY KEY ((block_number),position_in_block)
);

CREATE TABLE IF NOT exists ethereum_classic.address_summary (
    id text PRIMARY KEY,
    confirmed_balance decimal,
    contract_address boolean,
    confirmed_total_received decimal,
    tx_number int,
    uncle_number int,
    mined_block_number int,
    version bigint,
    kafka_delta_offset bigint,
    kafka_delta_partition int,
    kafka_delta_topic text,
    kafka_delta_offset_committed boolean
);

CREATE TABLE IF NOT exists ethereum_classic.mined_block_by_address (
    miner text,
    block_number bigint,
    block_time timestamp,
    block_reward decimal,
    uncles_reward decimal,
    tx_fees decimal,
    tx_number int,
    PRIMARY KEY (miner, block_number)
)  WITH CLUSTERING ORDER BY ( block_number DESC );

CREATE TABLE IF NOT exists ethereum_classic.uncle (
     hash text PRIMARY KEY,
     position int,
     number bigint,
     timestamp timestamp,
     block_number bigint,
     block_time timestamp,
     block_hash text,
     miner text,
     uncle_reward text
);


CREATE TABLE IF NOT exists ethereum_classic.mined_uncle_by_address (
     hash text,
     position int,
     number bigint,
     timestamp timestamp,
     block_number bigint,
     block_time timestamp,
     block_hash text,
     miner text,
     uncle_reward text,
     PRIMARY KEY (miner, block_number)
)  WITH CLUSTERING ORDER BY ( block_number DESC );