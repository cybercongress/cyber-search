
CREATE TABLE IF NOT exists ethereum.block (
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
    miner_contract_hash text,
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

CREATE TABLE IF NOT exists ethereum.tx (
    hash text PRIMARY KEY,
    nonce bigint,
    block_hash text,
    block_time timestamp,
    first_seen_time timestamp,
    block_number bigint,
    "from" text,
    "to" text,
    value text,
    gas_price decimal,
    gas_limit bigint,
    gas_used bigint,
    fee text,
    input text,
    created_contract text,
    trace_json text
);

CREATE TABLE IF NOT exists ethereum.tx_preview_by_contract (
    contract_hash text,
    hash text,
    block_time timestamp,
    first_seen_time timestamp,
    fee decimal,
    block_number bigint,
    "from" text,
    "to" text,
    value text,
    PRIMARY KEY (contract_hash, block_time, hash)
)  WITH CLUSTERING ORDER BY ( block_time DESC );

CREATE TABLE IF NOT exists ethereum.tx_preview_by_block (
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


CREATE TABLE IF NOT exists ethereum.contract_summary (
    hash text PRIMARY KEY,
    confirmed_balance text,
    smart_contract boolean,
    confirmed_total_received text,
    tx_number int,
    uncle_number int,
    mined_block_number int,
    first_activity_date timestamp,
    last_activity_date timestamp,
    version bigint,
    kafka_delta_offset bigint,
    kafka_delta_partition int,
    kafka_delta_topic text,
    kafka_delta_offset_committed boolean
);

CREATE TABLE IF NOT exists ethereum.mined_block_by_contract (
    miner_contract_hash text,
    block_number bigint,
    block_time timestamp,
    block_reward decimal,
    uncles_reward decimal,
    tx_fees decimal,
    tx_number int,
    PRIMARY KEY (miner_contract_hash, block_number)
)  WITH CLUSTERING ORDER BY ( block_number DESC );


CREATE TABLE IF NOT exists ethereum.uncle (
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


CREATE TABLE IF NOT exists ethereum.mined_uncle_by_contract (
     hash text,
     position int,
     number bigint,
     timestamp timestamp,
     block_number bigint,
     block_time timestamp,
     block_hash text,
     miner_contract_hash text,
     uncle_reward text,
     PRIMARY KEY (miner_contract_hash, block_number)
)  WITH CLUSTERING ORDER BY ( block_number DESC );