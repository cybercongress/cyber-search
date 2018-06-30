CREATE KEYSPACE ethereum WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};


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
    status text,
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
    trace_json blob
);

CREATE TABLE IF NOT exists ethereum.tx_preview_by_contract (
    contract_hash text,
    hash text,
    block_time bigint,
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
    successful_operation_number int,
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


INSERT INTO ethereum.block
(number, hash, parent_hash, nonce, timestamp, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root,
 miner_contract_hash, difficulty, total_difficulty, extra_data, size, gas_limit, gas_used, tx_number, tx_fees,
 block_reward, uncles_reward, uncles)
VALUES
(0, 'a', '', 0, '2017-04-01T11:21:59.001+0000', '', '', 'txroot', '', '', 'a', 0, 0, '', 0, 100, 100, 1, '0', '5', '5', []);


INSERT INTO ethereum.tx_preview_by_block
(block_number, position_in_block, fee, value, hash, "from", "to", creates_contract)
VALUES
(0, 0, 0, 10, 'txa', 'b', 'a', false);


INSERT INTO ethereum.contract_summary
(hash, confirmed_balance, smart_contract, confirmed_total_received, tx_number, successful_operation_number,
uncle_number, mined_block_number, first_activity_date, last_activity_date, version, kafka_delta_offset, kafka_delta_partition,
kafka_delta_topic, kafka_delta_offset_committed)
VALUES
('a', '10', false, '10', 1, 0, 0, 1, '2017-04-01T11:21:59.001+0000', '2017-04-01T11:21:59.001+0000', 0, 0, 0, 'tx_pump', true);


INSERT INTO ethereum.tx_preview_by_contract
(contract_hash, hash, block_time, first_seen_time, fee, block_number, "from", "to", value)
VALUES
('a', 'txa', 1230933600, '2017-04-01T11:21:59.001+0000', 0, 0, 'b', 'a', '10');


INSERT INTO ethereum.mined_block_by_contract
(miner_contract_hash, block_number, block_time, block_reward, uncles_reward, tx_fees, tx_number)
VALUES
('a', 0, '2017-04-01T11:21:59.001+0000', 5, 5, 0, 1);


INSERT INTO ethereum.mined_uncle_by_contract
(hash, position, number, timestamp, block_number, block_time, block_hash, miner_contract_hash, uncle_reward)
VALUES
('ua', 0, 0, '2017-04-01T11:21:59.001+0000', 0, '2017-04-01T11:21:59.001+0000', 'a', 'a', '5');


INSERT INTO ethereum.tx
(hash, status, nonce, block_hash, block_time, first_seen_time, block_number, "from", "to", value, gas_price, gas_limit,
gas_used, fee, input, created_contract)
VALUES
('txa', 'OK', 0, 'a', '2017-04-01T11:21:59.001+0000', '2017-04-01T11:21:59.001+0000', 0, 'b', 'a', '10', 0, 0, 0, '0', '', '');


INSERT INTO ethereum.uncle
(hash, position, number, timestamp, block_number, block_time, block_hash, miner, uncle_reward)
VALUES
('ua', 0, 0, '2017-04-01T11:21:59.001+0000', 0, '2017-04-01T11:21:59.001+0000', 'a', 'a', '5');
