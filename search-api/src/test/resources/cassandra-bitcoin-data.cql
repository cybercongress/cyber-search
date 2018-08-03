CREATE KEYSPACE bitcoin WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};


CREATE TYPE IF NOT EXISTS bitcoin.tx_preview_io (
    contracts FROZEN < list < text > >,
    amount decimal
);

CREATE TABLE IF NOT EXISTS bitcoin.tx_preview_by_contract (
    contract_hash text,
    block_time bigint,
    hash text,
    PRIMARY KEY ( contract_hash, block_time, hash )
) WITH CLUSTERING ORDER BY ( block_time DESC );

CREATE TABLE IF NOT EXISTS bitcoin.tx_preview_by_block (
    block_number bigint,
    "index" int,
    fee decimal,
    hash text,
    ins FROZEN < list < bitcoin.tx_preview_io > >,
    outs FROZEN < list < bitcoin.tx_preview_io > >,
    PRIMARY KEY ( block_number, "index" )
) WITH CLUSTERING ORDER BY ( "index" ASC );

CREATE TYPE IF NOT EXISTS bitcoin.script_sig (
    asm text,
    hex text
);

CREATE TYPE IF NOT EXISTS bitcoin.tx_out (
    contracts FROZEN < list < text > >,
    amount decimal,
    asm text,
    out int,
    required_signatures int
);

CREATE TYPE IF NOT EXISTS bitcoin.tx_in (
    contracts FROZEN < list < text > >,
    amount decimal,
    scriptSig FROZEN < bitcoin.script_sig >,
    txinwitness FROZEN < list < text > >,
    tx_hash text,
    tx_out int
);

CREATE TABLE IF NOT EXISTS bitcoin.tx (
     hash text PRIMARY KEY,
     block_number bigint,
     block_hash text,
     block_time timestamp,
     first_seen_time timestamp,
     size int,
     coinbase text,
     fee text,
     total_input text,
     total_output text,
     ins FROZEN < list < bitcoin.tx_in > >,
     outs FROZEN < list < bitcoin.tx_out > >
);



CREATE TABLE IF NOT EXISTS bitcoin.block (
     hash text,
     parent_hash text,
     number bigint PRIMARY KEY,
     miner_contract_hash text,
     block_reward decimal,
     tx_fees decimal,
     coinbase_data text,
     timestamp timestamp,
     nonce bigint,
     merkleroot text,
     size int,
     version int,
     weight int,
     bits text,
     tx_number int,
     total_outputs_value text,
     difficulty varint
);

CREATE TABLE IF NOT exists bitcoin.mined_block_by_contract (
    miner_contract_hash text,
    block_number bigint,
    block_time timestamp,
    block_reward decimal,
    tx_fees decimal,
    tx_number int,
    PRIMARY KEY (miner_contract_hash, block_number)
)  WITH CLUSTERING ORDER BY ( block_number DESC );


CREATE TABLE IF NOT EXISTS bitcoin.contract_summary (
    hash text PRIMARY KEY,
    confirmed_balance text,
    confirmed_total_received text,
    confirmed_tx_number int,
    unconfirmed_tx_values map<text,decimal>,
    first_activity_date timestamp,
    last_activity_date timestamp,
    version bigint,
    kafka_delta_offset bigint,
    kafka_delta_partition int,
    kafka_delta_topic text,
    kafka_delta_offset_committed boolean
);


INSERT INTO bitcoin.block
(hash, parent_hash, number, miner_contract_hash, block_reward, tx_fees, coinbase_data,
 timestamp, nonce, merkleroot, size, version, weight, bits, tx_number, total_outputs_value, difficulty)
VALUES ('a', '', 0, 'a', 50, 0, '', '2017-04-01T11:21:59.001+0000', 0, 'a', 100, 0, 100, '', 1, '50', 1);


INSERT INTO bitcoin.tx_preview_by_block
(block_number, "index", fee, hash, ins, outs)
VALUES
(0, 0, 0, 'txa', [], [{contracts: ['a'], amount: 50}]);


INSERT INTO bitcoin.contract_summary
(hash, confirmed_balance, confirmed_total_received, confirmed_tx_number, unconfirmed_tx_values, first_activity_date,
 last_activity_date, version, kafka_delta_offset, kafka_delta_partition, kafka_delta_topic, kafka_delta_offset_committed)
VALUES
('a', '50', '50', 1, {}, '2017-04-01T11:21:59.001+0000', '2017-04-01T11:21:59.001+0000', 0, 0, 0, 'tx-pump', true);


INSERT INTO bitcoin.tx_preview_by_contract
(contract_hash, block_time, hash)
VALUES ('a', 1231006505, 'txa');

INSERT INTO bitcoin.tx_preview_by_contract
(contract_hash, block_time, hash)
VALUES ('a', 1233006505, 'txb');

INSERT INTO bitcoin.tx_preview_by_contract
(contract_hash, block_time, hash)
VALUES ('a', 1232006505, 'txc');

INSERT INTO bitcoin.tx_preview_by_contract
(contract_hash, block_time, hash)
VALUES ('a', 1232007505, 'txm');

INSERT INTO bitcoin.tx_preview_by_contract
(contract_hash, block_time, hash)
VALUES ('a', 1232008505, 'txm2');


INSERT INTO bitcoin.mined_block_by_contract
(miner_contract_hash, block_number, block_time, block_reward, tx_fees, tx_number)
VALUES ('a', 0, '2017-04-01T11:21:59.001+0000', 50, 0, 1);


INSERT INTO bitcoin.tx
(hash, block_number, block_hash, block_time, first_seen_time, size, coinbase, fee, total_input, total_output, ins, outs)
VALUES
('txa', 0, 'a', '2017-04-01T11:21:59.001+0000', '2017-04-01T11:21:59.001+0000', 0, '', '0', '0', '50', [], [{contracts: ['a'], amount: 50, asm: '', out: 0, required_signatures: 1}]);

INSERT INTO bitcoin.tx
(hash, block_number, block_hash, block_time, first_seen_time, size, coinbase, fee, total_input, total_output, ins, outs)
VALUES
('txb', 100, 'b', '2017-04-01T11:21:59.001+0000', '2017-04-01T11:21:59.001+0000', 0, '', '0', '0', '50', [], [{contracts: ['a'], amount: 50, asm: '', out: 0, required_signatures: 1}]);

INSERT INTO bitcoin.tx
(hash, block_number, block_hash, block_time, first_seen_time, size, coinbase, fee, total_input, total_output, ins, outs)
VALUES
('txc', 53, 'c', '2017-04-01T11:21:59.001+0000', '2017-04-01T11:21:59.001+0000', 0, '', '0', '0', '50', [], [{contracts: ['a'], amount: 50, asm: '', out: 0, required_signatures: 1}]);

INSERT INTO bitcoin.tx
(hash, block_number, block_hash, block_time, first_seen_time, size, coinbase, fee, total_input, total_output, ins, outs)
VALUES
('txm', -1, '', null, '2017-06-01T11:21:59.001+0000', 0, '', '0', '0', '50', [], [{contracts: ['a'], amount: 50, asm: '', out: 0, required_signatures: 1}]);

INSERT INTO bitcoin.tx
(hash, block_number, block_hash, block_time, first_seen_time, size, coinbase, fee, total_input, total_output, ins, outs)
VALUES
('txm2', -1, '', null, '2017-06-02T11:21:59.001+0000', 0, '', '0', '0', '50', [], [{contracts: ['a'], amount: 50, asm: '', out: 0, required_signatures: 1}]);
