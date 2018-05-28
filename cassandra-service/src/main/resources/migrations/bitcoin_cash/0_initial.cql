
CREATE TYPE IF NOT EXISTS bitcoin_cash.tx_preview_io (
    contracts FROZEN < list < text > >,
    amount decimal
);

CREATE TABLE IF NOT EXISTS bitcoin_cash.tx_preview_by_contract (
    contract_hash text,
    fee decimal,
    block_number bigint,
    block_time bigint,
    hash text,
    ins FROZEN < list < bitcoin_cash.tx_preview_io > >,
    outs FROZEN < list < bitcoin_cash.tx_preview_io > >,
    PRIMARY KEY ( contract_hash, block_time, hash )
) WITH CLUSTERING ORDER BY ( block_time DESC );

CREATE TABLE IF NOT EXISTS bitcoin_cash.tx_preview_by_block (
    block_number bigint,
    "index" int,
    fee decimal,
    hash text,
    ins FROZEN < list < bitcoin_cash.tx_preview_io > >,
    outs FROZEN < list < bitcoin_cash.tx_preview_io > >,
    PRIMARY KEY ( block_number, "index" )
) WITH CLUSTERING ORDER BY ( "index" ASC );


CREATE TYPE IF NOT EXISTS bitcoin.script_sig (
    asm text,
    hex text
);

CREATE TYPE IF NOT EXISTS bitcoin_cash.tx_out (
    contracts FROZEN < list < text > >,
    amount decimal,
    asm text,
    out int,
    required_signatures int
);

CREATE TYPE IF NOT EXISTS bitcoin_cash.tx_in (
    contracts FROZEN < list < text > >,
    amount decimal,
    scriptSig FROZEN < bitcoin_cash.script_sig >,
    txinwitness FROZEN < list < text > >,
    tx_hash text,
    tx_out int
);

CREATE TABLE IF NOT EXISTS bitcoin_cash.tx (
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
     ins FROZEN < list < bitcoin_cash.tx_in > >,
     outs FROZEN < list < bitcoin_cash.tx_out > >
);



CREATE TABLE IF NOT EXISTS bitcoin_cash.block (
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

CREATE TABLE IF NOT exists bitcoin_cash.mined_block_by_contract (
    miner_contract_hash text,
    block_number bigint,
    block_time timestamp,
    block_reward decimal,
    tx_fees decimal,
    tx_number int,
    PRIMARY KEY (miner_contract_hash, block_number)
)  WITH CLUSTERING ORDER BY ( block_number DESC );


CREATE TABLE IF NOT EXISTS bitcoin_cash.contract_summary (
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