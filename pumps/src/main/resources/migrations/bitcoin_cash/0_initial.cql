
CREATE TYPE IF NOT EXISTS bitcoin_cash.tx_preview_io (
    addresses FROZEN < list < text > >,
    amount text
);

CREATE TABLE IF NOT EXISTS bitcoin_cash.tx_preview_by_address (
    address text,
    fee decimal,
    block_number bigint,
    block_time timestamp,
    hash text,
    ins FROZEN < list < bitcoin_cash.tx_preview_io > >,
    outs FROZEN < list < bitcoin_cash.tx_preview_io > >,
    PRIMARY KEY ( address, block_time, hash )
) WITH CLUSTERING ORDER BY ( block_time DESC );

CREATE TABLE IF NOT EXISTS bitcoin_cash.tx_preview_by_block (
    block_number bigint,
    "index" int,
    fee decimal,
    hash text,
    ins FROZEN < list < bitcoin_cash.tx_preview_io > >,
    outs FROZEN < list < bitcoin_cash.tx_preview_io > >,
    PRIMARY KEY ( block_number, hash )
);



CREATE TYPE IF NOT EXISTS bitcoin_cash.tx_out (
    addresses FROZEN < list < text > >,
    amount text,
    asm text,
    out int,
    required_signatures int
);

CREATE TYPE IF NOT EXISTS bitcoin_cash.tx_in (
    addresses FROZEN < list < text > >,
    amount text,
    asm text,
    tx_id text,
    tx_out int
);

CREATE TABLE IF NOT EXISTS bitcoin_cash.tx (
     hash text PRIMARY KEY,
     block_number bigint,
     block_hash text,
     block_time timestamp,
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
     height bigint PRIMARY KEY,
     time timestamp,
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



CREATE TABLE IF NOT EXISTS bitcoin_cash.address (
    id text PRIMARY KEY,
    balance text,
    total_received text,
    last_transaction_block bigint,
    tx_number int
);