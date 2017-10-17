CREATE TYPE IF NOT EXISTS bitcoin.tx_preview_io (
    address text,
    amount text
);


CREATE TYPE IF NOT EXISTS bitcoin.block_tx (
    fee text,
    lock_time bigint,
    hash text,
    ins  FROZEN<list<bitcoin.tx_preview_io>>,
    outs FROZEN<list<bitcoin.tx_preview_io>>
);


CREATE TABLE IF NOT EXISTS bitcoin.block (
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
     difficulty varint,
     txs list<FROZEN <bitcoin.block_tx>>
);


CREATE TYPE IF NOT EXISTS bitcoin.tx_out (
    address text,
    amount text,
    asm text,
    out int,
    required_signatures int
);


CREATE TYPE IF NOT EXISTS bitcoin.tx_in (
    address text,
    amount text,
    asm text,
    tx_id text,
    tx_out int
);


CREATE TABLE IF NOT EXISTS bitcoin.tx (
     txid text PRIMARY KEY,
     block_number bigint,
     block_hash text,
     block_time timestamp,
     size int,
     coinbase text,
     fee text,
     total_input text,
     total_output text,
     ins FROZEN<list<bitcoin.tx_in>>,
     outs FROZEN<list<bitcoin.tx_out>>
);


CREATE TABLE IF NOT EXISTS bitcoin.tx_preview_by_address (
    address text,
    fee text,
    block_time timestamp,
    hash text,
    ins FROZEN<list<bitcoin.tx_preview_io>>,
    outs FROZEN<list<bitcoin.tx_preview_io>>,
    PRIMARY KEY (address,block_time,hash)
);


CREATE TABLE IF NOT EXISTS bitcoin.address (
    address text PRIMARY KEY,
    balance text,
    total_received text,
    tx_number int
);