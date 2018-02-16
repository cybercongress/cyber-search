
CREATE TYPE IF NOT EXISTS bitcoin.tx_preview_io (
    addresses FROZEN < list < text > >,
    amount text
);

CREATE TABLE IF NOT EXISTS bitcoin.tx_preview_by_address (
    address text,
    fee decimal,
    block_number bigint,
    block_time timestamp,
    hash text,
    ins FROZEN < list < bitcoin.tx_preview_io > >,
    outs FROZEN < list < bitcoin.tx_preview_io > >,
    PRIMARY KEY ( address, block_time, hash )
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



CREATE TYPE IF NOT EXISTS bitcoin.tx_out (
    addresses FROZEN < list < text > >,
    amount text,
    asm text,
    out int,
    required_signatures int
);

CREATE TYPE IF NOT EXISTS bitcoin.tx_in (
    addresses FROZEN < list < text > >,
    amount text,
    asm text,
    tx_id text,
    tx_out int
);

CREATE TABLE IF NOT EXISTS bitcoin.tx (
     hash text PRIMARY KEY,
     block_number bigint,
     block_hash text,
     block_time timestamp,
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



CREATE TABLE IF NOT EXISTS bitcoin.address (
    id text PRIMARY KEY,
    confirmed_balance text,
    confirmed_total_received decimal,
    confirmed_tx_number int,
    unconfirmed_tx_values map<text,decimal>
);