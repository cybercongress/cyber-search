CREATE KEYSPACE IF NOT EXISTS blockchains
    WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','LOCAL_DEVELOPMENT' : 1};

CREATE TYPE IF NOT EXISTS blockchains.bitcoin_block_tx_io (
    address text,
    amount text
);


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_block_tx (
    fee text,
    lock_time bigint,
    hash text,
    ins list<FROZEN <blockchains.bitcoin_block_tx_io>>,
    outs list<FROZEN <blockchains.bitcoin_block_tx_io>>
);


CREATE TABLE IF NOT EXISTS blockchains.bitcoin_block (
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
     txs list<FROZEN <blockchains.bitcoin_block_tx>>
);


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_tx_out (
    address text,
    amount text,
    asm text,
    out int,
    required_signatures int
);


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_tx_in (
    address text,
    amount text,
    asm text,
    tx_id text,
    tx_out int
);


CREATE TABLE IF NOT EXISTS blockchains.bitcoin_tx (
     txid text PRIMARY KEY,
     block_number bigint,
     block_hash text,
     block_time timestamp,
     size int,
     coinbase text,
     lock_time bigint,
     fee text,
     total_input text,
     total_output text,
     ins list<FROZEN <blockchains.bitcoin_tx_in>>,
     outs list<FROZEN <blockchains.bitcoin_tx_out>>
);