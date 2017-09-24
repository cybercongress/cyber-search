CREATE KEYSPACE IF NOT EXISTS blockchains
    WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','LOCAL_DEVELOPMENT' : 1};


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_block_tx_io (
    address text,
    amount decimal
);


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_block_tx (
    fee decimal,
    lock_time timestamp,
    hash text,
    ins list<FROZEN <blockchains.bitcoin_block_tx_io>>,
    outs list<FROZEN <blockchains.bitcoin_block_tx_io>>
);


CREATE TABLE IF NOT EXISTS blockchains.bitcoin (
     hash text,
     height varint PRIMARY KEY,
     time timestamp,
     nonce int,
     merkleroot text,
     size int,
     version int,
     weight int,
     bits int,
     difficulty varint,
     txs list<FROZEN <blockchains.bitcoin_block_tx>>
);


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_tx_out (
    address text,
    amount decimal,
    asm text,
    out tinyint,
    required_signatures tinyint
);


CREATE TYPE IF NOT EXISTS blockchains.bitcoin_tx_in (
    address text,
    amount decimal,
    asm text,
    tx_id text,
    tx_out tinyint
);


CREATE TABLE IF NOT EXISTS blockchains.bitcoin_tx (
     hash text PRIMARY KEY,
     block_number varint,
     coinbase text,
     lock_time timestamp,
     fee decimal,
     total_input decimal,
     total_output decimal,
     ins list<FROZEN <blockchains.bitcoin_tx_in>>,
     outs list<FROZEN <blockchains.bitcoin_tx_out>>
);