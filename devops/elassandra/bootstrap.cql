CREATE KEYSPACE IF NOT EXISTS blockchains
    WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','LOCAL_DEVELOPMENT' : 1};


CREATE TABLE IF NOT EXISTS blockchains.ethereum (
     hash text,
     parentHash text,
     height varint PRIMARY KEY,
     timestamp timestamp,
     nonce text,
     receiptsRoot text,
     stateRoot text,
     transactionsRoot text,
     logsBloom text,
     gasUsed int,
     gasLimit int,
     size int,
     difficulty int,
     totalDifficulty int,
     extraData text,
     sha3Uncles text,
     transactionsCount int,
     miner text,

     uncles list<FROZEN <blockchains.ethereum_block_uncle>>,
     txs list<FROZEN <blockchains.ethereum_block_tx>>
);

CREATE TABLE IF NOT EXISTS blockchains.ethereum_block_tx (
     hash text PRIMARY KEY,
     nonce int,
     blockHash text,
     blockNumber int,
     transactionIndex int,
     from text,
     to text,
     value int,
     gas int,
     gasPrice int,
     input text,
     v text,
     standardV text,
     r text,
     raw text,
     publicKey text,
     networkId text,
     creates text,
     condition text,
);
