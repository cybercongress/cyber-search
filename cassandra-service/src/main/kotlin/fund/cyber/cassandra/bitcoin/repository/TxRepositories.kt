package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import org.springframework.data.repository.reactive.ReactiveCrudRepository



interface BitcoinTxRepository : ReactiveCrudRepository<CqlBitcoinTx, String>