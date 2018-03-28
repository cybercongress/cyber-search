package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressTx
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTx
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import org.springframework.data.cassandra.core.cql.QueryOptions
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux


interface BitcoinTxRepository : ReactiveCrudRepository<CqlBitcoinTx, String>

interface BitcoinAddressTxRepository : ReactiveCrudRepository<CqlBitcoinAddressTx, MapId> {

    fun findAllByAddress(address: String, options: QueryOptions = QueryOptions.empty()): Flux<CqlBitcoinAddressTx>
}

interface BitcoinBlockTxRepository : ReactiveCassandraRepository<CqlBitcoinBlockTx, MapId> {

    fun findAllByBlockNumber(blockNumber: Long, options: QueryOptions = QueryOptions.empty()): Flux<CqlBitcoinBlockTx>
}
