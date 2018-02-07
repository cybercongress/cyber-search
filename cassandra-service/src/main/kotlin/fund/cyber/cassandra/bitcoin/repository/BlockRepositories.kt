package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTx
import org.springframework.data.cassandra.core.cql.QueryOptions
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository
import reactor.core.publisher.Flux


interface BitcoinBlockRepository : ReactiveCassandraRepository<CqlBitcoinBlock, Long>


interface BitcoinBlockTxRepository : ReactiveCassandraRepository<CqlBitcoinBlockTx, MapId> {

    fun findAllByBlockNumber(blockNumber: Long, options: QueryOptions = QueryOptions.empty()): Flux<CqlBitcoinBlockTx>
}





