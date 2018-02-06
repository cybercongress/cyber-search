package cyber.search.bitcoin.repository

import cyber.search.bitcoin.model.BitcoinBlock
import cyber.search.bitcoin.model.BitcoinBlockTransaction
import org.springframework.data.cassandra.core.cql.QueryOptions
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository
import reactor.core.publisher.Flux



interface BitcoinBlockRepository : ReactiveCassandraRepository<BitcoinBlock, Long>


interface BitcoinBlockTxRepository : ReactiveCassandraRepository<BitcoinBlockTransaction, MapId> {

    fun findAllByBlockNumber(blockNumber: Long, options: QueryOptions = QueryOptions.empty()): Flux<BitcoinBlockTransaction>
}





