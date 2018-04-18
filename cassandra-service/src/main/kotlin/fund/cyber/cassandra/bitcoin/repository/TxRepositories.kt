package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.repository.reactive.ReactiveCrudRepository


interface BitcoinTxRepository : ReactiveCrudRepository<CqlBitcoinTx, String>

interface BitcoinBlockTxRepository : ReactiveCassandraRepository<CqlBitcoinBlockTxPreview, MapId>

interface PageableBitcoinBlockTxRepository : CassandraRepository<CqlBitcoinBlockTxPreview, MapId> {
    fun findAllByBlockNumber(blockNumber: Long, page: Pageable): Slice<CqlBitcoinBlockTxPreview>
}
