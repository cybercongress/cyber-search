package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice


interface BitcoinTxRepository : RoutingReactiveCassandraRepository<CqlBitcoinTx, String>

interface BitcoinBlockTxRepository : RoutingReactiveCassandraRepository<CqlBitcoinBlockTxPreview, MapId>

interface PageableBitcoinBlockTxRepository : CassandraRepository<CqlBitcoinBlockTxPreview, MapId> {
    fun findAllByBlockNumber(blockNumber: Long, page: Pageable): Slice<CqlBitcoinBlockTxPreview>
}
