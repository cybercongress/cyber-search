package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.common.SearchRepository
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice


interface BitcoinTxRepository : SearchRepository<CqlBitcoinTx, String>

interface BitcoinBlockTxRepository : SearchRepository<CqlBitcoinBlockTxPreview, MapId>

interface PageableBitcoinBlockTxRepository : CassandraRepository<CqlBitcoinBlockTxPreview, MapId> {
    fun findAllByBlockNumber(blockNumber: Long, page: Pageable): Slice<CqlBitcoinBlockTxPreview>
}
