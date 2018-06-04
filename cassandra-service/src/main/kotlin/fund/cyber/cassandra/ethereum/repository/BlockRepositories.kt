package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice

interface EthereumBlockRepository : RoutingReactiveCassandraRepository<CqlEthereumBlock, Long>

interface EthereumBlockTxRepository : RoutingReactiveCassandraRepository<CqlEthereumBlockTxPreview, MapId>

interface PageableEthereumBlockTxRepository : CassandraRepository<CqlEthereumBlockTxPreview, MapId> {
    fun findAllByBlockNumber(blockNumber: Long, page: Pageable): Slice<CqlEthereumBlockTxPreview>
}
