package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import reactor.core.publisher.Flux

interface EthereumContractRepository : RoutingReactiveCassandraRepository<CqlEthereumContractSummary, String>

interface EthereumContractTxRepository : RoutingReactiveCassandraRepository<CqlEthereumContractTxPreview, MapId>{
    fun findAllByContractHashAndBlockTime(contractHash: String, blockTime: Long): Flux<CqlEthereumContractTxPreview>
}

interface EthereumContractMinedBlockRepository
    : RoutingReactiveCassandraRepository<CqlEthereumContractMinedBlock, MapId>

interface EthereumContractUncleRepository : RoutingReactiveCassandraRepository<CqlEthereumContractMinedUncle, MapId>

interface PageableEthereumContractTxRepository : CassandraRepository<CqlEthereumContractTxPreview, MapId> {
    fun findAllByContractHash(contractHash: String, page: Pageable): Slice<CqlEthereumContractTxPreview>
}

interface PageableEthereumContractMinedBlockRepository : CassandraRepository<CqlEthereumContractMinedBlock, MapId> {
    fun findAllByMinerContractHash(minerContractHash: String, page: Pageable): Slice<CqlEthereumContractMinedBlock>
}

interface PageableEthereumContractMinedUncleRepository : CassandraRepository<CqlEthereumContractMinedUncle, MapId> {
    fun findAllByMinerContractHash(minerContractHash: String, page: Pageable): Slice<CqlEthereumContractMinedUncle>
}
