package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressTxPreview
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.repository.reactive.ReactiveCrudRepository

interface EthereumAddressRepository : ReactiveCrudRepository<CqlEthereumAddressSummary, String>

interface EthereumAddressTxRepository : ReactiveCrudRepository<CqlEthereumAddressTxPreview, MapId>

interface EthereumAddressMinedBlockRepository : ReactiveCrudRepository<CqlEthereumAddressMinedBlock, MapId>

interface EthereumAddressUncleRepository : ReactiveCrudRepository<CqlEthereumAddressMinedUncle, MapId>

interface PageableEthereumAddressTxRepository : CassandraRepository<CqlEthereumAddressTxPreview, MapId> {
    fun findAllByAddress(address: String, page: Pageable): Slice<CqlEthereumAddressTxPreview>
}

interface PageableEthereumAddressMinedBlockRepository : CassandraRepository<CqlEthereumAddressMinedBlock, MapId> {
    fun findAllByMiner(miner: String, page: Pageable): Slice<CqlEthereumAddressMinedBlock>
}

interface PageableEthereumAddressMinedUncleRepository : CassandraRepository<CqlEthereumAddressMinedUncle, MapId> {
    fun findAllByMiner(miner: String, page: Pageable): Slice<CqlEthereumAddressMinedUncle>
}
