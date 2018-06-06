package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import reactor.core.publisher.Flux


interface BitcoinContractSummaryRepository : RoutingReactiveCassandraRepository<CqlBitcoinContractSummary, String>

interface BitcoinContractMinedBlockRepository: RoutingReactiveCassandraRepository<CqlBitcoinContractMinedBlock, String>

interface PageableBitcoinContractMinedBlockRepository: CassandraRepository<CqlBitcoinContractMinedBlock, MapId> {
    fun findAllByMinerContractHash(minerContractHash: String, page: Pageable): Slice<CqlBitcoinContractMinedBlock>
}

interface BitcoinContractTxRepository : RoutingReactiveCassandraRepository<CqlBitcoinContractTxPreview, MapId> {
    fun findAllByContractHashAndBlockTime(contractHash: String, blockTime: Long): Flux<CqlBitcoinContractTxPreview>
}

interface PageableBitcoinContractTxRepository : CassandraRepository<CqlBitcoinContractTxPreview, MapId> {
    fun findAllByContractHash(contractHash: String, page: Pageable): Slice<CqlBitcoinContractTxPreview>
}

