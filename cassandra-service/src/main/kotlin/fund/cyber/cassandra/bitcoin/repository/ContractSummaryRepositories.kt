package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.repository.reactive.ReactiveCrudRepository


interface BitcoinContractSummaryRepository : ReactiveCrudRepository<CqlBitcoinContractSummary, String>

interface BitcoinContractMinedBlockRepository: ReactiveCrudRepository<CqlBitcoinContractMinedBlock, String>

interface PageableBitcoinContractMinedBlockRepository: CassandraRepository<CqlBitcoinContractMinedBlock, MapId> {
    fun findAllByMinerContractHash(minerContractHash: String, page: Pageable): Slice<CqlBitcoinContractMinedBlock>
}

interface BitcoinContractTxRepository : ReactiveCrudRepository<CqlBitcoinContractTxPreview, MapId>

interface PageableBitcoinContractTxRepository : CassandraRepository<CqlBitcoinContractTxPreview, MapId> {
    fun findAllByContractHash(contractHash: String, page: Pageable): Slice<CqlBitcoinContractTxPreview>
}

