package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressMinedBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressTxPreview
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.repository.reactive.ReactiveCrudRepository


interface BitcoinAddressSummaryRepository : ReactiveCrudRepository<CqlBitcoinAddressSummary, String>

interface BitcoinAddressMinedBlockRepository: ReactiveCrudRepository<CqlBitcoinAddressMinedBlock, String>

interface PageableBitcoinAddressMinedBlockRepository: CassandraRepository<CqlBitcoinAddressMinedBlock, MapId> {
    fun findAllByMiner(miner: String, page: Pageable): Slice<CqlBitcoinAddressMinedBlock>
}

interface BitcoinAddressTxRepository : ReactiveCrudRepository<CqlBitcoinAddressTxPreview, MapId>

interface PageableBitcoinAddressTxRepository : CassandraRepository<CqlBitcoinAddressTxPreview, MapId> {
    fun findAllByAddress(address: String, page: Pageable): Slice<CqlBitcoinAddressTxPreview>
}

