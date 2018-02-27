package fund.cyber.address.bitcoin

import fund.cyber.address.common.summary.AddressSummaryStorage
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinUpdateAddressSummaryRepository
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class BitcoinAddressSummaryStorage(
        private val addressSummaryRepository: BitcoinUpdateAddressSummaryRepository
) : AddressSummaryStorage<CqlBitcoinAddressSummary> {

    override fun findById(id: String): Mono<CqlBitcoinAddressSummary> = addressSummaryRepository.findById(id)

    override fun findAllByIdIn(ids: Iterable<String>): Flux<CqlBitcoinAddressSummary> = addressSummaryRepository.findAllByIdIn(ids)

    override fun update(summary: CqlBitcoinAddressSummary, oldVersion: Long): Mono<Boolean> = addressSummaryRepository.update(summary, oldVersion)

    override fun insertIfNotRecord(summary: CqlBitcoinAddressSummary): Mono<Boolean> = addressSummaryRepository.insertIfNotRecord(summary)

    override fun commitUpdate(address: String, newVersion: Long): Mono<Boolean> = addressSummaryRepository.commitUpdate(address, newVersion)

    override fun update(summary: CqlBitcoinAddressSummary): Mono<CqlBitcoinAddressSummary> = addressSummaryRepository.save(summary)

    override fun remove(address: String): Mono<Void> = addressSummaryRepository.deleteById(address)
}