package fund.cyber.address.ethereum

import fund.cyber.address.common.summary.AddressSummaryStorage
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import fund.cyber.cassandra.ethereum.repository.EthereumUpdateAddressSummaryRepository
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumAddressSummaryStorage(
        private val addressSummaryRepository: EthereumUpdateAddressSummaryRepository
) : AddressSummaryStorage<CqlEthereumAddressSummary> {

    override fun findById(id: String): Mono<CqlEthereumAddressSummary> = addressSummaryRepository.findById(id)

    override fun findAllByIdIn(ids: Iterable<String>): Flux<CqlEthereumAddressSummary> = addressSummaryRepository.findAllByIdIn(ids)

    override fun update(summary: CqlEthereumAddressSummary, oldVersion: Long): Mono<Boolean> = addressSummaryRepository.update(summary, oldVersion)

    override fun insertIfNotRecord(summary: CqlEthereumAddressSummary): Mono<Boolean> = addressSummaryRepository.insertIfNotRecord(summary)

    override fun commitUpdate(address: String, newVersion: Long): Mono<Boolean> = addressSummaryRepository.commitUpdate(address, newVersion)

    override fun update(summary: CqlEthereumAddressSummary): Mono<CqlEthereumAddressSummary> = addressSummaryRepository.save(summary)

    override fun remove(address: String): Mono<Void> = addressSummaryRepository.deleteById(address)
}