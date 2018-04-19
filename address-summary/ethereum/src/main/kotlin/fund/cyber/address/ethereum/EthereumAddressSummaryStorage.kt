package fund.cyber.address.ethereum

import fund.cyber.address.common.summary.AddressSummaryStorage
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.repository.EthereumUpdateContractSummaryRepository
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumAddressSummaryStorage(
        private val contractSummaryRepository: EthereumUpdateContractSummaryRepository
) : AddressSummaryStorage<CqlEthereumContractSummary> {

    override fun findById(id: String): Mono<CqlEthereumContractSummary> = contractSummaryRepository.findById(id)

    override fun findAllByIdIn(ids: Iterable<String>): Flux<CqlEthereumContractSummary> = contractSummaryRepository
            .findAllById(ids)

    override fun update(summary: CqlEthereumContractSummary, oldVersion: Long): Mono<Boolean> = contractSummaryRepository
            .update(summary, oldVersion)

    override fun insertIfNotRecord(summary: CqlEthereumContractSummary): Mono<Boolean> = contractSummaryRepository
            .insertIfNotRecord(summary)

    override fun commitUpdate(address: String, newVersion: Long): Mono<Boolean> = contractSummaryRepository
            .commitUpdate(address, newVersion)

    override fun update(summary: CqlEthereumContractSummary): Mono<CqlEthereumContractSummary> = contractSummaryRepository
            .save(summary)

    override fun remove(address: String): Mono<Void> = contractSummaryRepository.deleteById(address)
}
