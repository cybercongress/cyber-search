package fund.cyber.contract.ethereum

import fund.cyber.contract.common.summary.ContractSummaryStorage
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.repository.EthereumUpdateContractSummaryRepository
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumContractSummaryStorage(
        private val contractSummaryRepository: EthereumUpdateContractSummaryRepository
) : ContractSummaryStorage<CqlEthereumContractSummary> {

    override fun findById(id: String): Mono<CqlEthereumContractSummary> = contractSummaryRepository.findByHash(id)

    override fun findAllByIdIn(ids: Iterable<String>): Flux<CqlEthereumContractSummary> = contractSummaryRepository
            .findAllById(ids)

    override fun update(summary: CqlEthereumContractSummary, oldVersion: Long): Mono<Boolean> = contractSummaryRepository
            .update(summary, oldVersion)

    override fun insertIfNotRecord(summary: CqlEthereumContractSummary): Mono<Boolean> = contractSummaryRepository
            .insertIfNotRecord(summary)

    override fun commitUpdate(contract: String, newVersion: Long): Mono<Boolean> = contractSummaryRepository
            .commitUpdate(contract, newVersion)

    override fun update(summary: CqlEthereumContractSummary): Mono<CqlEthereumContractSummary> = contractSummaryRepository
            .save(summary)

    override fun remove(contract: String): Mono<Void> = contractSummaryRepository.deleteById(contract)
}
