package fund.cyber.contract.bitcoin

import fund.cyber.contract.common.summary.ContractSummaryStorage
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinUpdateContractSummaryRepository
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class BitcoinContractSummaryStorage(
        private val contractSummaryRepository: BitcoinUpdateContractSummaryRepository
) : ContractSummaryStorage<CqlBitcoinContractSummary> {

    override fun findById(id: String): Mono<CqlBitcoinContractSummary> = contractSummaryRepository.findById(id)

    override fun findAllByIdIn(ids: Iterable<String>): Flux<CqlBitcoinContractSummary> = contractSummaryRepository
            .findAllByIdIn(ids)

    override fun update(summary: CqlBitcoinContractSummary, oldVersion: Long): Mono<Boolean> = contractSummaryRepository
            .update(summary, oldVersion)

    override fun insertIfNotRecord(summary: CqlBitcoinContractSummary): Mono<Boolean> = contractSummaryRepository
            .insertIfNotRecord(summary)

    override fun commitUpdate(contract: String, newVersion: Long): Mono<Boolean> = contractSummaryRepository
            .commitUpdate(contract, newVersion)

    override fun update(summary: CqlBitcoinContractSummary): Mono<CqlBitcoinContractSummary> = contractSummaryRepository
            .save(summary)

    override fun remove(contract: String): Mono<Void> = contractSummaryRepository.deleteById(contract)
}
