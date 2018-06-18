package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.bitcoin.dto.ContractSummaryDto
import fund.cyber.api.common.BiRepositoryItemRequestHandler
import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.TripleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.api.common.toPageableFlux
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinContractHandlersConfiguration {

    @Bean
    fun bitcoinContractItemHandler() = TripleRepositoryItemRequestHandler(
        "/contract/{hash}",
        BitcoinContractSummaryRepository::class.java,
        BitcoinContractTxRepository::class.java,
        BitcoinTxRepository::class.java
    ) { request, contractSummaryRepository, contractTxRepository, txRepository ->

        val contractHash = request.pathVariable("hash").toSearchHashFormat()

        val contract = contractSummaryRepository.findById(contractHash)

        val result = contract.flatMap { contractSummary ->
            contractTxRepository.findAllByContractHashAndBlockTime(contractSummary.hash, -1)
                .flatMap { txPreview -> txRepository.findById(txPreview.hash) }.collectList()
                .map { txes -> ContractSummaryDto(contractSummary, txes) }
        }
        result.asServerResponse()
    }

    @Bean
    fun bitcoinContractTxesItemHandler() = BiRepositoryItemRequestHandler(
        "/contract/{hash}/transactions",
        PageableBitcoinContractTxRepository::class.java,
        BitcoinTxRepository::class.java
    ) { request, contractTxRepository, txRepository ->

        val hash = request.pathVariable("hash").toSearchHashFormat()
        request
            .toPageableFlux { pageable -> contractTxRepository.findAllByContractHash(hash, pageable) }
            .flatMap { contractTx -> txRepository.findById(contractTx.hash) }
            .asServerResponse()
    }

    @Bean
    fun bitcoinContractBlocksItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/blocks",
        PageableBitcoinContractMinedBlockRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash").toSearchHashFormat()
        request.toPageableFlux { pageable -> repository.findAllByMinerContractHash(hash, pageable) }.asServerResponse()
    }

}
