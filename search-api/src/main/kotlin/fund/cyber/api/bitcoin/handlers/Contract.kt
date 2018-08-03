package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.bitcoin.dto.ContractSummaryDto
import fund.cyber.api.common.BiRepositoryItemRequestHandler
import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.TripleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.api.common.toPageableResponse
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

        request.toPageableResponse(
            getPage = { pageable -> contractTxRepository.findAllByContractHash(hash, pageable) },
            mapPage = { pageFlux ->
                pageFlux.flatMap { contractTx -> txRepository.findById(contractTx.hash) }
                    .sort { a, b ->
                        if (a.isMempoolTx() && b.isMempoolTx()) {
                            return@sort b.firstSeenTime.compareTo(a.firstSeenTime)
                        }
                        if (a.isMempoolTx() || b.isMempoolTx()) {
                            return@sort if (a.isMempoolTx()) -1 else 1
                        }
                        return@sort b.blockNumber.compareTo(a.blockNumber)
                    }
            }
        )
    }

    @Bean
    fun bitcoinContractBlocksItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/blocks",
        PageableBitcoinContractMinedBlockRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash").toSearchHashFormat()
        request.toPageableResponse { pageable -> repository.findAllByMinerContractHash(hash, pageable) }
    }

}
