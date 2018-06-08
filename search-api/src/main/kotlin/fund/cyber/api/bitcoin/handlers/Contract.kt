package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.bitcoin.dto.ContractSummaryDto
import fund.cyber.api.common.BiRepositoryItemRequestHandler
import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.api.common.toPageableResponse
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinContractHandlersConfiguration {

    @Bean
    fun bitcoinContractItemHandler() = BiRepositoryItemRequestHandler(
        "/contract/{hash}",
        BitcoinContractSummaryRepository::class.java,
        BitcoinContractTxRepository::class.java
    ) { request, contractSummaryRepository, contractTxRepository ->

        val contractHash = request.pathVariable("hash")

        val contract = contractSummaryRepository.findById(contractHash)
        val contractUnconfirmedTxes = contractTxRepository.findAllByContractHashAndBlockTime(contractHash, -1)

        val result = contract.zipWith(contractUnconfirmedTxes.collectList()) { contr, txes ->
            ContractSummaryDto(contr, txes)
        }
        result.asServerResponse()
    }

    @Bean
    fun bitcoinContractTxesItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/transactions",
        PageableBitcoinContractTxRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        request.toPageableResponse { pageable -> repository.findAllByContractHash(hash, pageable) }
    }

    @Bean
    fun bitcoinContractBlocksItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/blocks",
        PageableBitcoinContractMinedBlockRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        request.toPageableResponse { pageable -> repository.findAllByMinerContractHash(hash, pageable) }
    }

}
