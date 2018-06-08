package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.BiRepositoryItemRequestHandler
import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.api.common.toPageableResponse
import fund.cyber.api.ethereum.dto.ContractSummaryDto
import fund.cyber.cassandra.ethereum.repository.EthereumContractRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumContractHandlersConfiguration {

    @Bean
    fun ethereumContractItemHandler() = BiRepositoryItemRequestHandler(
        "/contract/{hash}",
        EthereumContractRepository::class.java,
        EthereumContractTxRepository::class.java
    ) { request, conractRepository, contractTxRepository ->

        val contractId = request.pathVariable("hash")

        val contract = conractRepository.findById(contractId)
        val contractUnconfirmedTxes = contractTxRepository.findAllByContractHashAndBlockTime(contractId, -1)

        val result = contract.zipWith(contractUnconfirmedTxes.collectList()) { contr, txes ->
            ContractSummaryDto(contr, txes)
        }
        result.asServerResponse()
    }

    @Bean
    fun ethereumContractTxesItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/transactions",
        PageableEthereumContractTxRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        request.toPageableResponse { pageable ->
            repository.findAllByContractHash(hash.toSearchHashFormat(), pageable)
        }
    }

    @Bean
    fun ethereumContractBlocksItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/blocks",
        PageableEthereumContractMinedBlockRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        request.toPageableResponse { pageable ->
            repository.findAllByMinerContractHash(hash.toSearchHashFormat(), pageable)
        }
    }

    @Bean
    fun ethereumContractUnclesItemHandler() = SingleRepositoryItemRequestHandler(
        "/contract/{hash}/uncles",
        PageableEthereumContractMinedUncleRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        request.toPageableResponse { pageable ->
            repository.findAllByMinerContractHash(hash.toSearchHashFormat(), pageable)
        }
    }

}
