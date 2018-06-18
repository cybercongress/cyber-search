package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.api.common.toPageableFlux
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumBlockHandlersConfiguration {

    @Bean
    fun ethereumBlockItemHandler() = SingleRepositoryItemRequestHandler(
        "/block/{blockNumber}",
        EthereumBlockRepository::class.java
    ) { request, repository ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        val block = repository.findById(blockNumber)
        block.asServerResponse()
    }

    @Bean
    fun ethereumBlockTxesItemHandler() = SingleRepositoryItemRequestHandler(
        "/block/{blockNumber}/transactions",
        PageableEthereumBlockTxRepository::class.java
    ) { request, repository ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        request.toPageableFlux { pageable -> repository.findAllByBlockNumber(blockNumber, pageable) }
            .asServerResponse()
    }

}
