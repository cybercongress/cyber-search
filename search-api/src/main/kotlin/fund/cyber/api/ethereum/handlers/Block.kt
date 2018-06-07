package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.toPageableResponse
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.web.reactive.function.server.ServerResponse

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
        ServerResponse.ok().body(block, CqlEthereumBlock::class.java)
    }

    @Bean
    fun ethereumBlockTxesItemHandler() = SingleRepositoryItemRequestHandler(
        "/block/{blockNumber}/transactions",
        PageableEthereumBlockTxRepository::class.java
    ) { request, repository ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        request.toPageableResponse { pageable -> repository.findAllByBlockNumber(blockNumber, pageable) }
    }

}
