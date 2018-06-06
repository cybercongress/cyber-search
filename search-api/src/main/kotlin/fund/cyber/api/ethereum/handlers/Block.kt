package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.RepositoryItemRequestHandler
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import reactor.core.publisher.toFlux

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumBlockHandlersConfiguration {

    @Bean
    fun ethereumBlockItemHandler() = RepositoryItemRequestHandler(
        "/block/{blockNumber}",
        EthereumBlockRepository::class.java
    ) { request, repository, _ ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        val block = repository.findById(blockNumber)
        ServerResponse.ok().body(block, CqlEthereumBlock::class.java)
    }

    @Bean
    fun ethereumBlockTxesItemHandler() = RepositoryItemRequestHandler(
        "/block/{blockNumber}/transactions",
        PageableEthereumBlockTxRepository::class.java
    ) { request, repository, _ ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository.findAllByBlockNumber(blockNumber, CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByBlockNumber(blockNumber, slice.nextPageable())
            } else return@RepositoryItemRequestHandler ServerResponse.ok().build()
        }
        ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumBlockTxPreview::class.java)
    }

}
