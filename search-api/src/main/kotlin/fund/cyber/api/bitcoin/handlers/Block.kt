package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.common.RepositoryItemRequestHandler
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlockTxPreview
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinBlockTxRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.toFlux

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinBlockHandlersConfiguration {

    @Bean
    fun bitcoinBlockItemHandler() = RepositoryItemRequestHandler(
        "/block/{blockNumber}",
        BitcoinBlockRepository::class.java
    ) { request, repository, _ ->

            val blockNumber = request.pathVariable("blockNumber").toLong()
            val block = repository.findById(blockNumber)
            ServerResponse.ok().body(block, CqlBitcoinBlock::class.java)
    }

    @Bean
    fun bitcoinBlockTxesItemHandler() = RepositoryItemRequestHandler(
        "/block/{blockNumber}/transactions",
        PageableBitcoinBlockTxRepository::class.java
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
        ServerResponse.ok().body(slice.content.toFlux(), CqlBitcoinBlockTxPreview::class.java)
    }

}
