package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.api.common.toPageableResponse
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinBlockTxRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinBlockHandlersConfiguration {

    @Bean
    fun bitcoinBlockItemHandler() = SingleRepositoryItemRequestHandler(
        "/block/{blockNumber}",
        BitcoinBlockRepository::class.java
    ) { request, repository ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        val block = repository.findById(blockNumber)
        block.asServerResponse()
    }

    @Bean
    fun bitcoinBlockTxesItemHandler() = SingleRepositoryItemRequestHandler(
        "/block/{blockNumber}/transactions",
        PageableBitcoinBlockTxRepository::class.java
    ) { request, repository ->

        val blockNumber = request.pathVariable("blockNumber").toLong()
        request.toPageableResponse { pageable -> repository.findAllByBlockNumber(blockNumber, pageable) }
    }
}
