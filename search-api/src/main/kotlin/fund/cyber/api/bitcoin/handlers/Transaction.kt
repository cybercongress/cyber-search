package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinTxHandlersConfiguration {

    @Bean
    fun bitcoinTxItemHandler() = SingleRepositoryItemRequestHandler(
        "/tx/{hash}",
        BitcoinTxRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash").toSearchHashFormat()
        val tx = repository.findById(hash)
        tx.asServerResponse()
    }

}
