package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.web.reactive.function.server.ServerResponse

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinTxHandlersConfiguration {

    @Bean
    fun bitcoinTxItemHandler() = SingleRepositoryItemRequestHandler(
        "/tx/{hash}",
        BitcoinTxRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        val tx = repository.findById(hash)
        ServerResponse.ok().body(tx, CqlBitcoinTx::class.java)
    }

}
