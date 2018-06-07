package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.web.reactive.function.server.ServerResponse

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumTxHandlersConfiguration {

    @Bean
    fun ethereumTxItemHandler() = SingleRepositoryItemRequestHandler(
        "/tx/{hash}",
        EthereumTxRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash")
        val tx = repository.findById(hash.toSearchHashFormat())
        ServerResponse.ok().body(tx, CqlEthereumTx::class.java).switchIfEmpty(ServerResponse.notFound().build())
    }

}
