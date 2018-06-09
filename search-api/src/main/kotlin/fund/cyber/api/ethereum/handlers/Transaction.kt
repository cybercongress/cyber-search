package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumTxHandlersConfiguration {

    @Bean
    fun ethereumTxItemHandler() = SingleRepositoryItemRequestHandler(
        "/tx/{hash}",
        EthereumTxRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash").toSearchHashFormat()
        val tx = repository.findById(hash)
        tx.asServerResponse()
    }

}
