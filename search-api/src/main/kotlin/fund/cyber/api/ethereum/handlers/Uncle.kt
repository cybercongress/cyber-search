package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.RepositoryItemRequestHandler
import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.web.reactive.function.server.ServerResponse

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumUncleHandlersConfiguration {

    @Bean
    fun ethereumUncleItemHandler() = RepositoryItemRequestHandler(
        "/uncle/{hash}",
        EthereumUncleRepository::class.java
    ) { request, repository, _ ->

        val hash = request.pathVariable("hash")
        val uncle = repository.findById(hash.toSearchHashFormat())
        ServerResponse.ok().body(uncle, CqlEthereumUncle::class.java)
    }

}
