package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.SingleRepositoryItemRequestHandler
import fund.cyber.api.common.asServerResponse
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.common.toSearchEthereumHashFormat
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumUncleHandlersConfiguration {

    @Bean
    fun ethereumUncleItemHandler() = SingleRepositoryItemRequestHandler(
        "/uncle/{hash}",
        EthereumUncleRepository::class.java
    ) { request, repository ->

        val hash = request.pathVariable("hash").toSearchEthereumHashFormat()
        val uncle = repository.findById(hash)
        uncle.asServerResponse()
    }

}
