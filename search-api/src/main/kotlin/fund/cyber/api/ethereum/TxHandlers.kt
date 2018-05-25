package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.cassandra.configuration.REPOSITORY_NAME_DELIMETER
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.support.GenericApplicationContext
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.RequestPredicates
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse

@Configuration
@DependsOn("ethereum-cassandra-repositories")
class EthereumTxHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun ethereumTxByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(EthereumTxRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val txRepository = applicationContext.getBean(beanName, EthereumTxRepository::class.java)

            val txByHash = HandlerFunction { request ->
                val hash = request.pathVariable("hash")
                val tx = txRepository.findById(hash.toSearchHashFormat())
                ServerResponse.ok().body(tx, CqlEthereumTx::class.java)
                    .switchIfEmpty(ServerResponse.notFound().build())
            }
            RouterFunctions.route(RequestPredicates.path("/${chainName.toLowerCase()}/tx/{hash}"), txByHash)
        }.asSingleRouterFunction()
    }
}
