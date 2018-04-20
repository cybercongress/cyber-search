package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.common.toSearchHashFormat
import fund.cyber.cassandra.configuration.REPOSITORY_NAME_DELIMETER
import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
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
class EthereumUncleHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun ethereumUncleByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(EthereumUncleRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val uncleRepository = applicationContext.getBean(beanName, EthereumUncleRepository::class.java)

            val uncleByHash = HandlerFunction { request ->
                val hash = request.pathVariable("hash")
                val uncle = uncleRepository.findById(hash.toSearchHashFormat())
                ServerResponse.ok().body(uncle, CqlEthereumUncle::class.java)
            }
            RouterFunctions.route(RequestPredicates.path("/${chainName.toLowerCase()}/uncle/{hash}"), uncleByHash)
        }.asSingleRouterFunction()
    }
}
