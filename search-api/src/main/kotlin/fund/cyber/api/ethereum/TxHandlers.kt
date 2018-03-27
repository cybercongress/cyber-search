package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
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
@DependsOn("cassandra-repositories")
class TxHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun txByHash(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->

            val txRepository = applicationContext.getBean(chain.name + "txRepository", EthereumTxRepository::class.java)

            val txByHash = HandlerFunction { request ->
                val blockNumber = request.pathVariable("hash")
                val block = txRepository.findById(blockNumber)
                ServerResponse.ok().body(block, CqlEthereumTx::class.java)
            }
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/tx/{hash}"), txByHash)
        }.asSingleRouterFunction()
    }
}
