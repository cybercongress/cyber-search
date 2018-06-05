package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.cassandra.common.REPOSITORY_NAME_DELIMITER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.support.GenericApplicationContext
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.RequestPredicates.path
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinTxHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinTxByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(BitcoinTxRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)

            val txRepository = applicationContext.getBean(beanName, BitcoinTxRepository::class.java)

            val txByHash = HandlerFunction { request ->
                val hash = request.pathVariable("hash")
                val tx = txRepository.findById(hash)
                ServerResponse.ok().body(tx, CqlBitcoinTx::class.java)
            }
            RouterFunctions.route(path("/${chainName.toLowerCase()}/tx/{hash}"), txByHash)
        }.asSingleRouterFunction()
    }
}
