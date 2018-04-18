package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinTx
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.search.model.chains.BitcoinFamilyChain
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
@DependsOn("bitcoin-cassandra-repositories")
class BitcoinTxHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinTxByHash(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val txRepository = applicationContext.getBean(chain.name + "txRepository", BitcoinTxRepository::class.java)

            val txByHash = HandlerFunction { request ->
                val hash = request.pathVariable("hash")
                val tx = txRepository.findById(hash)
                ServerResponse.ok().body(tx, CqlBitcoinTx::class.java)
            }
            RouterFunctions.route(path("/${chain.lowerCaseName}/tx/{hash}"), txByHash)
        }.asSingleRouterFunction()
    }
}
