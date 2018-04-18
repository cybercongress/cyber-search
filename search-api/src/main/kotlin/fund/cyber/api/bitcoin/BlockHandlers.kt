package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.bitcoin.functions.BlockTxesByBlockNumber
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinBlockTxRepository
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
class BitcoinBlockHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinBlockByNumber(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->
            val blockRepository = applicationContext
                    .getBean(chain.name + "blockRepository", BitcoinBlockRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val blockNumber = request.pathVariable("blockNumber").toLong()
                val block = blockRepository.findById(blockNumber)
                ServerResponse.ok().body(block, CqlBitcoinBlock::class.java)
            }
            RouterFunctions.route(path("/${chain.lowerCaseName}/block/{blockNumber}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinBlockTxesByNumber(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->
            val repository = applicationContext.getBean(
                    chain.name + "pageableBlockTxRepository", PageableBitcoinBlockTxRepository::class.java
            )
            val handler = BlockTxesByBlockNumber(repository)
            RouterFunctions.route(path("/${chain.lowerCaseName}/block/{blockNumber}/transactions"), handler)
        }.asSingleRouterFunction()
    }
}
