package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.bitcoin.functions.BlockTxesByBlockNumber
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinBlockTxRepository
import fund.cyber.cassandra.configuration.REPOSITORY_NAME_DELIMETER
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

        return applicationContext.getBeanNamesForType(BitcoinBlockRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val blockRepository = applicationContext
                    .getBean(beanName, BitcoinBlockRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val blockNumber = request.pathVariable("blockNumber").toLong()
                val block = blockRepository.findById(blockNumber)
                ServerResponse.ok().body(block, CqlBitcoinBlock::class.java)
            }
            RouterFunctions.route(path("/${chainName.toLowerCase()}/block/{blockNumber}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinBlockTxesByNumber(): RouterFunction<ServerResponse> {
        return applicationContext.getBeanNamesForType(PageableBitcoinBlockTxRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val repository = applicationContext.getBean(beanName, PageableBitcoinBlockTxRepository::class.java)
            val handler = BlockTxesByBlockNumber(repository)
            RouterFunctions.route(path("/${chainName.toLowerCase()}/block/{blockNumber}/transactions"), handler)
        }.asSingleRouterFunction()
    }
}
