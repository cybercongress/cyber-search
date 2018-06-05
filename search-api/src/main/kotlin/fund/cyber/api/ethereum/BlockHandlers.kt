package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.ethereum.functions.BlockTxesByBlockNumber
import fund.cyber.cassandra.common.REPOSITORY_NAME_DELIMITER
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.support.GenericApplicationContext
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.RequestPredicates.path

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumBlockHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun ethereumBlockByNumber(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(EthereumBlockRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)

            val blockRepository = applicationContext.getBean(beanName, EthereumBlockRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val blockNumber = request.pathVariable("blockNumber").toLong()
                val block = blockRepository.findById(blockNumber)
                ServerResponse.ok().body(block, CqlEthereumBlock::class.java)
            }
            RouterFunctions.route(path("/${chainName.toLowerCase()}/block/{blockNumber}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumBlockTxesByNumber(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableEthereumBlockTxRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)

            val repository = applicationContext.getBean(beanName, PageableEthereumBlockTxRepository::class.java)
            val handler = BlockTxesByBlockNumber(repository)
            RouterFunctions.route(path("/${chainName.toLowerCase()}/block/{blockNumber}/transactions"), handler)
        }.asSingleRouterFunction()
    }
}
