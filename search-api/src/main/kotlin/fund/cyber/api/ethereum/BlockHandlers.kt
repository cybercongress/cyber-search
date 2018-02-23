package fund.cyber

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.ethereum.functions.BlockTxesByBlockNumber
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import fund.cyber.search.model.chains.EthereumFamilyChain
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
@DependsOn("cassandra-repositories")
class BlockHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun blockByNumber(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->
            val blockRepository = applicationContext.getBean(chain.name + "blockRepository", EthereumBlockRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val blockNumber = request.pathVariable("blockNumber").toLong()
                val block = blockRepository.findById(blockNumber)
                ServerResponse.ok().body(block, CqlEthereumBlock::class.java)
            }
            RouterFunctions.route(path("/${chain.lowerCaseName}/block/{blockNumber}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun blockTxesByNumber(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->
            val repository = applicationContext.getBean(
                    chain.name + "pageableBlockTxRepository", PageableEthereumBlockTxRepository::class.java
            )
            val handler = BlockTxesByBlockNumber(repository)
            RouterFunctions.route(path("/${chain.lowerCaseName}/block/{blockNumber}/transactions"), handler)
        }.asSingleRouterFunction()
    }
}