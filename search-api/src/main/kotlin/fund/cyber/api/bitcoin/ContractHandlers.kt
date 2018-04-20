package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.bitcoin.functions.ContractBlocksByHash
import fund.cyber.api.bitcoin.functions.ContractTxesByHash
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
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
class BitcoinContractHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinContractByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(BitcoinContractSummaryRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val repository = applicationContext.getBean(beanName, BitcoinContractSummaryRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val contractHash = request.pathVariable("hash")
                val contract = repository.findById(contractHash)
                ServerResponse.ok().body(contract, CqlBitcoinContractSummary::class.java)
            }
            RouterFunctions.route(path("/${chainName.toLowerCase()}/contract/{hash}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinContractTxesByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableBitcoinContractTxRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val repository = applicationContext.getBean(beanName, PageableBitcoinContractTxRepository::class.java)
            val handler = ContractTxesByHash(repository)
            RouterFunctions.route(path("/${chainName.toLowerCase()}/contract/{hash}/transactions"), handler)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinContractBlocksByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableBitcoinContractMinedBlockRepository::class.java)
                .map { beanName ->
                    val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

                    val repository = applicationContext
                            .getBean(beanName, PageableBitcoinContractMinedBlockRepository::class.java)
                    val handler = ContractBlocksByHash(repository)
                    RouterFunctions.route(path("/${chainName.toLowerCase()}/contract/{hash}/blocks"), handler)
                }.asSingleRouterFunction()
    }
}
