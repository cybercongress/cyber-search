package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.common.toSearchHashFormat
import fund.cyber.api.ethereum.functions.ContractBlocksByHash
import fund.cyber.api.ethereum.functions.ContractTxesByHash
import fund.cyber.api.ethereum.functions.ContractUnclesByHash
import fund.cyber.cassandra.configuration.REPOSITORY_NAME_DELIMETER
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.repository.EthereumContractRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractTxRepository
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
class EthereumContractHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun ethereumContractById(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(EthereumContractRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

            val repository = applicationContext.getBean(beanName, EthereumContractRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val contractId = request.pathVariable("hash")
                val contract = repository.findById(contractId.toSearchHashFormat())
                ServerResponse.ok().body(contract, CqlEthereumContractSummary::class.java)
            }
            RouterFunctions.route(RequestPredicates.path("/${chainName.toLowerCase()}/contract/{hash}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumContractTxesByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableEthereumContractTxRepository::class.java)
                .map { beanName ->
                    val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

                    val repository = applicationContext
                            .getBean(beanName, PageableEthereumContractTxRepository::class.java)
                    val handler = ContractTxesByHash(repository)
                    RouterFunctions
                            .route(
                                    RequestPredicates.path("/${chainName.toLowerCase()}/contract/{hash}/transactions"),
                                    handler
                            )
                }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumContractBlocksByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableEthereumContractMinedBlockRepository::class.java)
                .map { beanName ->
                    val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

                    val repository = applicationContext.getBean(
                            beanName,
                            PageableEthereumContractMinedBlockRepository::class.java
                    )
                    val handler = ContractBlocksByHash(repository)
                    RouterFunctions
                            .route(
                                    RequestPredicates.path("/${chainName.toLowerCase()}/contract/{hash}/blocks"),
                                    handler
                            )
                }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumContractUnclesByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableEthereumContractMinedUncleRepository::class.java)
                .map { beanName ->
                    val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMETER)

                    val repository = applicationContext.getBean(
                            beanName,
                            PageableEthereumContractMinedUncleRepository::class.java
                    )
                    val handler = ContractUnclesByHash(repository)
                    RouterFunctions
                            .route(RequestPredicates.path(
                                    "/${chainName.toLowerCase()}/contract/{hash}/uncles"),
                                    handler
                            )
                }.asSingleRouterFunction()
    }
}
