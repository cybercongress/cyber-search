package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.ethereum.functions.AddressTxesByAddres
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import fund.cyber.cassandra.ethereum.repository.EthereumAddressRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumAddressTxRepository
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
class AddressHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun addressById(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->
            val repository = applicationContext.getBean(chain.name + "addressRepository", EthereumAddressRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val blockNumber = request.pathVariable("id")
                val block = repository.findById(blockNumber)
                ServerResponse.ok().body(block, CqlEthereumAddressSummary::class.java)
            }
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/address/{id}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun addressTxesByAddress(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->
            val repository = applicationContext.getBean(
                    chain.name + "pageableAddressTxRepository", PageableEthereumAddressTxRepository::class.java
            )
            val handler = AddressTxesByAddres(repository)
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/address/{id}/transactions"), handler)
        }.asSingleRouterFunction()
    }
}