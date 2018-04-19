package fund.cyber.api.ethereum

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.common.toSearchHashFormat
import fund.cyber.api.ethereum.functions.AddressBlocksByAddress
import fund.cyber.api.ethereum.functions.AddressTxesByAddress
import fund.cyber.api.ethereum.functions.AddressUnclesByAddress
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.repository.EthereumContractRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractTxRepository
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
@DependsOn("ethereum-cassandra-repositories")
class EthereumAddressHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun ethereumAddressById(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->

            val repository = applicationContext
                    .getBean(chain.name + "addressRepository", EthereumContractRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val addressId = request.pathVariable("hash")
                val address = repository.findById(addressId.toSearchHashFormat())
                ServerResponse.ok().body(address, CqlEthereumContractSummary::class.java)
            }
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/address/{hash}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumAddressTxesByAddress(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableAddressTxRepository", PageableEthereumContractTxRepository::class.java
            )
            val handler = AddressTxesByAddress(repository)
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/address/{hash}/transactions"), handler)
        }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumAddressBlocksByAddress(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableAddressBlockRepository",
                    PageableEthereumContractMinedBlockRepository::class.java
            )
            val handler = AddressBlocksByAddress(repository)
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/address/{hash}/blocks"), handler)
        }.asSingleRouterFunction()
    }

    @Bean
    fun ethereumAddressUnclesByAddress(): RouterFunction<ServerResponse> {

        return EthereumFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableAddressUncleRepository",
                    PageableEthereumContractMinedUncleRepository::class.java
            )
            val handler = AddressUnclesByAddress(repository)
            RouterFunctions.route(RequestPredicates.path("/${chain.lowerCaseName}/address/{hash}/uncles"), handler)
        }.asSingleRouterFunction()
    }
}
