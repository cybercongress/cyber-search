package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.bitcoin.functions.AddressBlocksByAddress
import fund.cyber.api.bitcoin.functions.AddressTxesByAddress
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
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
class BitcoinAddressHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinAddressById(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val repository = applicationContext
                    .getBean(chain.name + "addressRepository", BitcoinContractSummaryRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val addressId = request.pathVariable("hash")
                val address = repository.findById(addressId)
                ServerResponse.ok().body(address, CqlBitcoinContractSummary::class.java)
            }
            RouterFunctions.route(path("/${chain.lowerCaseName}/address/{hash}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinAddressTxesByAddress(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableAddressTxRepository", PageableBitcoinContractTxRepository::class.java
            )
            val handler = AddressTxesByAddress(repository)
            RouterFunctions.route(path("/${chain.lowerCaseName}/address/{hash}/transactions"), handler)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinAddressBlocksByAddress(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableAddressBlockRepository",
                    PageableBitcoinContractMinedBlockRepository::class.java
            )
            val handler = AddressBlocksByAddress(repository)
            RouterFunctions.route(path("/${chain.lowerCaseName}/address/{hash}/blocks"), handler)
        }.asSingleRouterFunction()
    }
}
