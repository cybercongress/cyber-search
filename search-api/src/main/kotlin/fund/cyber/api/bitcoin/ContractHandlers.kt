package fund.cyber.api.bitcoin

import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.bitcoin.functions.ContractBlocksByHash
import fund.cyber.api.bitcoin.functions.ContractTxesByHash
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
class BitcoinContractHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinContractByHash(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val repository = applicationContext
                    .getBean(chain.name + "contractRepository", BitcoinContractSummaryRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val contractHash = request.pathVariable("hash")
                val contract = repository.findById(contractHash)
                ServerResponse.ok().body(contract, CqlBitcoinContractSummary::class.java)
            }
            RouterFunctions.route(path("/${chain.lowerCaseName}/contract/{hash}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinContractTxesByHash(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableContractTxRepository", PageableBitcoinContractTxRepository::class.java
            )
            val handler = ContractTxesByHash(repository)
            RouterFunctions.route(path("/${chain.lowerCaseName}/contract/{hash}/transactions"), handler)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinContractBlocksByHash(): RouterFunction<ServerResponse> {

        return BitcoinFamilyChain.values().map { chain ->

            val repository = applicationContext.getBean(
                    chain.name + "pageableContractBlockRepository",
                    PageableBitcoinContractMinedBlockRepository::class.java
            )
            val handler = ContractBlocksByHash(repository)
            RouterFunctions.route(path("/${chain.lowerCaseName}/contract/{hash}/blocks"), handler)
        }.asSingleRouterFunction()
    }
}
