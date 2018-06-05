package fund.cyber.api.bitcoin

import fund.cyber.api.bitcoin.dto.ContractSummaryDto
import fund.cyber.api.common.asSingleRouterFunction
import fund.cyber.api.bitcoin.functions.ContractBlocksByHash
import fund.cyber.api.bitcoin.functions.ContractTxesByHash
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
import fund.cyber.cassandra.common.REPOSITORY_NAME_DELIMITER
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
@DependsOn("bitcoin-search-repositories")
class BitcoinContractHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinContractByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(BitcoinContractSummaryRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)

            val repository = applicationContext.getBean(beanName, BitcoinContractSummaryRepository::class.java)

            //todo: make variables with repositories names and common method to get bean name
            val contractTxRepositoryBeanName = "$chainName$REPOSITORY_NAME_DELIMITER" +
                BitcoinContractTxRepository::class.java.name
            val contractTxRepository = applicationContext
                .getBean(contractTxRepositoryBeanName, BitcoinContractTxRepository::class.java)

            val blockByNumber = HandlerFunction { request ->
                val contractHash = request.pathVariable("hash")

                val contract = repository.findById(contractHash)
                val contractUnconfirmedTxes = contractTxRepository
                    .findAllByContractHashAndBlockTime(contractHash, -1)

                val result = contract.zipWith(contractUnconfirmedTxes.collectList()) { contr, txes ->
                    ContractSummaryDto(contr, txes)
                }
                ServerResponse.ok().body(result, ContractSummaryDto::class.java)
            }
            RouterFunctions.route(path("/${chainName.toLowerCase()}/contract/{hash}"), blockByNumber)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinContractTxesByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableBitcoinContractTxRepository::class.java).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)

            val repository = applicationContext.getBean(beanName, PageableBitcoinContractTxRepository::class.java)
            val handler = ContractTxesByHash(repository)
            RouterFunctions.route(path("/${chainName.toLowerCase()}/contract/{hash}/transactions"), handler)
        }.asSingleRouterFunction()
    }

    @Bean
    fun bitcoinContractBlocksByHash(): RouterFunction<ServerResponse> {

        return applicationContext.getBeanNamesForType(PageableBitcoinContractMinedBlockRepository::class.java)
            .map { beanName ->
                val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)

                val repository = applicationContext
                    .getBean(beanName, PageableBitcoinContractMinedBlockRepository::class.java)
                val handler = ContractBlocksByHash(repository)
                RouterFunctions.route(path("/${chainName.toLowerCase()}/contract/{hash}/blocks"), handler)
            }.asSingleRouterFunction()
    }
}
