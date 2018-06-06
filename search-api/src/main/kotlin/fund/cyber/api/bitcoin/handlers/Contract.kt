package fund.cyber.api.bitcoin.handlers

import fund.cyber.api.bitcoin.dto.ContractSummaryDto
import fund.cyber.api.common.RepositoryItemRequestHandler
import fund.cyber.api.common.getSearchRepositoryBean
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractMinedBlock
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractTxPreview
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractSummaryRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.PageableBitcoinContractTxRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.support.GenericApplicationContext
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.toFlux

@Configuration
@DependsOn("bitcoin-search-repositories")
class BitcoinContractHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun bitcoinContractItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}",
        BitcoinContractSummaryRepository::class.java
    ) { request, repository, chain ->

        val contractTxRepository = applicationContext
            .getSearchRepositoryBean(BitcoinContractTxRepository::class.java, chain)

        val contractHash = request.pathVariable("hash")

        val contract = repository.findById(contractHash)
        val contractUnconfirmedTxes = contractTxRepository
            .findAllByContractHashAndBlockTime(contractHash, -1)

        val result = contract.zipWith(contractUnconfirmedTxes.collectList()) { contr, txes ->
            ContractSummaryDto(contr, txes)
        }
        ServerResponse.ok().body(result, ContractSummaryDto::class.java)
    }

    @Bean
    fun bitcoinContractTxesItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}/transactions",
        PageableBitcoinContractTxRepository::class.java
    ) { request, repository, _ ->

        val hash = request.pathVariable("hash")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository.findAllByContractHash(hash, CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByContractHash(hash, slice.nextPageable())
            } else return@RepositoryItemRequestHandler ServerResponse.notFound().build()
        }
        ServerResponse.ok().body(slice.content.toFlux(), CqlBitcoinContractTxPreview::class.java)
    }

    @Bean
    fun bitcoinContractBlocksItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}/blocks",
        PageableBitcoinContractMinedBlockRepository::class.java
    ) { request, repository, _ ->

        val hash = request.pathVariable("hash")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository.findAllByMinerContractHash(hash, CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByMinerContractHash(hash, slice.nextPageable())
            } else return@RepositoryItemRequestHandler ServerResponse.notFound().build()
        }
        ServerResponse.ok().body(slice.content.toFlux(), CqlBitcoinContractMinedBlock::class.java)
    }

}
