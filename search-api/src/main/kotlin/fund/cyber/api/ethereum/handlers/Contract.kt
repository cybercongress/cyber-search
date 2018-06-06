package fund.cyber.api.ethereum.handlers

import fund.cyber.api.common.RepositoryItemRequestHandler
import fund.cyber.api.common.getSearchRepositoryBean
import fund.cyber.api.ethereum.dto.ContractSummaryDto
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedUncle
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractTxPreview
import fund.cyber.cassandra.ethereum.repository.EthereumContractRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractTxRepository
import fund.cyber.common.toSearchHashFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.support.GenericApplicationContext
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.toFlux

@Configuration
@DependsOn("ethereum-search-repositories")
class EthereumContractHandlersConfiguration {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun ethereumContractItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}",
        EthereumContractRepository::class.java
    ) { request, repository, chain ->

        val contractTxRepository = applicationContext
            .getSearchRepositoryBean(EthereumContractTxRepository::class.java, chain)

        val contractId = request.pathVariable("hash")

        val contract = repository.findById(contractId)
        val contractUnconfirmedTxes = contractTxRepository.findAllByContractHashAndBlockTime(contractId, -1)

        val result = contract.zipWith(contractUnconfirmedTxes.collectList()) { contr, txes ->
            ContractSummaryDto(contr, txes)
        }
        ServerResponse.ok().body(result, ContractSummaryDto::class.java)
    }

    @Bean
    fun ethereumContractTxesItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}/transactions",
        PageableEthereumContractTxRepository::class.java
    ) { request, repository, _ ->

        val hash = request.pathVariable("hash")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository.findAllByContractHash(hash.toSearchHashFormat(), CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByContractHash(hash, slice.nextPageable())
            } else return@RepositoryItemRequestHandler ServerResponse.notFound().build()
        }
        ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumContractTxPreview::class.java)
    }

    @Bean
    fun ethereumContractBlocksItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}/blocks",
        PageableEthereumContractMinedBlockRepository::class.java
    ) { request, repository, _ ->

        val hash = request.pathVariable("hash")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository
            .findAllByMinerContractHash(hash.toSearchHashFormat(), CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByMinerContractHash(hash, slice.nextPageable())
            } else return@RepositoryItemRequestHandler ServerResponse.notFound().build()
        }
        ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumContractMinedBlock::class.java)
    }

    @Bean
    fun ethereumContractUnclesItemHandler() = RepositoryItemRequestHandler(
        "/contract/{hash}/uncles",
        PageableEthereumContractMinedUncleRepository::class.java
    ) { request, repository, _ ->

        val hash = request.pathVariable("hash")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository
            .findAllByMinerContractHash(hash.toSearchHashFormat(), CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByMinerContractHash(hash, slice.nextPageable())
            } else return@RepositoryItemRequestHandler ServerResponse.notFound().build()
        }
        ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumContractMinedUncle::class.java)
    }

}
