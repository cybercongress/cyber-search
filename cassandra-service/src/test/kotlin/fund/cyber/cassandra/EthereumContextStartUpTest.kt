package fund.cyber.cassandra

import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUpdateContractSummaryRepository
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Instant

@DirtiesContext
@TestPropertySource(properties = ["CHAIN:ETHEREUM"])
class EthereumContextStartUpTest : CassandraTestBase() {

    @Autowired
    lateinit var blockRepository: EthereumBlockRepository

    @Autowired
    lateinit var contractSummaryRepository: EthereumUpdateContractSummaryRepository

    @Test
    @DisplayName("Should successfully create context for Ethereum repositories")
    fun shouldCreateContextForEthereumChain() {
        Assertions.assertNull(blockRepository.findAll().blockFirst())
    }

    @Test
    @DisplayName("Should use correctly queries for UpdateContractSummaryRepository ")
    fun shouldUseCorrectQueriesForUpdateContractSummaryRepositories() {

        val contractSummary = CqlEthereumContractSummary(
            hash = "H", txNumber = 322, smartContract = false, confirmedBalance = "0.322",
            confirmedTotalReceived = "0.322", firstActivityDate = Instant.now(), version = 322,
            kafkaDeltaOffset = 0, kafkaDeltaPartition = 0, kafkaDeltaTopic = "T", kafkaDeltaOffsetCommitted = false,
            lastActivityDate = Instant.now(), minedBlockNumber = 0, minedUncleNumber = 0, successfulOpNumber = 322
        )

        Assertions.assertTrue(contractSummaryRepository.insertIfNotRecord(contractSummary).block()!!)
        Assertions.assertEquals(contractSummary, contractSummaryRepository.findByHash(contractSummary.hash).block())

        val updateSummary = contractSummary.copy(lastActivityDate = Instant.now(), version = 323)
        Assertions.assertTrue(contractSummaryRepository.update(updateSummary, 322).block()!!)
        Assertions.assertEquals(updateSummary, contractSummaryRepository.findByHash(updateSummary.hash).block())
    }
}
