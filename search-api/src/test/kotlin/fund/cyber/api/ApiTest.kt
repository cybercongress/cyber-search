package fund.cyber.api

import fund.cyber.SearchApiApplication
import fund.cyber.cassandra.CassandraTestBase
import org.cassandraunit.spring.CassandraDataSet
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.support.GenericApplicationContext
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.web.reactive.server.WebTestClient

@CassandraTestBase
@ContextConfiguration(classes = [SearchApiApplication::class])
abstract class BaseApiContextTest

//todo: ElasticSearch tests. Possible solutions: https://stackoverflow.com/questions/41298467/how-to-start-elasticsearch-5-1-embedded-in-my-java-application
@CassandraDataSet(value = ["cassandra-bitcoin-data.cql", "cassandra-ethereum-data.cql"])
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApiTest : BaseApiContextTest() {


    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    private lateinit var webClient: WebTestClient

    @BeforeAll
    fun setUp() {
        webClient = WebTestClient.bindToApplicationContext(applicationContext).build()
    }

    @Test
    fun pingTest() {

        webClient.get().uri("/ping").accept(MediaType.APPLICATION_JSON_UTF8).exchange().expectStatus().isOk()
    }

    @Test
    fun bitcoinBlockTestOk() {

        webClient.get().uri("/bitcoin/block/0")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"number\": 0,\n" +
                "  \"hash\": \"a\",\n" +
                "  \"parentHash\": \"\",\n" +
                "  \"minerContractHash\": \"a\",\n" +
                "  \"blockReward\": 50,\n" +
                "  \"txFees\": 0,\n" +
                "  \"coinbaseData\": \"\",\n" +
                "  \"timestamp\": 1.491045719001E9,\n" +
                "  \"nonce\": 0,\n" +
                "  \"merkleroot\": \"a\",\n" +
                "  \"size\": 100,\n" +
                "  \"version\": 0,\n" +
                "  \"weight\": 100,\n" +
                "  \"bits\": \"\",\n" +
                "  \"difficulty\": 1,\n" +
                "  \"txNumber\": 1,\n" +
                "  \"totalOutputsValue\": \"50\"\n" +
                "}")
    }

    @Test
    fun bitcoinBlockTestNotFound() {

        webClient.get().uri("/bitcoin/block/1")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun bitcoinBlockTxTestOk() {

        webClient.get().uri("/bitcoin/block/0/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"blockNumber\": 0,\n" +
                "    \"index\": 0,\n" +
                "    \"hash\": \"txa\",\n" +
                "    \"fee\": 0,\n" +
                "    \"ins\": [],\n" +
                "    \"outs\": [\n" +
                "      {\n" +
                "        \"contracts\": [\n" +
                "          \"a\"\n" +
                "        ],\n" +
                "        \"amount\": 50\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]")
    }

    @Test
    fun bitcoinBlockTxTestNotFound() {

        webClient.get().uri("/bitcoin/block/1/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun bitcoinContractTestOk() {

        webClient.get().uri("/bitcoin/contract/a")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"hash\": \"a\",\n" +
                "  \"confirmedBalance\": 50,\n" +
                "  \"confirmedTotalReceived\": 50,\n" +
                "  \"confirmedTxNumber\": 1,\n" +
                "  \"firstActivityDate\": 1.491045719001E9,\n" +
                "  \"lastActivityDate\": 1.491045719001E9,\n" +
                "  \"unconfirmedTxValues\": {}\n" +
                "}")
    }

    @Test
    fun bitcoinContractTestNotFound() {

        webClient.get().uri("/bitcoin/contract/b")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun bitcoinContractTxesTestOk() {

        webClient.get().uri("/bitcoin/contract/a/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"hash\": \"txa\",\n" +
                "    \"blockNumber\": 0,\n" +
                "    \"blockHash\": \"a\",\n" +
                "    \"coinbase\": \"\",\n" +
                "    \"firstSeenTime\": 1.491045719001E9,\n" +
                "    \"blockTime\": 1.491045719001E9,\n" +
                "    \"size\": 0,\n" +
                "    \"fee\": \"0\",\n" +
                "    \"totalInput\": \"0\",\n" +
                "    \"totalOutput\": \"50\",\n" +
                "    \"ins\": [],\n" +
                "    \"outs\": [\n" +
                "      {\n" +
                "        \"contracts\": [\n" +
                "          \"a\"\n" +
                "        ],\n" +
                "        \"amount\": 50,\n" +
                "        \"asm\": \"\",\n" +
                "        \"out\": 0,\n" +
                "        \"requiredSignatures\": 1\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]\n")
    }

    @Test
    fun bitcoinContractTxesTestNotFound() {

        webClient.get().uri("/bitcoin/contract/b/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun bitcoinContractBlocksTestOk() {

        webClient.get().uri("/bitcoin/contract/a/blocks")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"minerContractHash\": \"a\",\n" +
                "    \"blockNumber\": 0,\n" +
                "    \"blockTime\": 1.491045719001E9,\n" +
                "    \"blockReward\": 50,\n" +
                "    \"txFees\": 0,\n" +
                "    \"txNumber\": 1\n" +
                "  }\n" +
                "]")
    }

    @Test
    fun bitcoinContractBlocksTestNotFound() {

        webClient.get().uri("/bitcoin/contract/b/blocks")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun bitcoinTransactionTestOk() {

        webClient.get().uri("/bitcoin/tx/txa")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"hash\": \"txa\",\n" +
                "  \"blockNumber\": 0,\n" +
                "  \"blockHash\": \"a\",\n" +
                "  \"coinbase\": \"\",\n" +
                "  \"firstSeenTime\": 1.491045719001E9,\n" +
                "  \"blockTime\": 1.491045719001E9,\n" +
                "  \"size\": 0,\n" +
                "  \"fee\": \"0\",\n" +
                "  \"totalInput\": \"0\",\n" +
                "  \"totalOutput\": \"50\",\n" +
                "  \"ins\": [],\n" +
                "  \"outs\": [\n" +
                "    {\n" +
                "      \"contracts\": [\n" +
                "        \"a\"\n" +
                "      ],\n" +
                "      \"amount\": 50,\n" +
                "      \"asm\": \"\",\n" +
                "      \"out\": 0,\n" +
                "      \"requiredSignatures\": 1\n" +
                "    }\n" +
                "  ]\n" +
                "}")
    }

    @Test
    fun bitcoinTransactionTestNotFound() {

        webClient.get().uri("/bitcoin/tx/txb")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumBlockTestOk() {

        webClient.get().uri("/ethereum/block/0")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"number\": 0,\n" +
                "  \"hash\": \"a\",\n" +
                "  \"parent_hash\": \"\",\n" +
                "  \"timestamp\": 1.491045719001E9,\n" +
                "  \"sha3Uncles\": \"\",\n" +
                "  \"logsBloom\": \"\",\n" +
                "  \"transactionsRoot\": \"txroot\",\n" +
                "  \"stateRoot\": \"\",\n" +
                "  \"receiptsRoot\": \"\",\n" +
                "  \"minerContractHash\": \"a\",\n" +
                "  \"difficulty\": 0,\n" +
                "  \"totalDifficulty\": 0,\n" +
                "  \"extraData\": \"\",\n" +
                "  \"size\": 0,\n" +
                "  \"gasLimit\": 100,\n" +
                "  \"gasUsed\": 100,\n" +
                "  \"txNumber\": 1,\n" +
                "  \"uncles\": [],\n" +
                "  \"blockReward\": \"5\",\n" +
                "  \"unclesReward\": \"5\",\n" +
                "  \"txFees\": \"0\"\n" +
                "}")
    }

    @Test
    fun ethereumBlockTestNotFound() {

        webClient.get().uri("/ethereum/block/1")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumBlockTxesTestOk() {

        webClient.get().uri("/ethereum/block/0/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"blockNumber\": 0,\n" +
                "    \"positionInBlock\": 0,\n" +
                "    \"fee\": 0,\n" +
                "    \"value\": 10,\n" +
                "    \"hash\": \"txa\",\n" +
                "    \"from\": \"b\",\n" +
                "    \"to\": \"a\",\n" +
                "    \"createsContract\": false\n" +
                "  }\n"+
                "]")
    }

    @Test
    fun ethereumBlockTxesTestNotFound() {

        webClient.get().uri("/ethereum/block/1/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumContractTestOk() {

        webClient.get().uri("/ethereum/contract/a")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"hash\": \"a\",\n" +
                "  \"confirmedBalance\": 10,\n" +
                "  \"smartContract\": false,\n" +
                "  \"confirmedTotalReceived\": 10,\n" +
                "  \"txNumber\": 1,\n" +
                "  \"minedUncleNumber\": 0,\n" +
                "  \"minedBlockNumber\": 1,\n" +
                "  \"firstActivityDate\": 1.491045719001E9,\n" +
                "  \"lastActivityDate\": 1.491045719001E9,\n" +
                "  \"unconfirmedTxValues\": {}\n" +
                "}")
    }

    @Test
    fun ethereumContractTestNotFound() {

        webClient.get().uri("/ethereum/contract/b")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumContractTxesTestOk() {

        webClient.get().uri("/ethereum/contract/a/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"contractHash\": \"a\",\n" +
                "    \"blockTime\": 1230933600,\n" +
                "    \"hash\": \"txa\",\n" +
                "    \"fee\": 0,\n" +
                "    \"firstSeenTime\": 1.491045719001E9,\n" +
                "    \"from\": \"b\",\n" +
                "    \"to\": \"a\",\n" +
                "    \"value\": \"10\"\n" +
                "  }\n" +
                "]")
    }

    @Test
    fun ethereumContractTxesTestNotFound() {

        webClient.get().uri("/ethereum/contract/b/transactions")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumContractBlocksTestOk() {

        webClient.get().uri("/ethereum/contract/a/blocks")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"minerContractHash\": \"a\",\n" +
                "    \"blockNumber\": 0,\n" +
                "    \"blockTime\": 1.491045719001E9,\n" +
                "    \"blockReward\": 5,\n" +
                "    \"unclesReward\": 5,\n" +
                "    \"txFees\": 0,\n" +
                "    \"txNumber\": 1\n" +
                "  }\n" +
                "]")
    }

    @Test
    fun ethereumContractBlocksTestNotFound() {

        webClient.get().uri("/ethereum/contract/b/blocks")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumContractUnclesTestOk() {

        webClient.get().uri("/ethereum/contract/a/uncles")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("[\n" +
                "  {\n" +
                "    \"minerContractHash\": \"a\",\n" +
                "    \"blockNumber\": 0,\n" +
                "    \"hash\": \"ua\",\n" +
                "    \"position\": 0,\n" +
                "    \"number\": 0,\n" +
                "    \"timestamp\": 1.491045719001E9,\n" +
                "    \"blockTime\": 1.491045719001E9,\n" +
                "    \"blockHash\": \"a\",\n" +
                "    \"uncleReward\": \"5\"\n" +
                "  }\n" +
                "]")
    }

    @Test
    fun ethereumContractUnclesTestNotFound() {

        webClient.get().uri("/ethereum/contract/b/uncles")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumTxTestOk() {

        webClient.get().uri("/ethereum/tx/txa")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"hash\": \"txa\",\n" +
                "  \"status\": \"OK\",\n" +
                "  \"nonce\": 0,\n" +
                "  \"blockHash\": \"a\",\n" +
                "  \"blockNumber\": 0,\n" +
                "  \"firstSeenTime\": 1.491045719001E9,\n" +
                "  \"blockTime\": 1.491045719001E9,\n" +
                "  \"from\": \"b\",\n" +
                "  \"to\": \"a\",\n" +
                "  \"value\": \"10\",\n" +
                "  \"gasPrice\": 0,\n" +
                "  \"gasLimit\": 0,\n" +
                "  \"gasUsed\": 0,\n" +
                "  \"fee\": \"0\",\n" +
                "  \"input\": \"\",\n" +
                "  \"createdContract\": \"\",\n" +
                "  \"trace\": null\n" +
                "}")
    }

    @Test
    fun ethereumTxTestNotFound() {

        webClient.get().uri("/ethereum/tx/txb")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }

    @Test
    fun ethereumUncleTestOk() {

        webClient.get().uri("/ethereum/uncle/ua")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_UTF8)
            .expectBody().json("{\n" +
                "  \"hash\": \"ua\",\n" +
                "  \"position\": 0,\n" +
                "  \"number\": 0,\n" +
                "  \"timestamp\": 1.491045719001E9,\n" +
                "  \"blockNumber\": 0,\n" +
                "  \"blockTime\": 1.491045719001E9,\n" +
                "  \"blockHash\": \"a\",\n" +
                "  \"miner\": \"a\",\n" +
                "  \"uncleReward\": \"5\"\n" +
                "}")
    }

    @Test
    fun ethereumUncleTestNotFound() {

        webClient.get().uri("/ethereum/uncle/ub")
            .accept(MediaType.APPLICATION_JSON_UTF8).exchange()
            .expectStatus().isNotFound()
    }
}