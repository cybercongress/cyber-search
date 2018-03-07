package fund.cyber.common.kafka.reader

import fund.cyber.common.kafka.BaseForKafkaIntegrationTest
import fund.cyber.common.kafka.SinglePartitionTopicDataPresentLatch
import fund.cyber.common.kafka.sendRecordsInTransaction
import org.junit.jupiter.api.*
import org.springframework.kafka.test.context.EmbeddedKafka


const val SINGLE_TRANSACTION_RECORD_TOPIC = "SINGLE_TRANSACTION_RECORD_TOPIC"

@EmbeddedKafka(
        partitions = 1, topics = [SINGLE_TRANSACTION_RECORD_TOPIC],
        brokerProperties = [
            "auto.create.topics.enable=false", "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1"
        ]
)
@DisplayName("Single-partitioned topic last item reader test")
class SinglePartitionSingeTransactionRecordReaderTest : BaseForKafkaIntegrationTest() {


    @BeforeEach
    fun produceRecords() {

        sendRecordsInTransaction(
                embeddedKafka.brokersAsString, SINGLE_TRANSACTION_RECORD_TOPIC, listOf("key" to 1)
        )

        SinglePartitionTopicDataPresentLatch(embeddedKafka.brokersAsString, SINGLE_TRANSACTION_RECORD_TOPIC, String::class.java, Int::class.java).countDownLatch.await()
    }

//    @Test
    @RepeatedTest(value = 100)
    @DisplayName("Test topic with transaction returns single record")
    fun testSingleTransactionRecord() {


        val reader = SinglePartitionTopicLastItemsReader(
                kafkaBrokers = embeddedKafka.brokersAsString, topic = SINGLE_TRANSACTION_RECORD_TOPIC,
                keyClass = String::class.java, valueClass = Int::class.java
        )
        val records = reader.readLastRecords(1)

        Assertions.assertEquals(1, records.size)
        Assertions.assertEquals("key", records.first().first)
        Assertions.assertEquals(1, records.first().second)
    }
}