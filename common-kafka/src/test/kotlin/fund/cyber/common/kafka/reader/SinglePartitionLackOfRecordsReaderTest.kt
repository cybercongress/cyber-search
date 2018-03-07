package fund.cyber.common.kafka.reader

import fund.cyber.common.kafka.BaseForKafkaIntegrationTest
import fund.cyber.common.kafka.SinglePartitionTopicDataPresentLatch
import fund.cyber.common.kafka.sendRecords
import org.junit.jupiter.api.*
import org.springframework.kafka.test.context.EmbeddedKafka


const val EXISTING_TOPIC_WITH_RECORDS_LACK = "EXISTING_EMPTY_TOPIC"

@EmbeddedKafka(
        partitions = 1, topics = [EXISTING_TOPIC_WITH_RECORDS_LACK],
        brokerProperties = [
            "auto.create.topics.enable=false", "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1"
        ]
)
@DisplayName("Single-partitioned topic lack of records reader test")
class SinglePartitionLackOfRecordsReaderTest : BaseForKafkaIntegrationTest() {


    private val itemsCount = 4

    @BeforeEach
    fun produceRecords() {

        val records = (0 until itemsCount).map { Pair("key", it) }
        sendRecords(embeddedKafka.brokersAsString, EXISTING_TOPIC_WITH_RECORDS_LACK, records)

        SinglePartitionTopicDataPresentLatch(embeddedKafka.brokersAsString, EXISTING_TOPIC_WITH_RECORDS_LACK, String::class.java, Int::class.java).countDownLatch.await()
    }

    @Test
    @DisplayName("Test topic with luck of records returns all records")
    fun testTopicWithLackOfRecords() {

        val itemsCount = 4

        val reader = SinglePartitionTopicLastItemsReader(
                kafkaBrokers = embeddedKafka.brokersAsString, topic = EXISTING_TOPIC_WITH_RECORDS_LACK,
                keyClass = String::class.java, valueClass = Int::class.java
        )
        val records = reader.readLastRecords(itemsCount + 1)

        Assertions.assertEquals(4, records.size)
        (0 until itemsCount).forEach { Assertions.assertEquals(itemsCount - it - 1, records[it].second) }
    }
}