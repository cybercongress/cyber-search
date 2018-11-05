package fund.cyber.common.kafka.reader

import fund.cyber.common.kafka.BaseKafkaIntegrationTest
import fund.cyber.common.kafka.SinglePartitionTopicDataPresentLatch
import fund.cyber.common.kafka.sendRecordsInTransaction
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test


const val SINGLE_TRANSACTION_RECORD_TOPIC = "SINGLE_TRANSACTION_RECORD_TOPIC"

@DisplayName("Single-partitioned topic last item reader test")
class SinglePartitionSingeTransactionRecordReaderTest : BaseKafkaIntegrationTest() {


    @BeforeEach
    fun produceRecords() {

        sendRecordsInTransaction(
            embeddedKafka.brokersAsString, SINGLE_TRANSACTION_RECORD_TOPIC, listOf("key" to 1)
        )

        SinglePartitionTopicDataPresentLatch(
            embeddedKafka.brokersAsString, SINGLE_TRANSACTION_RECORD_TOPIC, String::class.java, Int::class.java
        ).await()
    }

    @Test
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
