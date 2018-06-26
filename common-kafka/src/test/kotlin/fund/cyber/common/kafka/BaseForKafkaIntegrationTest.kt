package fund.cyber.common.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import javax.annotation.PostConstruct


@Tag("kafka-integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringJUnitConfig
abstract class BaseKafkaIntegrationTest {

    @Autowired
    lateinit var embeddedKafka: KafkaEmbedded

    lateinit var adminClient: AdminClient

    @PostConstruct
    fun postConstruct() {
        adminClient = AdminClient.create(adminClientProperties(embeddedKafka.brokersAsString))!!
    }
}


private const val KAFKA_STARTUP_TEST_TOPIC = "KAFKA_STARTUP_TEST_TOPIC"

@EmbeddedKafka(
    topics = [KAFKA_STARTUP_TEST_TOPIC], partitions = 1,
    brokerProperties = [
        "auto.create.topics.enable=false", "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"
    ]
)
@TestPropertySource(properties = ["KAFKA_BROKERS:\${spring.embedded.kafka.brokers}"])
abstract class BaseKafkaIntegrationTestWithStartedKafka : BaseKafkaIntegrationTest() {

    @BeforeAll
    fun waitContainerToStart() {

        sendRecordsInTransaction(embeddedKafka.brokersAsString, KAFKA_STARTUP_TEST_TOPIC, listOf(Pair("key", 0)))

        SinglePartitionTopicDataPresentLatch(
            embeddedKafka.brokersAsString, KAFKA_STARTUP_TEST_TOPIC, String::class.java, Int::class.java
        ).await()
    }
}
