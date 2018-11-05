package fund.cyber.common.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import javax.annotation.PostConstruct


@Tag("kafka-integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringJUnitConfig
@ContextConfiguration(classes = [KafkaEmbeddedHolder::class])
abstract class BaseKafkaIntegrationTest {

    val embeddedKafka = KafkaEmbeddedHolder.getKafka()

    lateinit var adminClient: AdminClient

    @PostConstruct
    fun postConstruct() {
        adminClient = AdminClient.create(adminClientProperties(embeddedKafka.brokersAsString))!!
    }
}


private const val KAFKA_STARTUP_TEST_TOPIC = "KAFKA_STARTUP_TEST_TOPIC"

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
