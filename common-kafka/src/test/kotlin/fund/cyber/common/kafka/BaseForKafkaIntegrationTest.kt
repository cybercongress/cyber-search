package fund.cyber.common.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener
import org.springframework.test.context.support.DirtiesContextBeforeModesTestExecutionListener
import org.springframework.test.context.support.DirtiesContextTestExecutionListener
import javax.annotation.PostConstruct


@Tag("kafka-integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringJUnitConfig
@TestExecutionListeners(listeners = [
    DependencyInjectionTestExecutionListener::class,
    DirtiesContextBeforeModesTestExecutionListener::class,
    DirtiesContextTestExecutionListener::class
])
abstract class BaseKafkaIntegrationTest {

    @Autowired
    lateinit var embeddedKafka: KafkaEmbedded

    lateinit var adminClient: AdminClient

    @PostConstruct
    fun postConstruct() {
        adminClient = AdminClient.create(adminClientProperties(embeddedKafka.brokersAsString))!!
    }
}


@EmbeddedKafka(
    controlledShutdown = true,
    brokerProperties = [
        "auto.create.topics.enable=false", "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"
    ]
)
@TestPropertySource(properties = ["KAFKA_BROKERS:\${spring.embedded.kafka.brokers}"])
abstract class BaseKafkaIntegrationTestWithStartedKafka : BaseKafkaIntegrationTest()
