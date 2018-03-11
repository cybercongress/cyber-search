package fund.cyber.common.kafka

import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener
import org.springframework.test.context.support.DirtiesContextBeforeModesTestExecutionListener
import org.springframework.test.context.support.DirtiesContextTestExecutionListener

@Tag("kafka-integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringJUnitConfig
@TestExecutionListeners(listeners = [
    DependencyInjectionTestExecutionListener::class,
    DirtiesContextBeforeModesTestExecutionListener::class,
    DirtiesContextTestExecutionListener::class
])
abstract class BaseForKafkaIntegrationTest {

    @Autowired
    lateinit var embeddedKafka: KafkaEmbedded
}