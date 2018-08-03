package fund.cyber.supply.bitcoin

import fund.cyber.common.kafka.getTopicConfig
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.supplyTopic
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.listener.AbstractMessageListenerContainer
import org.springframework.kafka.listener.KafkaMessageListenerContainer

@DisplayName("Application context start-up configuration test")
class ApplicationConfigurationTest : BitcoinSupplyBaseTest() {

    @Autowired
    lateinit var chainInfo: ChainInfo

    @Autowired
    lateinit var messageListenerContainer: KafkaMessageListenerContainer<PumpEvent, ByteArray>

    @Test
    @DisplayName("Should correctly create supply topic")
    fun shouldCorrectlyCreateSupplyTopic() {
        val topicConfig = adminClient.getTopicConfig(chainInfo.supplyTopic)!!
        Assertions.assertEquals("52428800", topicConfig.get(TopicConfig.RETENTION_BYTES_CONFIG).value())
    }

    @Test
    @DisplayName("Should use message transaction message listener container")
    fun shouldUseTransactionMessageListenerContainer() {
        val properties = messageListenerContainer.containerProperties
        Assertions.assertEquals(AbstractMessageListenerContainer.AckMode.BATCH, properties.ackMode)
        Assertions.assertNotNull(properties.transactionManager)
    }
}