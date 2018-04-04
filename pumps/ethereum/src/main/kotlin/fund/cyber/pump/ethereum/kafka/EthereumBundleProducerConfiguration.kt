package fund.cyber.pump.ethereum.kafka

import fund.cyber.common.kafka.JsonSerializer
import fund.cyber.common.kafka.defaultProducerConfig
import fund.cyber.common.with
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.transaction.KafkaTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement

@EnableKafka
@Configuration
@EnableTransactionManagement
class EthereumBundleProducerConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chain: EthereumFamilyChain

    //todo add topic configuration(retention policy and etc)
    @Bean
    fun bitcoinRawTxTopic(): NewTopic {
        return NewTopic(chain.txPumpTopic, 1, 1)
    }

    @Bean
    fun bitcoinRawBlockTopic(): NewTopic {
        return NewTopic(chain.blockPumpTopic, 1, 1)
    }

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs = mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers
        )
        return KafkaAdmin(configs)
    }

    @Bean
    fun producerFactory(): ProducerFactory<PumpEvent, Any> {
        return DefaultKafkaProducerFactory<PumpEvent, Any>(producerConfigs(), JsonSerializer(), JsonSerializer())
                .apply {
                    setTransactionIdPrefix(chain.name + "_PUMP")
                }
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<PumpEvent, Any> {
        return KafkaTemplate(producerFactory())
    }

    @Bean
    fun producerConfigs(): Map<String, Any> = defaultProducerConfig().with(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers
    )

    @Bean
    fun transactionManager(): KafkaTransactionManager<PumpEvent, Any> {
        return KafkaTransactionManager(producerFactory())
    }
}
