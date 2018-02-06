package fund.cyber.pump.bitcoin.sink

import cyber.search.configuration.KAFKA_BROKERS
import cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import cyber.search.model.chains.BitcoinFamilyChain
import cyber.search.model.events.PumpEvent
import cyber.search.model.events.blockPumpTopic
import cyber.search.model.events.txPumpTopic
import fund.cyber.common.kafka.JsonSerializer
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
open class BitcoinBundleProducerConfiguration {


    @Value("#{systemProperties['$KAFKA_BROKERS'] ?: '$KAFKA_BROKERS_DEFAULT'}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chain: BitcoinFamilyChain

    @Bean
    open fun bitcoinRawTxTopic(): NewTopic {
        return NewTopic(chain.txPumpTopic, 1, 1)
    }

    @Bean
    open fun bitcoinRawBlockTopic(): NewTopic {
        return NewTopic(chain.blockPumpTopic, 1, 1)
    }

    @Bean
    open fun kafkaAdmin(): KafkaAdmin {
        val configs = mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers
        )
        return KafkaAdmin(configs)
    }

    @Bean
    open fun producerFactory(): ProducerFactory<PumpEvent, Any> {
        return DefaultKafkaProducerFactory<PumpEvent, Any>(producerConfigs(), JsonSerializer(), JsonSerializer()).apply {
            setTransactionIdPrefix(chain.name + "_PUMP")
        }
    }

    @Bean
    open fun kafkaTemplate(): KafkaTemplate<PumpEvent, Any> {
        return KafkaTemplate(producerFactory())
    }

    @Bean
    open fun producerConfigs(): Map<String, Any> = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092"
    )

    @Bean
    open fun transactionManager(): KafkaTransactionManager<PumpEvent, Any> {
        return KafkaTransactionManager(producerFactory())
    }


}