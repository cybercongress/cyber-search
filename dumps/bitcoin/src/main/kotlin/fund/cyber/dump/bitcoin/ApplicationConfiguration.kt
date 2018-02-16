package fund.cyber.dump.bitcoin

import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.configuration.env
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties


//todo add dump of tx, block tx, address tx
@EnableKafka
@Configuration
open class ApplicationConfiguration {

    @Value("#{systemProperties['$KAFKA_BROKERS'] ?: '$KAFKA_BROKERS_DEFAULT'}")
    private lateinit var kafkaBrokers: String


    @Autowired
    lateinit var bitcoinBlockRepository: BitcoinBlockRepository

    @Bean
    open fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }

    @Bean
    open fun blocksListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, BitcoinBlock> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "bitcoin-blocks-dump-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(BitcoinBlock::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain().blockPumpTopic).apply {
            messageListener = BlockDumpProcess(bitcoinBlockRepository, chain())
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    private fun consumerConfigs(): MutableMap<String, Any> = mutableMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to 10 * 1000,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase()
    )
}