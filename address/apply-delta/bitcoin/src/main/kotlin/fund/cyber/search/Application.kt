package fund.cyber.search

import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.common.kafka.JsonDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.RECORD
import org.springframework.kafka.listener.ContainerStoppingErrorHandler


fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}


@EnableKafka
@SpringBootApplication
open class Application {

    @Bean
    open fun consumerConfigs(): Map<String, Any> = mapOf(
            BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            AUTO_OFFSET_RESET_CONFIG to "earliest",
            GROUP_ID_CONFIG to "bitcoin-apply-address-summary-deltas-process",
            ENABLE_AUTO_COMMIT_CONFIG to false,
            ISOLATION_LEVEL_CONFIG to READ_COMMITTED.toString().toLowerCase()
    )

    @Bean
    open fun consumerFactory(): ConsumerFactory<String, BitcoinTx> {
        return DefaultKafkaConsumerFactory(
                consumerConfigs(), JsonDeserializer(String::class.java), JsonDeserializer(BitcoinTx::class.java)
        )
    }

    @Bean
    open fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, BitcoinTx> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, BitcoinTx>()
        factory.consumerFactory = consumerFactory()
        //factory.setConcurrency(6)
        factory.containerProperties.ackMode = RECORD
        factory.containerProperties.isAckOnError = false
        factory.containerProperties.setErrorHandler(ContainerStoppingErrorHandler())
        return factory
    }
}

