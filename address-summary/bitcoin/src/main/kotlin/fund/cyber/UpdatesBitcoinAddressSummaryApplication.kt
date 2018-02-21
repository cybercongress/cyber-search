package fund.cyber

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.bitcoin.repository.BitcoinUpdateAddressSummaryRepository
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer
import java.math.BigDecimal


@SpringBootApplication(exclude = [CassandraDataAutoConfiguration::class, KafkaAutoConfiguration::class])
class UpdatesBitcoinAddressSummaryApplication {

    @Bean
    fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val context = SpringApplication.run(UpdatesBitcoinAddressSummaryApplication::class.java, *args)
//            val repository = context.getBean(BitcoinUpdateAddressSummaryRepository::class.java)
//            val summary = repository.findById("a1").block()!!
//            val newSummary = CqlBitcoinAddressSummary(
//                    id = "a3", confirmed_balance = summary.confirmed_balance + BigDecimal.valueOf(10),
//                    confirmed_total_received = summary.confirmed_total_received + BigDecimal.valueOf(10),
//                    confirmed_tx_number = summary.confirmed_tx_number + 1, kafka_delta_offset = 4,
//                    kafka_delta_topic = summary.kafka_delta_topic, kafka_delta_partition = summary.kafka_delta_partition,
//                    kafka_delta_offset_committed = false, unconfirmed_tx_values = summary.unconfirmed_tx_values,
//                    version = 3
//            )
//            val valueFromCSS = repository.insertIfNotExists(newSummary).block()
//
//            println(valueFromCSS)

        }
//        INSERT INTO address_summary (id, confirmed_balance, confirmed_tx_number, confirmed_total_received , version, kafka_delta_offset, kafka_delta_topic, kafka_delta_partition, kafka_delta_offset_commited ) VALUES ( 'a1', 10, 1, 10, 2, 3, 'topic', 1, true) ;

    }
}

