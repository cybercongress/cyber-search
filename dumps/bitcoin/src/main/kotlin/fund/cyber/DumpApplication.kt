package fund.cyber

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration

//todo add cassandra schema management
@SpringBootApplication(exclude = [CassandraDataAutoConfiguration::class, KafkaAutoConfiguration::class])
class BitcoinDumpApplication {

    companion object {

        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication(BitcoinDumpApplication::class.java).run(*args)
        }
    }
}