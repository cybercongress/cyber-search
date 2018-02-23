package fund.cyber

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration
import org.springframework.boot.runApplication


@SpringBootApplication(exclude = [CassandraDataAutoConfiguration::class])
class SearchApiApplication


fun main(args: Array<String>) {
    runApplication<SearchApiApplication>(*args)
}