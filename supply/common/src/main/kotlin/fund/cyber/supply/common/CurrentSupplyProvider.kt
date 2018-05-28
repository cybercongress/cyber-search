package fund.cyber.supply.common

import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.search.configuration.GENESIS_SUPPLY
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.supplyTopic
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.math.BigDecimal


private val log = LoggerFactory.getLogger(CurrentSupplyProvider::class.java)!!

@Component
class CurrentSupplyProvider {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Value("\${$GENESIS_SUPPLY:}")
    private lateinit var genesisSupply: String

    @Autowired
    lateinit var chainInfo: ChainInfo

    fun <T> getLastCalculatedSupply(supplyClass: Class<T>, genesisSupplyCreator: (genesis: BigDecimal) -> T): T {

        val topicReader = SinglePartitionTopicLastItemsReader(
            kafkaBrokers = kafkaBrokers, topic = chainInfo.supplyTopic,
            keyClass = Any::class.java, valueClass = supplyClass
        )

        val keyToValue = topicReader.readLastRecords(1).firstOrNull()

        return if (keyToValue != null) {
            keyToValue.second
        } else {
            if (genesisSupply.trim().isEmpty()) {
                log.error("Please specify env variable `GENESIS_SUPPLY`. " +
                    "For example, initial Ethereum supply is 72009990.50")
                throw RuntimeException("`GENESIS_SUPPLY` is not provided")
            }
            genesisSupplyCreator(BigDecimal(genesisSupply))
        }
    }
}