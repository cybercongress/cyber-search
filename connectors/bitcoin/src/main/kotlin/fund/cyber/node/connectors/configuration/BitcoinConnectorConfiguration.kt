package fund.cyber.node.connectors.configuration

import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.*


const val BTCD_URL = "btdc.url"
const val BATCH_SIZE = "batch.size"
const val UPDATE_INTERVAL = "batch.size"

const val BATCH_SIZE_DEFAULT = 4
const val UPDATE_INTERVAL_DEFAULT = 30L

const val BITCOIN_PARTITION = "bitcoin"


val bitcoinConnectorConfiguration = ConfigDef()
        .define(BTCD_URL, STRING, "http://cyber:cyber@127.0.0.1:8334", HIGH, "Define btdc url.")!!
        .define(BATCH_SIZE, INT, BATCH_SIZE_DEFAULT, HIGH, "Define number of concurrent requests to btdc.")!!
        .define(UPDATE_INTERVAL, LONG, UPDATE_INTERVAL_DEFAULT, MEDIUM, "Define interval for querying new block.")!!


class BitcoinConnectorConfiguration(
        properties: Map<String, String>
) : AbstractConfig(bitcoinConnectorConfiguration, properties, true) {

    val btcdUrl = getString(BTCD_URL)!!
    val batchSize = getInt(BATCH_SIZE)!!
    val updateInterval = getLong(UPDATE_INTERVAL)!!
}


val jacksonJsonSerializer = ObjectMapper().registerKotlinModule()
val jacksonJsonDeserializer = ObjectMapper().registerKotlinModule().configure(FAIL_ON_UNKNOWN_PROPERTIES, false)!!
