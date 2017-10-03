package fund.cyber.node.connectors.configuration

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Type.*


val PARITY_URL = "parity.url"
val BATCH_SIZE = "batch.size"

val BATCH_SIZE_DEFAULT = 32

val ethereumConnectorConfiguration = ConfigDef()
        .define(PARITY_URL, STRING, "http://127.0.0.1:8545", HIGH, "Define parity url.")!!
        .define(BATCH_SIZE, INT, BATCH_SIZE_DEFAULT, HIGH, "Define number of concurrent requests to parity.")!!


class EthereumConnectorConfiguration(
        properties: Map<String, String>
) : AbstractConfig(ethereumConnectorConfiguration, properties, true) {

    val parityUrl = getString(PARITY_URL)!!
    val batchSize = getInt(BATCH_SIZE)!!
}

val jacksonJsonSerializer = ObjectMapper().registerKotlinModule()
val jacksonJsonDeserializer = ObjectMapper().registerKotlinModule().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!
