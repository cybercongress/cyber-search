package fund.cyber.pump.ethereum_classic

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
//import org.apache.kafka.common.config.AbstractConfig
//import org.apache.kafka.common.config.ConfigDef
//import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
//import org.apache.kafka.common.config.ConfigDef.Type.*
import java.math.BigInteger
//
//
val PARITY_URL = "http://127.0.0.1:8545"//"parity.url"
val BATCH_SIZE = "batch.size"
const val START_BLOCK = "start.block"
//
const val BATCH_SIZE_DEFAULT: Long = 32
const val START_BLOCK_DEFAULT = "-1"
//
//const val ETHEREUM_PARTITION = "ethereum"
//
//val ethereumConnectorConfiguration = ConfigDef()
//        .define(PARITY_URL, STRING, "http://127.0.0.1:8545", HIGH, "Define parity url.")!!
//        .define(BATCH_SIZE, INT, BATCH_SIZE_DEFAULT, HIGH, "Define number of concurrent requests to parity.")!!
//        .define(START_BLOCK, STRING, START_BLOCK_DEFAULT, ConfigDef.Importance.LOW, "Define start block.")!!
//
//
//class EthereumConnectorConfiguration(
//        properties: Map<String, String>
//) /*: AbstractConfig(ethereumConnectorConfiguration, properties, true)*/ {
//
//    val parityUrl = getString(PARITY_URL)!!
//    val batchSize = getInt(BATCH_SIZE)!!
//    val startBlock = BigInteger(getString(START_BLOCK)!!)
//    val startBlockRaw = getString(START_BLOCK)!!
//}
//
val jacksonJsonSerializer = ObjectMapper().registerKotlinModule()
        .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
//
//val jacksonJsonDeserializer = ObjectMapper().registerKotlinModule().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!
