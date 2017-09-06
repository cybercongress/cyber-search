package fund.cyber.node.connectors.configuration

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Type.*


val BTCD_URL = "btdc.url"
val CHUNK_SIZE = "chunk.size"
val BATCH_SIZE = "batch.size"

val batch_size_default = 32

val bitcoinConnectorConfiguration = ConfigDef()
        .define(BTCD_URL, STRING, "http://cyber:cyber@127.0.0.1:8334", HIGH, "Define btdc url.")!!
        .define(CHUNK_SIZE, LONG, 25, HIGH, "Define cassandra row key granularity.")!!
        .define(BATCH_SIZE, INT, batch_size_default, HIGH, "Define number of concurrent requests to btdc.")!!


class BitcoinConnectorConfiguration(
        properties: Map<String, String>
) : AbstractConfig(bitcoinConnectorConfiguration, properties, true) {

    val btcdUrl = getString(BTCD_URL)!!
    val chunkSize = getLong(CHUNK_SIZE)!!
    val batchSize = getInt(BATCH_SIZE)!!
}