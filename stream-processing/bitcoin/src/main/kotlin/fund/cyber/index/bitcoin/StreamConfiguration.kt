package fund.cyber.index.bitcoin

import com.datastax.driver.core.Cluster
import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.index.bitcoin.converter.BitcoinAddressConverter
import fund.cyber.index.bitcoin.converter.BitcoinBlockConverter
import fund.cyber.index.bitcoin.converter.BitcoinTransactionConverter
import fund.cyber.index.btcd.BtcdBlock
import fund.cyber.node.common.env
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.model.BitcoinAddress
import fund.cyber.node.model.BitcoinAddressTransaction
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.node.model.BitcoinTransaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.ehcache.CacheManager
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.xml.XmlConfiguration
import java.util.*


object AppContext {

    val streamsConfiguration = StreamConfiguration()
    val transactionConverter = BitcoinTransactionConverter()
    val blockConverter = BitcoinBlockConverter()
    val addressConverter = BitcoinAddressConverter()

    val cassandra = Cluster.builder()
            .addContactPoint(streamsConfiguration.cassandraServers)
            .build().init()
            .apply {
                configuration.poolingOptions.maxQueueSize = 10 * 1024
            }

    private val cacheManager = getCacheManager()
    val txCache = cacheManager.getCache("transactions", String::class.java, BitcoinTransaction::class.java)
    val addressCache = cacheManager.getCache("addresses", String::class.java, BitcoinAddress::class.java)

    val bitcoinDaoService = BitcoinDaoService(cassandra, txCache, addressCache)
}


class StreamConfiguration(
        val cassandraServers: String = env("CASSANDRA_CONNECTION", "localhost"),
        private val kafkaServers: String = env("KAFKA_CONNECTION", "localhost:9092"),
        private val applicationIdMinorVersion: String = env("APPLICATION_ID_SUFFIX", "0"),
        private val stateStateCommitTime: Long = env("COMMIT_STATE_MS", 10),
        private val applicationId: String = "cyber.index.bitcoin.block.splitter.v1.$applicationIdMinorVersion"
) {
    fun streamProperties(): Properties {
        return Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 25 * 1024 * 1024)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 9 * 1000)
            put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, stateStateCommitTime)
            put(StreamsConfig.STATE_DIR_CONFIG, "/opt/cyberfund/search/kafka-stream")
        }
    }
}

fun getCacheManager(): CacheManager {
    val ehcacheSettingsUri = BitcoinBlockSplitterApplication::class.java.getResource("/ehcache.xml")
    val cacheManager = CacheManagerBuilder.newCacheManager(XmlConfiguration(ehcacheSettingsUri))
    cacheManager.init()
    return cacheManager
}


val btcdBlockSerde = defaultJsonSerde(BtcdBlock::class.java)
val bitcoinTransactionSerde = defaultJsonSerde(BitcoinTransaction::class.java)
val bitcoinBlockSerde = defaultJsonSerde(BitcoinBlock::class.java)
val bitcoinAddressSerde = defaultJsonSerde(BitcoinAddress::class.java)
val bitcoinAddressTransactionSerde = defaultJsonSerde(BitcoinAddressTransaction::class.java)

private fun <T> defaultJsonSerde(type: Class<T>): Serde<T> {
    return Serdes.serdeFrom(JsonSerializer(), JsonDeserializer(type))!!
}