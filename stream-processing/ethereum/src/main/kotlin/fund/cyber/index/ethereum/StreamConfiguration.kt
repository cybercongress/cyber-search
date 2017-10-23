package fund.cyber.index.ethereum

import com.datastax.driver.core.Cluster
import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.index.ethereum.converter.EthereumAddressConverter
import fund.cyber.index.ethereum.converter.EthereumParityToDaoConverter
import fund.cyber.node.common.env
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.model.EthereumAddress
import fund.cyber.node.model.EthereumAddressTransaction
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.ehcache.CacheManager
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.xml.XmlConfiguration
import org.web3j.protocol.core.methods.response.EthBlock
import java.util.*

object ApplicationContext {

    val streamsConfiguration = StreamConfiguration()
    val parityToDaoConverter = EthereumParityToDaoConverter()
    val addressConverter = EthereumAddressConverter()

    val cassandra = Cluster.builder()
            .addContactPoint(streamsConfiguration.cassandraServers)
            .build().init()
            .apply {
                configuration.poolingOptions.maxQueueSize = 10 * 1024
            }

    private val cacheManager = getCacheManager()
    val addressCache = cacheManager.getCache("addresses", String::class.java, EthereumAddress::class.java)

    val ethereumDaoService = EthereumDaoService(cassandra, addressCache)
}

class StreamConfiguration(
        val cassandraServers: String = env("CASSANDRA_CONNECTION", "localhost"),
        private val kafkaServers: String = env("KAFKA_CONNECTION", "localhost:9092"),
        private val stateStateCommitTime: Long = env("COMMIT_STATE_MS", 10),
        val processLastBlock: Long = env("PROCESS_LAST_BLOCK", -1L),
        private val applicationIdMinorVersion: String = env("APPLICATION_ID_SUFFIX", "0"),
        private val applicationId: String = "cyber.index.ethereum.block.splitter.v1.$applicationIdMinorVersion"
) {
    fun streamProperties(): Properties {
        return Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 25 * 1024 * 1024)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 9 * 1000)
            put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, stateStateCommitTime)
            put(StreamsConfig.STATE_DIR_CONFIG, "/opt/cyberfund/search/kafka-stream")
            put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60 * 1000)
            put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100L)
        }
    }
}

val parityBlockSerde = defaultJsonSerde(EthBlock.Block::class.java)
val ethereumTxSerde = defaultJsonSerde(EthereumTransaction::class.java)
val ethereumBlockSerde = defaultJsonSerde(EthereumBlock::class.java)
val ethereumAddressSerde = defaultJsonSerde(EthereumAddress::class.java)
val ethereumAddressTransactionSerde = defaultJsonSerde(EthereumAddressTransaction::class.java)

private fun <T> defaultJsonSerde(type: Class<T>): Serde<T> {
    return Serdes.serdeFrom(JsonSerializer(), JsonDeserializer(type))!!
}

fun getCacheManager(): CacheManager {
    val ehcacheSettingsUri = EthereumBlockSplitterApplication::class.java.getResource("/ehcache.xml")
    val cacheManager = CacheManagerBuilder.newCacheManager(XmlConfiguration(ehcacheSettingsUri))
    cacheManager.init()
    return cacheManager
}