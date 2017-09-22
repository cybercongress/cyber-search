package fund.cyber.index.bitcoin

import fund.cyber.index.IndexTopics
import fund.cyber.index.btcd.BtcdBlock
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory

object BitcoinBlockSplitterApplication {

    private val log = LoggerFactory.getLogger(BitcoinBlockSplitterApplication::class.java)

    @JvmStatic
    fun main(args: Array<String>) {

        val streamsConfiguration = StreamConfiguration()
        val builder = KStreamBuilder()

        val bitcoinBlockSerde = Serdes.serdeFrom<BtcdBlock>(JsonSerializer<BtcdBlock>(), JsonDeserializer(BtcdBlock::class.java))

        builder.stream<Any, BtcdBlock>(null, bitcoinBlockSerde, IndexTopics.bitcoinSourceTopic)
                .flatMapValues { btcdBlock ->
                    val block = getBlockForStorage(btcdBlock)
                    listOf(block)
                }

        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { thread: Thread, throwable: Throwable ->
            log.error("Error during splitting bitcoin block ", throwable)
        }
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}