package fund.cyber.index.bitcoin

import com.datastax.driver.core.Cluster
import fund.cyber.index.IndexTopics
import fund.cyber.index.btcd.BtcdBlock
import fund.cyber.index.btcd.BtcdRegularTransactionInput
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory
import fund.cyber.node.model.BitcoinTransaction
import com.datastax.driver.mapping.MappingManager
import fund.cyber.index.bitcoin.converter.BitcoinTransactionConverter


object BitcoinBlockSplitterApplication {

    private val log = LoggerFactory.getLogger(BitcoinBlockSplitterApplication::class.java)

    @JvmStatic
    fun main(args: Array<String>) {

        val streamsConfiguration = StreamConfiguration()

        val cassandra = Cluster.builder()
                .addContactPoint(streamsConfiguration.cassandraServers)
                .build()

        val transactionConverter = BitcoinTransactionConverter()
        val builder = KStreamBuilder()

        val bitcoinBlockSerde = Serdes.serdeFrom<BtcdBlock>(JsonSerializer<BtcdBlock>(), JsonDeserializer(BtcdBlock::class.java))

        builder.stream<Any, BtcdBlock>(null, bitcoinBlockSerde, IndexTopics.bitcoinSourceTopic)
                .flatMapValues { btcdBlock ->

                    val inputTransactions = loadInputTransactions(btcdBlock, cassandra)
                    val newTransactionsByBlock = transactionConverter.btcdTransactionsToDao(btcdBlock, inputTransactions)
                    listOf(newTransactionsByBlock)
                }
                .to("bitcoin_tx")

        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { thread: Thread, throwable: Throwable ->
            log.error("Error during splitting bitcoin block ", throwable)
        }
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread {
            streams.close()
            cassandra.close()
        })
    }

    private fun loadInputTransactions(btcdBlock: BtcdBlock, cassandra: Cluster): Map<String, BitcoinTransaction> {

        val incomingNonCoinbaseTransactionsIds = btcdBlock.rawtx
                .flatMap { transaction -> transaction.vin }
                .filter { txInput -> txInput is BtcdRegularTransactionInput }
                .map { txInput -> (txInput as BtcdRegularTransactionInput).txid }
                .joinToString(separator = ",")

        if (incomingNonCoinbaseTransactionsIds.isEmpty()) return emptyMap()

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(BitcoinTransaction::class.java)

        val resultSet = session.execute("SELECT * FROM bitcoin_tx WHERE hash in ($incomingNonCoinbaseTransactionsIds)")

        return mapper.map(resultSet).associateBy { tx -> tx.txId }
    }
}