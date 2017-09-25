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
import fund.cyber.node.model.BitcoinTransactionIn
import fund.cyber.node.model.BitcoinTransactionOut
import org.ehcache.Cache
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.xml.XmlConfiguration


object BitcoinBlockSplitterApplication {

    private val log = LoggerFactory.getLogger(BitcoinBlockSplitterApplication::class.java)

    @JvmStatic
    fun main(args: Array<String>) {

        val cache = getTransactionCache()
        val streamsConfiguration = StreamConfiguration()

        val cassandra = Cluster.builder()
                .addContactPoint(streamsConfiguration.cassandraServers)
                .build()

        val transactionConverter = BitcoinTransactionConverter(cache)
        val builder = KStreamBuilder()

        val btcdBlockSerde = Serdes.serdeFrom<BtcdBlock>(JsonSerializer<BtcdBlock>(), JsonDeserializer(BtcdBlock::class.java))
        val bitcoinTxBlockSerde = Serdes.serdeFrom<BitcoinTransaction>(JsonSerializer<BitcoinTransaction>(), JsonDeserializer(BitcoinTransaction::class.java))

        builder.stream<Any, BtcdBlock>(null, btcdBlockSerde, IndexTopics.bitcoinSourceTopic)
                .flatMapValues { btcdBlock ->
                    tryToHandleTransaction(btcdBlock, cache, cassandra, transactionConverter)
                }
                .to(null, bitcoinTxBlockSerde, "bitcoin_tx")

        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { thread: Thread, throwable: Throwable ->
            log.error("Error during splitting bitcoin block ", throwable)
            streams.close()
        }
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread {
            streams.close()
            cassandra.close()
        })
    }

    private fun tryToHandleTransaction(btcdBlock: BtcdBlock, cache: Cache<String, BitcoinTransaction>,
                                       cassandra: Cluster, transactionConverter: BitcoinTransactionConverter
    ): List<BitcoinTransaction> {

        return try {
            //refresh cache
            val inputTransactions = loadInputTransactions(btcdBlock, cassandra)
            cache.putAll(inputTransactions)

            transactionConverter.btcdTransactionsToDao(btcdBlock)
        } catch (e: Exception) {
            Thread.sleep(1000)
            tryToHandleTransaction(btcdBlock, cache, cassandra, transactionConverter)
        }
    }

    private fun loadInputTransactions(btcdBlock: BtcdBlock, cassandra: Cluster): Map<String, BitcoinTransaction> {

        val incomingNonCoinbaseTransactionsIds = btcdBlock.rawtx
                .flatMap { transaction -> transaction.vin }
                .filter { txInput -> txInput is BtcdRegularTransactionInput }
                .map { txInput -> (txInput as BtcdRegularTransactionInput).txid }
                .joinToString(separator = "','", postfix = "'", prefix = "'")

        //"''"
        if (incomingNonCoinbaseTransactionsIds.length == 2) return emptyMap()

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(BitcoinTransaction::class.java)

        val resultSet = session.execute("SELECT * FROM bitcoin_tx WHERE txid in ($incomingNonCoinbaseTransactionsIds)")

        return resultSet.map { row ->
            BitcoinTransaction(
                    txId = row.getString("txId"), fee = row.getString("fee"), size = row.getInt("size"),
                    block_number = row.getLong("block_number"), lock_time = row.getLong("lock_time"),
                    total_output = row.getString("total_output"), total_input = row.getString("total_input"),
                    blockTime = row.getTimestamp("blockTime").toInstant().toString(), coinbase = row.getString("coinbase"),
                    ins = row.getList("ins", BitcoinTransactionIn::class.java),
                    outs = row.getList("outs", BitcoinTransactionOut::class.java)
            )
        }.associateBy { tx -> tx.txId }
    }

    private fun getTransactionCache(): Cache<String, BitcoinTransaction> {
        val ehcacheSettingsUri = javaClass.getResource("/ehcache.xml")
        val cacheManager = CacheManagerBuilder.newCacheManager(XmlConfiguration(ehcacheSettingsUri))
        cacheManager.init()
        return cacheManager.getCache("transactions", String::class.java, BitcoinTransaction::class.java)
    }
}