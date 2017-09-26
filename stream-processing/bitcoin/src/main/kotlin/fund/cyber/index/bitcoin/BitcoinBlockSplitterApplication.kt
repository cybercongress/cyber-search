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
import com.datastax.driver.mapping.MappingManager
import fund.cyber.index.bitcoin.converter.BitcoinBlockConverter
import fund.cyber.index.bitcoin.converter.BitcoinTransactionConverter
import fund.cyber.node.model.*
import org.apache.kafka.streams.kstream.Predicate
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
                .build().init()

        val transactionConverter = BitcoinTransactionConverter(cache)
        val blockConverter = BitcoinBlockConverter()
        val builder = KStreamBuilder()

        val btcdBlockSerde = Serdes.serdeFrom<BtcdBlock>(JsonSerializer<BtcdBlock>(), JsonDeserializer(BtcdBlock::class.java))
        val bitcoinTransactionSerde = Serdes.serdeFrom<BitcoinTransaction>(JsonSerializer<BitcoinTransaction>(), JsonDeserializer(BitcoinTransaction::class.java))
        val bitcoinBlockSerde = Serdes.serdeFrom<BitcoinBlock>(JsonSerializer<BitcoinBlock>(), JsonDeserializer(BitcoinBlock::class.java))

        val bitcoinItemsStream = builder.stream<Any, BtcdBlock>(null, btcdBlockSerde, IndexTopics.bitcoinSourceTopic)
                .flatMapValues { btcdBlock ->
                    val transactions = tryToHandleTransaction(btcdBlock, cache, cassandra, transactionConverter)
                    val blockAndTransactions = mutableListOf<BitcoinItem>(blockConverter.btcdBlockToDao(btcdBlock, transactions))
                    blockAndTransactions.addAll(transactions)
                    blockAndTransactions
                }
                .branch(
                        Predicate { _, v -> v is BitcoinBlock },
                        Predicate { _, v -> v is BitcoinTransaction }
                )


        bitcoinItemsStream[0].mapValues { v -> v as BitcoinBlock }.to(null, bitcoinBlockSerde, "bitcoin_block")
        bitcoinItemsStream[1].mapValues { v -> v as BitcoinTransaction }.to(null, bitcoinTransactionSerde, "bitcoin_tx")

        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { thread: Thread, throwable: Throwable ->
            log.error("Error during splitting bitcoin block ", throwable)
            streams.close()
            cassandra.close()
        }
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread {
            streams.close()
            cassandra.close()
        })
    }

    private fun tryToHandleTransaction(btcdBlock: BtcdBlock, cache: Cache<String, BitcoinTransaction>,
                                       cassandra: Cluster, transactionConverter: BitcoinTransactionConverter,
                                       tryNumber: Int = 0
    ): List<BitcoinTransaction> {

        return try {
            //refresh cache
            val inputTransactions = loadInputTransactions(btcdBlock, cassandra)
            cache.putAll(inputTransactions)

            transactionConverter.btcdTransactionsToDao(btcdBlock)
        } catch (e: Exception) {
            if (tryNumber > 100) {
                log.error("Error during processing block ${btcdBlock.height}", e)
                throw RuntimeException(e)
            } else {
                Thread.sleep(1000)
                tryToHandleTransaction(btcdBlock, cache, cassandra, transactionConverter, tryNumber + 1)
            }
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
                    block_time = row.getTimestamp("block_time").toInstant().toString(),
                    coinbase = row.getString("coinbase"), block_hash = btcdBlock.hash,
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