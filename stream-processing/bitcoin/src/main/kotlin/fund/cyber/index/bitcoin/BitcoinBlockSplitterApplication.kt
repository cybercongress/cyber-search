package fund.cyber.index.bitcoin

import com.datastax.driver.core.Cluster
import fund.cyber.index.IndexTopics
import fund.cyber.index.btcd.BtcdBlock
import fund.cyber.index.btcd.BtcdRegularTransactionInput
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory
import com.datastax.driver.mapping.MappingManager
import fund.cyber.index.bitcoin.ApplicationContext.cassandra
import fund.cyber.index.bitcoin.ApplicationContext.streamsConfiguration
import fund.cyber.index.bitcoin.ApplicationContext.transactionConverter
import fund.cyber.index.bitcoin.converter.BitcoinBlockConverter
import fund.cyber.node.model.*
import org.apache.kafka.streams.kstream.Predicate
import org.ehcache.Cache

val log = LoggerFactory.getLogger(BitcoinBlockSplitterApplication::class.java)!!

object BitcoinBlockSplitterApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val builder = KStreamBuilder()

        val bitcoinItemsStream = builder.stream<Any, BtcdBlock>(null, btcdBlockSerde, IndexTopics.bitcoinSourceTopic)
                .filter({ _, v ->
                    if (v == null) log.debug("Found null item")
                    v != null
                })
                .flatMapValues { btcdBlock ->
                    log.debug("Processing ${btcdBlock.height} block")
                    convertBtcdBlockToBitcoinItems(btcdBlock)
                }
                .branch(
                        Predicate { _, item -> item is BitcoinBlock },
                        Predicate { _, item -> item is BitcoinTransaction }
                )

        bitcoinItemsStream[0].mapValues { v -> v as BitcoinBlock }.to(null, bitcoinBlockSerde, "bitcoin_block")
        bitcoinItemsStream[1].mapValues { v -> v as BitcoinTransaction }.to(null, bitcoinTransactionSerde, "bitcoin_tx")

        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { _: Thread, throwable: Throwable ->
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
}


private fun convertBtcdBlockToBitcoinItems(
        btcdBlock: BtcdBlock, tryNumber: Int = 0, cache: Cache<String, BitcoinTransaction> = ApplicationContext.cache,
        blockConverter: BitcoinBlockConverter = ApplicationContext.blockConverter): List<BitcoinItem> {

    return try {

        val inputTransactions = loadInputTransactions(btcdBlock)
        cache.putAll(inputTransactions)

        val transactions = transactionConverter.btcdTransactionsToDao(btcdBlock)
        val block = blockConverter.btcdBlockToDao(btcdBlock, transactions)

        mutableListOf<BitcoinItem>().plus(block).plus(transactions)
    } catch (e: Exception) {
        if (tryNumber > 100) {
            log.error("Error during processing block ${btcdBlock.height}", e)
            throw RuntimeException(e)
        } else {
            Thread.sleep(1000)
            convertBtcdBlockToBitcoinItems(btcdBlock, tryNumber + 1)
        }
    }
}


private fun loadInputTransactions(
        btcdBlock: BtcdBlock, cassandra: Cluster = ApplicationContext.cassandra): Map<String, BitcoinTransaction> {

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
                txid = row.getString("txid"), fee = row.getString("fee"), size = row.getInt("size"),
                block_number = row.getLong("block_number"), lock_time = row.getLong("lock_time"),
                total_output = row.getString("total_output"), total_input = row.getString("total_input"),
                block_time = row.getTimestamp("block_time").toInstant().toString(),
                coinbase = row.getString("coinbase"), block_hash = btcdBlock.hash,
                ins = row.getList("ins", BitcoinTransactionIn::class.java),
                outs = row.getList("outs", BitcoinTransactionOut::class.java)
        )
    }.associateBy { tx -> tx.txid }
}