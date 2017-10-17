package fund.cyber.index.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.index.IndexTopics
import fund.cyber.index.bitcoin.AppContext.cassandra
import fund.cyber.index.bitcoin.AppContext.streamsConfiguration
import fund.cyber.index.bitcoin.AppContext.transactionConverter
import fund.cyber.index.bitcoin.converter.BitcoinBlockConverter
import fund.cyber.index.btcd.BtcdBlock
import fund.cyber.index.btcd.BtcdRegularTransactionInput
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.node.model.BitcoinItem
import fund.cyber.node.model.BitcoinTransaction
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Predicate
import org.ehcache.Cache
import org.slf4j.LoggerFactory

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
        btcdBlock: BtcdBlock, tryNumber: Int = 0,
        txCache: Cache<String, BitcoinTransaction> = AppContext.txCache,
        blockConverter: BitcoinBlockConverter = AppContext.blockConverter): List<BitcoinItem> {

    return try {

        val inputTransactions = getTransactionsInputs(btcdBlock)

        val transactions = transactionConverter.btcdTransactionsToDao(btcdBlock, inputTransactions)
        val block = blockConverter.btcdBlockToDao(btcdBlock, transactions)

        transactions.forEach { tx -> txCache.put(tx.txid, tx) }

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


private fun getTransactionsInputs(
        btcdBlock: BtcdBlock,
        bitcoinDaoService: BitcoinDaoService = AppContext.bitcoinDaoService): List<BitcoinTransaction> {

    val incomingNonCoinbaseTransactionsIds = btcdBlock.rawtx
            .flatMap { transaction -> transaction.vin }
            .filter { txInput -> txInput is BtcdRegularTransactionInput }
            .map { txInput -> (txInput as BtcdRegularTransactionInput).txid }

    return bitcoinDaoService.getTxsByIds(incomingNonCoinbaseTransactionsIds)
}