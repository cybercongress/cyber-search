package fund.cyber.index.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.index.bitcoin.AppContext.cassandra
import fund.cyber.index.bitcoin.AppContext.streamsConfiguration
import fund.cyber.index.bitcoin.AppContext.transactionConverter
import fund.cyber.index.bitcoin.converter.BitcoinAddressConverter
import fund.cyber.index.bitcoin.converter.BitcoinBlockConverter
import fund.cyber.node.model.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Predicate
import org.ehcache.Cache
import org.slf4j.LoggerFactory

val log = LoggerFactory.getLogger(BitcoinBlockSplitterApplication::class.java)!!

object BitcoinBlockSplitterApplication {

    var lastProcessedBlockNumber = AppContext.streamsConfiguration.processLastBlock

    @JvmStatic
    fun main(args: Array<String>) {

        val builder = KStreamBuilder()

        val bitcoinItemsStream = builder.stream<Any, JsonRpcBitcoinBlock>(null, btcdBlockSerde, " source")
                .filter({ _, v ->
                    if (v == null) log.debug("Found null item")
                    v != null
                })
                .filter({ _, btcdBlock ->
                    if (lastProcessedBlockNumber >= btcdBlock.height) log.debug("Skipping ${btcdBlock.height} block")
                    lastProcessedBlockNumber < btcdBlock.height
                })
                .flatMapValues { btcdBlock ->
                    log.debug("Processing ${btcdBlock.height} block")
                    lastProcessedBlockNumber = btcdBlock.height
                    convertBtcdBlockToBitcoinItems(btcdBlock)
                }
                .branch(
                        Predicate { _, item -> item is BitcoinBlock },
                        Predicate { _, item -> item is BitcoinTransaction },
                        Predicate { _, item -> item is BitcoinAddress },
                        Predicate { _, item -> item is BitcoinAddressTransaction }
                )

        bitcoinItemsStream[0].mapValues { v -> v as BitcoinBlock }.to(null, bitcoinBlockSerde, "bitcoin_block")
        bitcoinItemsStream[1].mapValues { v -> v as BitcoinTransaction }.to(null, bitcoinTransactionSerde, "bitcoin_tx")
        bitcoinItemsStream[2].mapValues { v -> v as BitcoinAddress }.to(null, bitcoinAddressSerde, "bitcoin_address")
        bitcoinItemsStream[3].mapValues { v -> v as BitcoinAddressTransaction }
                .to(null, bitcoinAddressTransactionSerde, "bitcoin_address_tx")

        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { _: Thread, throwable: Throwable ->
            log.error("Error during splitting bitcoin block ", throwable)
            System.exit(0)
        }
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread {
            cassandra.close()
            streams.close()
        })
    }
}


private fun convertBtcdBlockToBitcoinItems(
        jsonRpcBlock: JsonRpcBitcoinBlock, tryNumber: Int = 0,
        txCache: Cache<String, BitcoinTransaction> = AppContext.txCache,
        addressCache: Cache<String, BitcoinAddress> = AppContext.addressCache,
        blockConverter: BitcoinBlockConverter = AppContext.blockConverter,
        addressConverter: BitcoinAddressConverter = AppContext.addressConverter): List<BitcoinItem> {

    return try {

        val inputTransactions = getTransactionsInputs(jsonRpcBlock)
        val transactions = transactionConverter.btcdTransactionsToDao(jsonRpcBlock, inputTransactions)
        val block = blockConverter.btcdBlockToDao(jsonRpcBlock, transactions)

        val existingAddressesUsedInBlock = getExistingAddressesUsedInBlock(block)
        val updatedAddresses = addressConverter.updateAddressesSummary(transactions, existingAddressesUsedInBlock)
        val addressesTransactions = addressConverter.transactionsPreviewsForAddresses(transactions)

        transactions.forEach { tx -> txCache.put(tx.txid, tx) }
        updatedAddresses.forEach { address -> addressCache.put(address.id, address) }

        mutableListOf<BitcoinItem>().plus(block).plus(transactions).plus(updatedAddresses).plus(addressesTransactions)
    } catch (e: Exception) {
        if (tryNumber > 100) {
            log.error("Error during processing block ${jsonRpcBlock.height}", e)
            throw RuntimeException(e)
        } else {
            Thread.sleep(1000)
            convertBtcdBlockToBitcoinItems(jsonRpcBlock, tryNumber + 1)
        }
    }
}

private fun getExistingAddressesUsedInBlock(
        block: BitcoinBlock, bitcoinDaoService: BitcoinDaoService = AppContext.bitcoinDaoService): List<BitcoinAddress> {
/*
    val addressesIds = block.txs.flatMap { tx -> tx.allAddressesUsedInTransaction() }
    return bitcoinDaoService.getAddressesWithLastTransactionBeforeGivenBlock(addressesIds, block.height)*/
    return emptyList()
}


private fun getTransactionsInputs(
        jsonRpcBlock: JsonRpcBitcoinBlock,
        bitcoinDaoService: BitcoinDaoService = AppContext.bitcoinDaoService): List<BitcoinTransaction> {

    val incomingNonCoinbaseTransactionsIds = jsonRpcBlock.rawtx
            .flatMap { transaction -> transaction.vin }
            .filter { txInput -> txInput is RegularTransactionInput }
            .map { txInput -> (txInput as RegularTransactionInput).txid }

    return bitcoinDaoService.getTxs(incomingNonCoinbaseTransactionsIds)
}