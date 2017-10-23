package fund.cyber.index.ethereum

import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.index.IndexTopics.ethereumSourceTopic
import fund.cyber.index.ethereum.ApplicationContext.cassandra
import fund.cyber.index.ethereum.ApplicationContext.streamsConfiguration
import fund.cyber.index.ethereum.converter.EthereumAddressConverter
import fund.cyber.index.ethereum.converter.EthereumParityToDaoConverter
import fund.cyber.node.model.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Predicate
import org.ehcache.Cache
import org.slf4j.LoggerFactory
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigInteger


val log = LoggerFactory.getLogger(EthereumBlockSplitterApplication::class.java)!!

object EthereumBlockSplitterApplication {

    var lastProcessedBlockNumber = BigInteger(ApplicationContext.streamsConfiguration.processLastBlock.toString())

    @JvmStatic
    fun main(args: Array<String>) {

        val builder = KStreamBuilder()

        val ethereumItemsStream = builder.stream<Any, EthBlock.Block>(null, parityBlockSerde, ethereumSourceTopic)
                .filter({ _, v ->
                    if (v == null) log.debug("Found null item")
                    v != null
                })
                .filter({ _, parityBlock ->
                    if (lastProcessedBlockNumber >= parityBlock.number) log.debug("Skipping ${parityBlock.numberRaw} block")
                    lastProcessedBlockNumber < parityBlock.number
                })
                .flatMapValues { parityBlock ->
                    log.debug("Processing ${parityBlock.number} block")
                    lastProcessedBlockNumber = parityBlock.number
                    convertParityBlockToEthereumItems(parityBlock)
                }
                .branch(
                        Predicate { _, item -> item is EthereumBlock },
                        Predicate { _, item -> item is EthereumTransaction },
                        Predicate { _, item -> item is EthereumAddress },
                        Predicate { _, item -> item is EthereumAddressTransaction }
                )

        ethereumItemsStream[0].mapValues { v -> v as EthereumBlock }.to(null, ethereumBlockSerde, "ethereum_block")
        ethereumItemsStream[1].mapValues { v -> v as EthereumTransaction }.to(null, ethereumTxSerde, "ethereum_tx")
        ethereumItemsStream[2].mapValues { v -> v as EthereumAddress }.to(null, ethereumAddressSerde, "ethereum_address")
        ethereumItemsStream[3].mapValues { v -> v as EthereumAddressTransaction }
                .to(null, ethereumAddressTransactionSerde, "ethereum_address_tx")


        val streams = KafkaStreams(builder, streamsConfiguration.streamProperties())

        streams.setUncaughtExceptionHandler { _: Thread, throwable: Throwable ->
            log.error("Error during splitting ethereum block ", throwable)
            System.exit(0)
        }
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread {
            cassandra.close()
            streams.close()
        })
    }

    private fun convertParityBlockToEthereumItems(
            parityBlock: EthBlock.Block, tryNumber: Int = 0,
            addressCache: Cache<String, EthereumAddress> = ApplicationContext.addressCache,
            addressConverter: EthereumAddressConverter = ApplicationContext.addressConverter,
            converter: EthereumParityToDaoConverter = ApplicationContext.parityToDaoConverter): List<EthereumItem> {

        return try {

            val transactions = converter.parityTransactionsToDao(parityBlock)
            val block = converter.parityBlockToDao(parityBlock)

            val existingAddressesUsedInBlock = getExistingAddressesUsedInBlock(block)
            val updatedAddresses = addressConverter.updateAddressesSummary(block, existingAddressesUsedInBlock)
            val addressesTransactions = addressConverter.transactionsPreviewsForAddresses(block)

            updatedAddresses.forEach { address -> addressCache.put(address.id, address) }


            mutableListOf<EthereumItem>().plus(block).plus(transactions).plus(updatedAddresses).plus(addressesTransactions)
        } catch (e: Exception) {
            if (tryNumber > 100) {
                log.error("Error during processing block ${parityBlock.number}", e)
                throw RuntimeException(e)
            } else {
                Thread.sleep(1000)
                convertParityBlockToEthereumItems(parityBlock, tryNumber + 1)
            }
        }
    }

    private fun getExistingAddressesUsedInBlock(
            block: EthereumBlock,
            ethereumDaoService: EthereumDaoService = ApplicationContext.ethereumDaoService): List<EthereumAddress> {

        return ethereumDaoService.getAddressesWithLastTransactionBeforeGivenBlock(block.addressesUsedInBlock(), block.number)
    }
}