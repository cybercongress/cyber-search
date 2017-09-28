package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.index.IndexTopics
import fund.cyber.node.common.readTextAndClose
import fund.cyber.node.connectors.client.AsyncBtcdClient
import fund.cyber.node.connectors.client.BlockResponse
import fund.cyber.node.connectors.configuration.*
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory


class BitcoinSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(BitcoinSourceConnectorTask::class.java)

    private val jsonDeserializer: ObjectMapper = jacksonJsonDeserializer

    private lateinit var taskConfiguration: BitcoinConnectorConfiguration
    private lateinit var btcdClient: AsyncBtcdClient

    private var batchSize: Int = BATCH_SIZE_DEFAULT
    private var updateInterval: Long = UPDATE_INTERVAL_DEFAULT
    private var lastNetworkBlock: Long = 0
    private var lastParsedBlockNumber: Long = 0


    override fun start(properties: Map<String, String>) {

        taskConfiguration = BitcoinConnectorConfiguration(properties)
        btcdClient = AsyncBtcdClient(taskConfiguration)

        batchSize = taskConfiguration.batchSize
        updateInterval = taskConfiguration.updateInterval

        lastParsedBlockNumber = lastParsedBlockNumber()
        lastNetworkBlock = btcdClient.getLastBlockNumber()
    }


    /**
     * Using async http client to obtains block. So we have to wait, while future client is done.
     */
    override fun poll(): List<SourceRecord> {

        try {
            val isBatchFetch = lastNetworkBlock - lastParsedBlockNumber > 10
            val rangeSize = if (isBatchFetch) batchSize else 1

            if (!isBatchFetch) {
                lastNetworkBlock = btcdClient.getLastBlockNumber()
                if (lastParsedBlockNumber == lastNetworkBlock) {
                    log.debug("Up-to-date (block $lastParsedBlockNumber), sleeping ${updateInterval}sec")
                    Thread.sleep(updateInterval * 1000)
                    return emptyList()
                }
            }

            log.info("Looking for ${lastParsedBlockNumber + 1}-${lastParsedBlockNumber + rangeSize} blocks")

            val blocks = LongRange(lastParsedBlockNumber + 1, lastParsedBlockNumber + rangeSize)
                    .associateBy({ blockNumber -> blockNumber }, { blockNumber -> btcdClient.getBlockByNumber(blockNumber) })
                    .mapValues { (blockNumber, futureBlock) ->
                        while (!futureBlock.isDone) Thread.sleep(10)
                        futureBlock.get().entity
                    }
                    .mapValues { (blockNumber, httpResponse) ->
                        val rawResult = httpResponse.content.readTextAndClose()
                        jsonDeserializer.readValue(rawResult, BlockResponse::class.java).getRawBlock()
                    }
                    .map { (blockNumber, rawBlock) ->

                        val valueByteArray = rawBlock.toByteArray()

                        log.debug("$blockNumber block : ")
                        log.debug("${valueByteArray.size}")
                        log.debug("${valueByteArray.last()}")
                        SourceRecord(
                                sourcePartition, sourceOffset(blockNumber),
                                IndexTopics.bitcoinSourceTopic, Schema.BYTES_SCHEMA, valueByteArray
                        )
                    }

            lastParsedBlockNumber += batchSize
            log.info("Obtained ${lastParsedBlockNumber + 1}-${lastParsedBlockNumber + rangeSize} blocks")

            return blocks
        } catch (e: Exception) {
            log.error("Unexpected error during polling bitcoin chain", e)
            return emptyList()
        }
    }

    private fun lastParsedBlockNumber(): Long {
        val blockNumber = context.offsetStorageReader()?.offset(sourcePartition)?.get("blockNumber") ?: return 0
        return blockNumber as Long
    }

    private fun sourceOffset(blockNumber: Long) = mapOf("blockNumber" to blockNumber)
    private val sourcePartition = mapOf("blockchain" to BITCOIN_PARTITION)

    override fun version(): String = "1.0"
    override fun stop() {}
}