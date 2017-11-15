package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
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
    private val jsonSerializer: ObjectMapper = jacksonJsonSerializer

    private lateinit var config: BitcoinConnectorConfiguration
    private lateinit var btcdClient: AsyncBtcdClient

    private var batchSize: Int = BATCH_SIZE_DEFAULT
    private var updateInterval: Long = UPDATE_INTERVAL_DEFAULT
    private var lastNetworkBlock: Long = 0
    private var lastParsedBlockNumber: Long = 0


    override fun start(properties: Map<String, String>) {

        config = BitcoinConnectorConfiguration(properties)
        btcdClient = AsyncBtcdClient(config)

        batchSize = config.batchSize
        updateInterval = config.updateInterval

        lastParsedBlockNumber = if (config.startBlock == START_BLOCK_DEFAULT) lastParsedBlockNumber() else config.startBlock
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
                        log.debug("$blockNumber rawResult :$rawResult")
                        jsonDeserializer.readValue(rawResult, BlockResponse::class.java).getRawBlock()
                    }
                    .map { (blockNumber, rawBlock) ->

                        val valueByteArray = jsonSerializer.writeValueAsBytes(rawBlock)

                        SourceRecord(
                                sourcePartition, sourceOffset(blockNumber),
                                "not matter, will be deleted", Schema.BYTES_SCHEMA, valueByteArray
                        )
                    }

            lastParsedBlockNumber += batchSize
            log.info("Obtained ${lastParsedBlockNumber + 1}-${lastParsedBlockNumber + rangeSize} blocks")

            return blocks
        } catch (e: Exception) {
            log.error("Unexpected error during polling bitcoin chain. Last parsed block $lastParsedBlockNumber", e)
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