package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.index.IndexTopics
import fund.cyber.node.common.readTextAndClose
import fund.cyber.node.connectors.client.AsyncBtcdClient
import fund.cyber.node.connectors.client.BlockResponse
import fund.cyber.node.connectors.configuration.BitcoinConnectorConfiguration
import fund.cyber.node.connectors.configuration.batch_size_default
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory


class BitcoinSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(BitcoinSourceConnectorTask::class.java)

    private lateinit var taskConfiguration: BitcoinConnectorConfiguration
    private lateinit var btcdClient: AsyncBtcdClient
    private lateinit var jsonSerializer: ObjectMapper
    private lateinit var jsonDeserializer: ObjectMapper

    private var chunkSize: Long = 1
    private var batchSize: Int = batch_size_default

    private var lastNetworkBlockOnStartup: Long = 0
    private var lastParsedBlockNumber: Long = 0


    override fun start(properties: Map<String, String>) {

        taskConfiguration = BitcoinConnectorConfiguration(properties)

        chunkSize = taskConfiguration.chunkSize
        batchSize = taskConfiguration.batchSize

        btcdClient = AsyncBtcdClient(taskConfiguration)
        jsonSerializer = ObjectMapper()
        jsonDeserializer = ObjectMapper()

        lastParsedBlockNumber = lastParsedBlockNumber()
        lastNetworkBlockOnStartup = btcdClient.getLastBlockNumber()
    }


    override fun poll(): List<SourceRecord> {

        try {

            val isBatchFetch = lastNetworkBlockOnStartup - lastParsedBlockNumber > 10
            val rangeSize = if (isBatchFetch) batchSize else 1

            val blocks = LongRange(lastParsedBlockNumber + 1, lastParsedBlockNumber + rangeSize)
                    .associateBy({ blockNumber -> blockNumber }, { blockNumber -> btcdClient.getBlockByNumber(blockNumber) })
                    .mapValues { (blockNumber, futureBlock) ->
                        while (!futureBlock.isDone) Thread.sleep(10)
                        futureBlock.get().entity
                    }
                    .mapValues { (blockNumber, httpEntity) ->
                        val rawResult = httpEntity.content.readTextAndClose()
                        log.debug(rawResult)
                        jsonDeserializer.readValue(rawResult, BlockResponse::class.java).getRawBlock()
                    }
                    .map { (blockNumber, rawBlock) ->
                        SourceRecord(
                                sourcePartition, sourceOffset(blockNumber), IndexTopics.bitcoinSourceTopic,
                                null, jsonSerializer.writeValueAsString(rawBlock)
                        )

                    }
                    .toList()

            lastParsedBlockNumber += batchSize
            return blocks
        } catch (e: Exception) {
            log.error("Unexpected error during polling ethereum chain", e)
            return emptyList()
        }
    }

    private fun lastParsedBlockNumber(): Long {
        val blockNumber = context.offsetStorageReader()?.offset(sourcePartition)?.get("blockNumber") ?: return 0
        return blockNumber as Long
    }

    private fun sourceOffset(blockNumber: Long) = mapOf("blockNumber" to blockNumber)
    private val sourcePartition = mapOf("blockchain" to bitcoin)

    override fun version(): String = "1.0"
    override fun stop() {}
}