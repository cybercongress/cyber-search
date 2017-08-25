package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.connectors.configuration.EthereumConnectorConfiguration
import fund.cyber.node.connectors.configuration.batch_size_default
import fund.cyber.node.connectors.model.Block
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.math.BigInteger.*
import java.util.concurrent.Executors

import fund.cyber.node.common.plus


class EthereumSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(EthereumSourceConnectorTask::class.java)

    private lateinit var taskConfiguration: EthereumConnectorConfiguration
    private lateinit var parityClient: Web3j
    private lateinit var jsonSerializer: ObjectMapper

    private lateinit var chunkSize: BigInteger
    private var batchSize: Int = batch_size_default

    private lateinit var lastNetworkBlockOnStartup: BigInteger
    private lateinit var lastParsedBlockNumber: BigInteger


    override fun start(properties: Map<String, String>) {

        taskConfiguration = EthereumConnectorConfiguration(properties)

        chunkSize = BigInteger.valueOf(taskConfiguration.chunkSize)
        batchSize = taskConfiguration.batchSize

        val executorService = Executors.newScheduledThreadPool(batchSize)
        parityClient = Web3j.build(HttpService(taskConfiguration.parityUrl), 15 * 1000, executorService)
        jsonSerializer = ObjectMapper()

        lastParsedBlockNumber = lastParsedBlockNumber()
        lastNetworkBlockOnStartup = parityClient.ethBlockNumber().send().blockNumber
    }


    override fun poll(): List<SourceRecord> {

        try {

            val isBatchFetch = lastNetworkBlockOnStartup - lastParsedBlockNumber > TEN
            val rangeEnd = if (isBatchFetch) batchSize else 1

            val blocks = IntRange(1, rangeEnd)
                    .map { increment -> lastParsedBlockNumber + increment }
                    .map { blockNumber -> blockParameter(blockNumber) }
                    .map { blockParameter -> parityClient.ethGetBlockByNumber(blockParameter, true).sendAsync() }
                    .map { futureBlock ->
                        while (!futureBlock.isDone) Thread.sleep(10)
                        if (futureBlock.isCompletedExceptionally) throw RuntimeException()
                        futureBlock.get()
                    }
                    .map { ethBlock ->
                        Block(
                                chunk_id = (ethBlock.block.number / chunkSize).toString(),
                                number = ethBlock.block.number.toString(),
                                rawBlock = "a" + jsonSerializer.writeValueAsString(ethBlock.block)
                        )
                    }
                    .map { block ->
                        SourceRecord(
                                sourcePartition, sourceOffset(lastParsedBlockNumber), ethereumSourceTopic,
                                STRING_SCHEMA, jsonSerializer.writeValueAsString(block)
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

    private fun lastParsedBlockNumber(): BigInteger {
        val blockNumber = context.offsetStorageReader()?.offset(sourcePartition)?.get("blockNumber") ?: return ZERO
        return BigInteger(blockNumber as String)
    }


    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
    private fun sourceOffset(blockNumber: BigInteger) = mapOf("blockNumber" to blockNumber.toString())
    private val sourcePartition = mapOf("blockchain" to ethereum)

    override fun version(): String = "1.0"
    override fun stop() {}
}
