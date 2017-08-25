package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.connectors.configuration.EthereumConnectorConfiguration
import fund.cyber.node.connectors.model.Block
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.math.BigInteger.TEN
import java.math.BigInteger.ZERO
import java.util.concurrent.Executors


class EthereumSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(EthereumSourceConnectorTask::class.java)

    private lateinit var taskConfiguration: EthereumConnectorConfiguration
    private lateinit var parityClient: Web3j
    private lateinit var jsonSerializer: ObjectMapper

    private lateinit var lastNetworkBlockOnStartup: BigInteger
    private lateinit var lastParsedBlockNumber: BigInteger


    override fun start(properties: Map<String, String>) {
        taskConfiguration = EthereumConnectorConfiguration(properties)
        parityClient = Web3j.build(HttpService(taskConfiguration.parityUrl), 15 * 1000, Executors.newScheduledThreadPool(32))
        jsonSerializer = ObjectMapper()

        lastParsedBlockNumber = lastParsedBlockNumber()
        lastNetworkBlockOnStartup = parityClient.ethBlockNumber().send().blockNumber
    }


    override fun poll(): List<SourceRecord> {

        try {

            val isBatchFetch = lastNetworkBlockOnStartup - lastParsedBlockNumber > TEN
            val batchSize = if (isBatchFetch) 8L else 1L

            val blocks = LongRange(1, batchSize)
                    .map { increment -> lastParsedBlockNumber + BigInteger.valueOf(increment) }
                    .map { blockNumber -> blockParameter(blockNumber) }
                    .map { blockParameter -> parityClient.ethGetBlockByNumber(blockParameter, true).sendAsync() }
                    .map { futureBlock ->
                        while (!futureBlock.isDone) Thread.sleep(10)
                        if (futureBlock.isCompletedExceptionally) throw RuntimeException()
                        futureBlock.get()
                    }
                    .map { ethBlock ->
                        Block(
                                number = ethBlock.block.number.toString(),
                                hash = ethBlock.block.hash,
                                timestamp = ethBlock.block.timestamp.toLong().toString() + "000",
                                rawBlock = jsonSerializer.writeValueAsString(ethBlock.block).removePrefix("{")
                        )
                    }
                    .map { block ->
                        SourceRecord(
                                sourcePartition, sourceOffset(lastParsedBlockNumber), ethereumSourceTopic,
                                STRING_SCHEMA, jsonSerializer.writeValueAsString(block)
                        )

                    }
                    .toList()

            lastParsedBlockNumber += BigInteger.valueOf(batchSize)
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
