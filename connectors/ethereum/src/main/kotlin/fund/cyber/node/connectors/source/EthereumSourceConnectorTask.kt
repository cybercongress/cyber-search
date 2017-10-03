package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.index.IndexTopics
import fund.cyber.node.common.plus
import fund.cyber.node.connectors.configuration.BATCH_SIZE_DEFAULT
import fund.cyber.node.connectors.configuration.EthereumConnectorConfiguration
import fund.cyber.node.connectors.configuration.jacksonJsonSerializer
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.math.BigInteger.TEN
import java.util.concurrent.Executors


class EthereumSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(EthereumSourceConnectorTask::class.java)

    private val jsonSerializer: ObjectMapper = jacksonJsonSerializer

    private lateinit var taskConfiguration: EthereumConnectorConfiguration
    private lateinit var parityClient: Web3j

    private var batchSize: Int = BATCH_SIZE_DEFAULT

    private lateinit var lastNetworkBlock: BigInteger
    private lateinit var lastParsedBlockNumber: BigInteger


    override fun start(properties: Map<String, String>) {

        val executorService = Executors.newScheduledThreadPool(batchSize)

        taskConfiguration = EthereumConnectorConfiguration(properties)
        parityClient = Web3j.build(HttpService(taskConfiguration.parityUrl), 15 * 1000, executorService)

        batchSize = taskConfiguration.batchSize

        lastParsedBlockNumber = lastParsedBlockNumber()
        lastNetworkBlock = parityClient.ethBlockNumber().send().blockNumber
    }


    override fun poll(): List<SourceRecord> {

        try {

            val isBatchFetch = lastNetworkBlock - lastParsedBlockNumber > TEN
            val rangeEnd = if (isBatchFetch) batchSize else 1

            if (!isBatchFetch) {
                lastNetworkBlock = parityClient.ethBlockNumber().send().blockNumber
                if (lastParsedBlockNumber == lastNetworkBlock) {
                    log.debug("Up-to-date (block $lastParsedBlockNumber)")
                    Thread.sleep(5000)
                    return emptyList()
                }
            }

            log.info("Looking for ${lastParsedBlockNumber + 1}-${lastParsedBlockNumber + rangeEnd} blocks")

            val blocks = IntRange(1, rangeEnd)
                    .map { increment -> lastParsedBlockNumber + increment }
                    .map { blockNumber -> blockParameter(blockNumber) }
                    .map { blockParameter -> parityClient.ethGetBlockByNumber(blockParameter, true).sendAsync() }
                    .map { futureBlock ->
                        while (!futureBlock.isDone) Thread.sleep(10)
                        if (futureBlock.isCompletedExceptionally) throw RuntimeException()
                        futureBlock.get().block
                    }
                    .map { block ->

                        val valueByteArray = jsonSerializer.writeValueAsBytes(block)

                        SourceRecord(
                                sourcePartition, sourceOffset(block.number),
                                IndexTopics.ethereumSourceTopic, Schema.BYTES_SCHEMA, valueByteArray
                        )
                    }

            lastParsedBlockNumber += batchSize
            log.info("Obtained ${lastParsedBlockNumber + 1}-${lastParsedBlockNumber + rangeEnd} blocks")

            return blocks
        } catch (e: Exception) {
            log.error("Unexpected error during polling ethereum chain", e)
            return emptyList()
        }
    }

    private fun lastParsedBlockNumber(): BigInteger {
        val blockNumber = context.offsetStorageReader()?.offset(sourcePartition)?.get("blockNumber") ?: return BigInteger.ZERO
        return BigInteger(blockNumber as String)
    }


    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
    private fun sourceOffset(blockNumber: BigInteger) = mapOf("blockNumber" to blockNumber.toString())
    private val sourcePartition = mapOf("blockchain" to ethereum)

    override fun version(): String = "1.0"
    override fun stop() {}
}