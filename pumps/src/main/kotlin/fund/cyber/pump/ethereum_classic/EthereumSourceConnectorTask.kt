package fund.cyber.pump.ethereum_classic

import com.fasterxml.jackson.databind.ObjectMapper
//import fund.cyber.index.IndexTopics
//import fund.cyber.node.common.plus
//import fund.cyber.node.connectors.configuration.*

//import org.apache.kafka.connect.data.Schema
//import org.apache.kafka.connect.source.SourceRecord
//import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigInteger
import java.math.BigInteger.TEN
import java.util.concurrent.Executors


class EthereumSourceConnectorTask /*: SourceTask()*/ {

    private val log = LoggerFactory.getLogger(EthereumSourceConnectorTask::class.java)

    private val jsonSerializer: ObjectMapper = jacksonJsonSerializer

//    private lateinit var config: EthereumConnectorConfiguration
    private lateinit var parityClient: Web3j

    private var batchSize: BigInteger = BigInteger.valueOf(BATCH_SIZE_DEFAULT)

    private lateinit var lastNetworkBlock: BigInteger
    private lateinit var lastParsedBlockNumber: BigInteger


    fun start(properties: Map<String, String>? = null) {

        val executorService = Executors.newScheduledThreadPool(batchSize.toInt())

//        config = EthereumConnectorConfiguration(properties)
        parityClient = Web3j.build(HttpService(PARITY_URL), 15 * 1000, executorService)

//        batchSize = BATCH_SIZE_DEFAULT//config.batchSize

        lastParsedBlockNumber = lastParsedBlockNumber()
        lastNetworkBlock = parityClient.ethBlockNumber().send().blockNumber
println(lastNetworkBlock)
        parityClient.catchUpToLatestAndSubscribeToNewBlocksObservable({"0"}, true).timeInterval().subscribe({interval ->
            println(interval.intervalInMilliseconds)
            val block = interval.value
            println(block.block.hash)
        })
    }


    fun poll(): List<EthBlock.Block> {

        try {

            val isBatchFetch = lastNetworkBlock - lastParsedBlockNumber > TEN
            val rangeEnd = if (isBatchFetch) batchSize.toInt() else 1

            if (!isBatchFetch) {
                lastNetworkBlock = parityClient.ethBlockNumber().send().blockNumber
                if (lastParsedBlockNumber == lastNetworkBlock) {
                    log.debug("Up-to-date (block $lastParsedBlockNumber)")
                    Thread.sleep(5000)
                    return emptyList()
                }
            }
            log.debug("Looking for ${lastParsedBlockNumber.toInt() + 1}-${lastParsedBlockNumber.toInt() + rangeEnd} blocks")

            val blocks = IntRange(1, rangeEnd)
                    .map { increment -> lastParsedBlockNumber + BigInteger.valueOf(increment.toLong()) }
                    .map { blockNumber -> blockParameter(blockNumber) }
                    .map { blockParameter -> parityClient.ethGetBlockByNumber(blockParameter, true).sendAsync() }
                    .map { futureBlock ->
                        while (!futureBlock.isDone) Thread.sleep(10)
                        if (futureBlock.isCompletedExceptionally) throw RuntimeException()
                        futureBlock.get().block
                    }
                    .map { block ->
                        block
//                        println(block.toString())
//                        val valueByteArray = jsonSerializer.writeValueAsBytes(block)

//                        SourceRecord(
//                                sourcePartition, sourceOffset(block.number),
//                                IndexTopics.ethereumSourceTopic, Schema.BYTES_SCHEMA, valueByteArray
//                        )
                    }

            lastParsedBlockNumber += batchSize
            log.debug("Obtained ${lastParsedBlockNumber.toInt() + 1}-${lastParsedBlockNumber.toInt() + rangeEnd} blocks")

            return blocks
        } catch (e: Exception) {
            e.printStackTrace()
            log.error("Unexpected error during polling ethereum chain. Last parsed block $lastParsedBlockNumber", e)
            return emptyList()
        }
    }

    private fun lastParsedBlockNumber(): BigInteger {
        val blockNumber = BigInteger.ZERO//context.offsetStorageReader()?.offset(sourcePartition)?.get("blockNumber") ?: return BigInteger.ZERO
        return blockNumber
    }


    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
    private fun sourceOffset(blockNumber: BigInteger) = mapOf("blockNumber" to blockNumber.toString())
//    private val sourcePartition = mapOf("blockchain" to ETHEREUM_PARTITION)

    fun version(): String = "1.0"
    fun stop() {}
}