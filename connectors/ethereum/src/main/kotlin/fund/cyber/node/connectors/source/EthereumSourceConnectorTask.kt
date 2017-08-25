package fund.cyber.node.connectors.source

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.connectors.configuration.EthereumConnectorConfiguration
import fund.cyber.node.connectors.model.TopicsKeys
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.math.BigInteger.ONE
import java.math.BigInteger.ZERO


class EthereumSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(EthereumSourceConnectorTask::class.java)

    private lateinit var taskConfiguration: EthereumConnectorConfiguration
    private lateinit var parityClient: Web3j
    private lateinit var jsonSerializer: ObjectMapper
    private var lastParsedBlockNumber: BigInteger = ZERO


    override fun start(properties: Map<String, String>) {
        taskConfiguration = EthereumConnectorConfiguration(properties)
        lastParsedBlockNumber = lastParsedBlockNumber()
        parityClient = Web3j.build(HttpService(taskConfiguration.parityUrl))
        jsonSerializer = ObjectMapper()
    }


    override fun poll(): List<SourceRecord> {

        try {

            val nextBlockNumber = lastParsedBlockNumber.add(ONE)
            val futureBlock = parityClient.ethGetBlockByNumber(blockParameter(nextBlockNumber), true).sendAsync()


            while (!futureBlock.isDone) Thread.sleep(10)

            if (futureBlock.isCompletedExceptionally) {
                log.error("Unexpected error during pulling parity")
                return emptyList()
            }

            val blockRecord = SourceRecord(
                    sourcePartition, sourceOffset(nextBlockNumber), ethereumSourceTopic,
                    STRING_SCHEMA, TopicsKeys.ADD.name,
                    STRING_SCHEMA, jsonSerializer.writeValueAsString(futureBlock.get().block)
            )

            lastParsedBlockNumber = nextBlockNumber
            return listOf(blockRecord)
        } catch (e: Exception) {
            log.error("Unexpected error during polling ethereum chain", e)
            return emptyList()
        }
    }

    fun lastParsedBlockNumber(): BigInteger {
        val blockNumber = context.offsetStorageReader()
                ?.offset(sourcePartition)?.get("blockNumber") ?: return ZERO
        return BigInteger(blockNumber as String)
    }


    fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
    fun sourceOffset(blockNumber: BigInteger) = mapOf("blockNumber" to blockNumber.toString())
    val sourcePartition = mapOf("blockchain" to ethereum)

    override fun version(): String = "1.0"
    override fun stop() {}
}
