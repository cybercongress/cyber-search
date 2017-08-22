package fund.cyber.node.connectors.source

import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.slf4j.LoggerFactory


class EthereumSourceConnectorTask : SourceTask() {

    private val log = LoggerFactory.getLogger(EthereumSourceConnectorTask::class.java)

    private val SERVICE_FIELD = "service"
    private val POSITION_FIELD = "position"
    private val TOPIC = "ethereum_source_connector"

    var i = 0


    override fun start(props: MutableMap<String, String>?) {
    }

    override fun stop() {
    }

    override fun version(): String {
        return "1.0"
    }

    override fun poll(): List<SourceRecord> {

        TimeUnit.SECONDS.sleep(1)
        i++
        log.debug(i.toString())
        log.error(i.toString())
        log.info(i.toString())

        val record = SourceRecord(blockNumberKey(), blockNumber(i), TOPIC, STRING_SCHEMA, i.toString())
        return listOf(record)
    }


    private fun blockNumberKey(): Map<String, String> = mapOf(SERVICE_FIELD to "ethereum_source_block_number")
    private fun blockNumber(offset: Int): Map<String, String> = mapOf(POSITION_FIELD to offset.toString())
}

/*
val appConfiguration = ApplicationConfiguration()

val JSON = MediaType.parse("application/json; charset=utf-8")
val requestBody = RequestBody.create(JSON, """{"method":"eth_getBlockByNumber","params":["0x1112222222221b",true],"id":1,"jsonrpc":"2.0"}""")

val blockRequest = Request.Builder()
        .url(appConfiguration.parity_url)
        .post(requestBody)
        .build()!!

val httpClient = ApplicationContext.httpClient

runBlocking {
    val response = httpClient.newCall(blockRequest).await()
    val block = jsonParser.readTree(response.body()?.string())
    println(block)
}





println(1.hex())*/
