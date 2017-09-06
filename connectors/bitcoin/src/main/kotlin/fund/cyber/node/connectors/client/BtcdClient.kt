package fund.cyber.node.connectors.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.connectors.configuration.BitcoinConnectorConfiguration
import fund.cyber.node.model.Request
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.apache.http.message.BasicHeader
import org.web3j.protocol.core.Response
import java.util.concurrent.Future


class AsyncBtcdClient(private val configuration: BitcoinConnectorConfiguration) {

    private val jsonSerializer = ObjectMapper()
    private val jsonDeserializer = ObjectMapper()


    private val requestConfig = RequestConfig.custom()
            .setSocketTimeout(3000)
            .setConnectTimeout(3000).build()

    private val httpClient = HttpAsyncClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setMaxConnPerRoute(configuration.batchSize)
            .setMaxConnTotal(configuration.batchSize)
            .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy())
            .build()

    private val headers = arrayOf(BasicHeader("Content-Type", "application/json; charset=UTF-8"))

    init {
        httpClient.start()
    }

    fun getBlockByNumber(number: Long): Future<HttpResponse> {

        val request = Request(method = "getblockbynumber", params = listOf(number, true, true))
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(configuration.btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        return httpClient.execute(httpPost, null)
    }


    fun getLastBlockNumber(): Long {

        val request = Request(method = "getblockcount", params = emptyList())
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(configuration.btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val futureResponse = httpClient.execute(httpPost, null)
        while (!futureResponse.isDone) Thread.sleep(100)

        val response = jsonDeserializer.readValue(futureResponse.get().entity.content, BlockNumberResponse::class.java)

        return response.getBlockNumber()
    }
}

class BlockNumberResponse : Response<String>() {
    fun getBlockNumber(): Long = result.toLong()
}

class BlockResponse : Response<JsonNode>() {
    fun getRawBlock(): String = result.toString()
}
