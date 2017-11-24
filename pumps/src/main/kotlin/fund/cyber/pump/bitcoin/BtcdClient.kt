package fund.cyber.pump.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.model.BtcdBlock
import fund.cyber.node.model.Request
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.message.BasicHeader
import org.web3j.protocol.core.Response


class BtcdClient(
        private val jsonSerializer: ObjectMapper,
        private val jsonDeserializer: ObjectMapper,
        private val httpClient: HttpClient,
        private val btcdUrl: String
) {

    private val headers = arrayOf(BasicHeader("Content-Type", "application/json; charset=UTF-8"))

    fun getBlockByNumber(number: Long): BtcdBlock? {

        val request = Request(method = "getblockbynumber", params = listOf(number, true, true))
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val response = httpClient.execute(httpPost, null)
        return jsonDeserializer.readValue(response.entity.content, BlockResponse::class.java).getRawBlock()
    }


    fun getLastBlockNumber(): Long {

        val request = Request(method = "getblockcount", params = emptyList())
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val response = httpClient.execute(httpPost, null)

        return jsonDeserializer.readValue(response.entity.content, BlockNumberResponse::class.java).getBlockNumber()
    }
}

class BlockNumberResponse : Response<String>() {
    fun getBlockNumber(): Long = result.toLong()
}

class BlockResponse : Response<BtcdBlock>() {
    fun getRawBlock(): BtcdBlock? = result
}