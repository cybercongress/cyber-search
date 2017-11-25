package fund.cyber.pump.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.model.BtcdBlock
import fund.cyber.node.model.BtcdTransaction
import fund.cyber.node.model.Request
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.message.BasicHeader
import org.web3j.protocol.core.Response


class BitcoinJsonRpcClient(
        private val jsonSerializer: ObjectMapper,
        private val jsonDeserializer: ObjectMapper,
        private val httpClient: HttpClient,
        private val btcdUrl: String
) {

    private val headers = arrayOf(BasicHeader("Content-Type", "application/json; charset=UTF-8"))

    fun getTxes(txIds: List<String>): List<BtcdTransaction> {
        val requests = txIds.map { id -> Request(method = "getrawtransaction", params = listOf(id, true)) }
        val response = executeRequest(requests)
        return jsonDeserializer.readValue(response.entity.content, TransactionsResponce::class.java).getTxes()
    }

    fun getBlockHash(number: Long): String? {
        val request = Request(method = "getblockhash", params = listOf(number))
        val response = executeRequest(request)
        return jsonDeserializer.readValue(response.entity.content, BlockHashResponse::class.java).getBlockHash()
    }

    fun getBlockByNumber(number: Long): BtcdBlock? {
        val request = Request(method = "getblockbynumber", params = listOf(number, true, true))
        val response = executeRequest(request)
        return jsonDeserializer.readValue(response.entity.content, BlockResponse::class.java).getRawBlock()
    }

    fun getBlockByHash(hash: String): BtcdBlock? {
        val request = Request(method = "getblock", params = listOf(hash, true))
        val response = executeRequest(request)
        return jsonDeserializer.readValue(response.entity.content, BlockResponse::class.java).getRawBlock()
    }

    fun getLastBlockNumber(): Long {
        val request = Request(method = "getblockcount", params = emptyList())
        val response = executeRequest(request)
        return jsonDeserializer.readValue(response.entity.content, BlockNumberResponse::class.java).getBlockNumber()
    }

    private fun executeRequest(request: Any): HttpResponse {
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        return httpClient.execute(httpPost, null)
    }
}

class TransactionsResponce : Response<List<BtcdTransaction>>() {
    fun getTxes(): List<BtcdTransaction> = result
}

class BlockNumberResponse : Response<String>() {
    fun getBlockNumber(): Long = result.toLong()
}

class BlockHashResponse : Response<String>() {
    fun getBlockHash(): String? = result
}

class BlockResponse : Response<BtcdBlock>() {
    fun getRawBlock(): BtcdBlock? = result
}