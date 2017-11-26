package fund.cyber.pump.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.model.BtcdBlock
import fund.cyber.node.model.BtcdTransaction
import fund.cyber.node.model.Request
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
        return executeRequest(requests, TxesResponse::class.java).getTxes()
    }

    fun getTxMempool(): List<String> {
        val request = Request(method = "getrawmempool")
        return executeRequest(request, TxMempoolResponse::class.java).getTxes()
    }

    fun getBlockHash(number: Long): String? {
        val request = Request(method = "getblockhash", params = listOf(number))
        return executeRequest(request, BlockHashResponse::class.java).getBlockHash()
    }

    fun getBlockByNumber(number: Long): BtcdBlock? {
        val request = Request(method = "getblockbynumber", params = listOf(number, true, true))
        return executeRequest(request, BlockResponse::class.java).getRawBlock()
    }

    fun getBlockByHash(hash: String): BtcdBlock? {
        val request = Request(method = "getblock", params = listOf(hash, true))
        return executeRequest(request, BlockResponse::class.java).getRawBlock()
    }

    fun getLastBlockNumber(): Long {
        val request = Request(method = "getblockcount", params = emptyList())
        return executeRequest(request, BlockNumberResponse::class.java).getBlockNumber()
    }


    private fun <T : Response<*>> executeRequest(request: Any, valueType: Class<T>): T {
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val response = httpClient.execute(httpPost, null)
        val value = jsonDeserializer.readValue(response.entity.content, valueType)

        if (value.error != null) {
            throw RuntimeException("Error during executing json rpc request `${value.error.message}`")
        }

        return value
    }
}

class TxMempoolResponse : Response<List<String>>() {
    fun getTxes(): List<String> = result ?: emptyList()
}

class TxesResponse : Response<List<BtcdTransaction>>() {
    fun getTxes(): List<BtcdTransaction> = result ?: emptyList()
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