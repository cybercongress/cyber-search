package fund.cyber.pump.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.model.JsonRpcBitcoinBlock
import fund.cyber.node.model.JsonRpcBitcoinTransaction
import fund.cyber.node.model.Request
import io.reactivex.Observable
import io.reactivex.schedulers.Schedulers
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

    private val headers = arrayOf(
            BasicHeader("Content-Type", "application/json; charset=UTF-8"),
            BasicHeader("Keep-Alive", "timeout=10, max=1000")
    )

    fun getTxes(txIds: List<String>): List<JsonRpcBitcoinTransaction> {
        val requests = txIds.map { id -> Request(method = "getrawtransaction", params = listOf(id, true)) }
        return executeBatchRequest(requests, TxResponse::class.java)
    }

    fun getTxMempool(): List<String> {
        val request = Request(method = "getrawmempool")
        return executeRequest(request, TxMempoolResponse::class.java)
    }

    fun getBlockHash(number: Long): String? {
        val request = Request(method = "getblockhash", params = listOf(number))
        return executeRequest(request, StringResponse::class.java)
    }

    fun getBlockByHash(hash: String): JsonRpcBitcoinBlock? {
        val request = Request(method = "getblock", params = listOf(hash, true))
        return executeRequest(request, BlockResponse::class.java)
    }

    fun getLastBlockNumber(): Long {
        val request = Request(method = "getblockcount", params = emptyList())
        return executeRequest(request, LongResponse::class.java)
    }

    fun getBlockByNumber(number: Long): JsonRpcBitcoinBlock? {

        val hash = getBlockHash(number) ?: return null
        val block = getBlockByHash(hash) ?: return null

        val transactions = if (block.height != 0L) getTxes(block.tx) else emptyList()
        return block.copy(rawtx = transactions)
    }

    private fun <C, T : Response<C>> executeBatchRequest(request: Any, valueType: Class<T>): List<C> {
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val httpResponse = httpClient.execute(httpPost, null)
        val jsonRpcResponses = jsonDeserializer.readValue<List<T>>(
                httpResponse.entity.content,
                jsonDeserializer.typeFactory.constructCollectionType(List::class.java, valueType)
        )

        jsonRpcResponses.forEach { response ->
            if (response.error != null) {
                throw RuntimeException("Error during executing json rpc request `${response.error.message}`")
            }
        }

        return jsonRpcResponses.map { response -> response.result }
    }

    private fun <C, T : Response<C>> executeRequest(request: Any, valueType: Class<T>): C {
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(btcdUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val httpResponse = httpClient.execute(httpPost, null)
        val jsonRpcResponse = jsonDeserializer.readValue(httpResponse.entity.content, valueType)

        if (jsonRpcResponse.error != null) {
            throw RuntimeException("Error during executing json rpc request `${jsonRpcResponse.error.message}`")
        }

        return jsonRpcResponse.result
    }
}

class TxMempoolResponse : Response<List<String>>()
class TxResponse : Response<JsonRpcBitcoinTransaction>()
class StringResponse : Response<String>()
class LongResponse : Response<Long>()
class BlockResponse : Response<JsonRpcBitcoinBlock>()