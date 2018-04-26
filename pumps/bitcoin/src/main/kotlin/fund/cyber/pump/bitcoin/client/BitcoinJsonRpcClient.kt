package fund.cyber.pump.bitcoin.client

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.search.configuration.BITCOIN_CHAIN_NODE_DEFAULT_URL
import fund.cyber.search.configuration.CHAIN_NODE_URL
import fund.cyber.search.configuration.env
import fund.cyber.search.model.Request
import fund.cyber.search.model.Response
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.message.BasicHeader
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers


@Component
class BitcoinJsonRpcClient(
    private val httpClient: HttpClient
) {

    private val endpointUrl = env(CHAIN_NODE_URL, BITCOIN_CHAIN_NODE_DEFAULT_URL)

    private val headers = arrayOf(BasicHeader("Content-Type", "application/json; charset=UTF-8"))

    private val jsonSerializer = ObjectMapper().registerKotlinModule()
        .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

    private val jsonDeserializer = ObjectMapper().registerKotlinModule()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)


    fun getTxes(txIds: List<String>): List<JsonRpcBitcoinTransaction> {
        val chunkSize = 100
        println("TRANSACTIONS COUNT: ${txIds.size}. STARTING PROCESSING")
        var counter = 0
        return txIds.chunked(chunkSize)
            .flatMap { chunk ->
                println("PROCESSING TXS FROM ${counter * chunkSize} TO ${(counter + 1) * chunkSize} OF ${txIds.size} TXS COUNT")
                counter++
                Flux.fromIterable(chunk)
                    .flatMap { txId ->
                        Mono.just(
                            executeRestRequest("/rest/tx/$txId", JsonRpcBitcoinTransaction::class.java)
                        ).subscribeOn(Schedulers.parallel())
                    }
                    .collectList().block()!!
            }
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
        return executeRestRequest("/rest/block/$hash", JsonRpcBitcoinBlock::class.java)
    }

    fun getLastBlockNumber(): Long {
        val request = Request(method = "getblockcount", params = emptyList())
        return executeRequest(request, LongResponse::class.java)
    }

    fun getBlockByNumber(number: Long): JsonRpcBitcoinBlock? {

        val hash = getBlockHash(number) ?: return null
        val block = getBlockByHash(hash) ?: return null

        return block
    }

    private fun <C, T : Response<C>> executeBatchRequest(request: Any, valueType: Class<T>): List<C> {
        println(request)
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(endpointUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val httpResponse = httpClient.execute(httpPost, null)
        val jsonRpcResponses = jsonDeserializer.readValue<List<T>>(
            httpResponse.entity.content,
            jsonDeserializer.typeFactory.constructCollectionType(List::class.java, valueType)
        )

        jsonRpcResponses.forEach { response ->
            if (response.error != null) {
                throw RuntimeException("Error during executing json rpc request `${response.error?.message}`")
            }
        }

        return jsonRpcResponses.map { response -> response.result!! }
    }

    private fun <C, T : Response<C>> executeRequest(request: Any, valueType: Class<T>): C {
        val payload = jsonSerializer.writeValueAsBytes(request)

        val httpPost = HttpPost(endpointUrl)
        httpPost.entity = ByteArrayEntity(payload)
        httpPost.setHeaders(headers)

        val httpResponse = httpClient.execute(httpPost, null)
        val jsonRpcResponse = jsonDeserializer.readValue(httpResponse.entity.content, valueType)

        if (jsonRpcResponse.error != null) {
            throw RuntimeException("Error during executing json rpc request `${jsonRpcResponse.error?.message}`")
        }

        return jsonRpcResponse.result!!
    }

    private fun <C> executeRestRequest(endpointPath: String,
                                       valueType: Class<C>,
                                       format: RestResponseFormat = RestResponseFormat.JSON): C {
        println("QUERYING $endpointUrl$endpointPath.${format.name.toLowerCase()}")
        val httpGet = HttpGet("$endpointUrl$endpointPath.${format.name.toLowerCase()}")
        httpGet.setHeaders(headers)

        val httpResponse = httpClient.execute(httpGet, null)
        val response = jsonDeserializer.readValue(httpResponse.entity.content, valueType)

        return response!!
    }
}

class TxMempoolResponse : Response<List<String>>()
class TxResponse : Response<JsonRpcBitcoinTransaction>()
class StringResponse : Response<String>()
class LongResponse : Response<Long>()
class BlockResponse : Response<JsonRpcBitcoinBlock>()

enum class RestResponseFormat {
    JSON, BIN, HEX
}