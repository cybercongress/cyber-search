package fund.cyber.search.model


data class Request(
        val jsonrpc: String = "2.0",
        val method: String,
        val params: List<Any> = emptyList(),
        val id: Long = 1
)


open class Response<out T>(
        val id: Long = 0,
        val result: T? = null,
        val error: Error? = null
)

data class Error(
        val code: Int,
        val message: String
)