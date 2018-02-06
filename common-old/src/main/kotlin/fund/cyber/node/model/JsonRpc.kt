package fund.cyber.node.model

data class Request(
        val jsonrpc: String = "2.0",
        val method: String,
        val params: List<Any> = emptyList(),
        val id: Long = 1
)