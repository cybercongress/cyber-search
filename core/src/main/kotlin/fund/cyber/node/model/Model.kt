package fund.cyber.node.model


data class Block(
        val chunk_id: String,
        val number: String,
        val rawBlock: String
)

data class Request(
        val jsonrpc: String = "2.0",
        val method: String,
        val params: List<Any>,
        val id: Long = 1
)