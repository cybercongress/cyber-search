package fund.cyber.node.connectors.model


data class Block(
        val number: String,
        val hash: String,
        val timestamp: String,
        val rawBlock: String
)