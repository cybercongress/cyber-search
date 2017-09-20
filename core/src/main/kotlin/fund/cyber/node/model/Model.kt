package fund.cyber.node.model


interface Item
interface ItemPreview

data class Block(
        val chunk_id: String,
        val number: String,
        val rawBlock: String
)