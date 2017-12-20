package fund.cyber.pump.common

import java.util.concurrent.ConcurrentHashMap



class TxMempool(
        currentIndexedTxesHashes: List<String>
) {

    private val indexedTxesHashes: MutableSet<String> = ConcurrentHashMap.newKeySet<String>().apply {
        addAll(currentIndexedTxesHashes)
    }

    fun isTxIndexed(hash: String) = indexedTxesHashes.contains(hash)
    fun txAddedToIndex(hash: String) = indexedTxesHashes.add(hash)
    fun txesAddedToIndex(hashes: List<String>) = indexedTxesHashes.addAll(hashes)
}
