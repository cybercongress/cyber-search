package fund.cyber.index.bitcoin

import fund.cyber.index.btcd.BtcdBlock
import fund.cyber.index.btcd.CoinbaseTransactionInput
import fund.cyber.index.btcd.RegularTransactionInput
import fund.cyber.index.btcd.Transaction
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.node.model.BitcoinBlockTransaction
import fund.cyber.node.model.BitcoinBlockTransactionIO
import java.math.BigDecimal
import java.time.Instant

/*data class BitcoinBlockTransaction(
        val fee: BigDecimal,
        val lock_time: Instant,
        val ins: List<BitcoinBlockTransactionIO>,
        val outs: List<BitcoinBlockTransactionIO>
)*/


/*-------------------------------------------------------------------------------------*/
/*-------------------------------------------------------------------------------------*/
/*-------------------------------------------------------------------------------------*/
/*-------------------------------------------------------------------------------------*/
/*data class Transaction(
        val txid: String,
        val hex: String,
        val version: Int,
        val locktime: Long,
        val vout: List<TransactionOutput>,

        val vin: List<TransactionInput>
)

data class TransactionOutput(
        val value: BigDecimal,
        val n: Int,
        val scriptPubKey: PubKeyScript
)

sealed class TransactionInput

data class CoinbaseTransactionInput(
        val coinbase: String,
        val sequence: Long,
        val txinwitness: String = ""
) : TransactionInput()

data class RegularTransactionInput(
        val txid: String,
        val vout: Int,
        val scriptSig: SignatureScript,
        val sequence: Long,
        val txinwitness: String = ""
) : TransactionInput()*/


fun getBlockForStorage(btcdBlock: BtcdBlock): BitcoinBlock {

    return BitcoinBlock(
            hash = btcdBlock.hash, height = btcdBlock.height, time = Instant.ofEpochMilli(btcdBlock.time),
            nonce = btcdBlock.nonce, merkleroot = btcdBlock.merkleroot, size = btcdBlock.size,
            version = btcdBlock.version, weight = btcdBlock.weight, difficulty = btcdBlock.difficulty,
            bits = btcdBlock.bits, txs = getBlockTransactions(btcdBlock)
    )
}

private fun getBlockTransactions(btcdBlock: BtcdBlock): List<BitcoinBlockTransaction> {

    return btcdBlock.tx
            .map { btcdTransaction ->
                BitcoinBlockTransaction(
                        hash = btcdTransaction.hex, lock_time = Instant.ofEpochMilli(btcdTransaction.locktime),
                        ins = getTransactionIns(btcdTransaction),
                        /* todo*/
                        outs = emptyList(), fee = BigDecimal.ZERO
                )
            }
}

private fun getTransactionIns(btcdTransaction: Transaction): List<BitcoinBlockTransactionIO> {
    val firstTransaction = btcdTransaction.vin[0]
    return when (firstTransaction) {
    /* todo*/
        is RegularTransactionInput -> {
            emptyList()
        }
        is CoinbaseTransactionInput -> {
            emptyList()
        }
    }
}