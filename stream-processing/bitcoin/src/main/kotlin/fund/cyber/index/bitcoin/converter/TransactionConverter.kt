package fund.cyber.index.bitcoin.converter

import fund.cyber.index.btcd.*
import fund.cyber.node.common.sumByBigDecimal
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionIn
import fund.cyber.node.model.BitcoinTransactionOut
import org.ehcache.Cache
import java.time.Instant


//todo remove top lvl function

class BitcoinTransactionConverter(private val transactionCache: Cache<String, BitcoinTransaction>) {

    fun btcdTransactionsToDao(btcdBlock: BtcdBlock): List<BitcoinTransaction> {

        return btcdBlock.rawtx.map { btcdTransaction -> btcdTransactionToDao(btcdTransaction, btcdBlock) }
    }


    fun btcdTransactionToDao(
            btcdTransaction: BtcdTransaction, btcdBlock: BtcdBlock): BitcoinTransaction {

        val firstInput = btcdTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return btcdCoinbaseTxToDao(btcdTransaction, btcdBlock)
        }

        val inputs = btcdTxInToDao(btcdTransaction.regularInputs())
        val outputs = btcdTransaction.vout.map { btcdTxOut -> btcdTxOutToDao(btcdTxOut) }

        val totalInput = inputs.sumByBigDecimal { input -> input.amount }
        val totalOutput = outputs.sumByBigDecimal { out -> out.amount }

        val transaction = BitcoinTransaction(
                txid = btcdTransaction.txid, block_number = btcdBlock.height, lock_time = btcdTransaction.locktime,
                ins = inputs, outs = outputs, total_input = totalInput.toString(), total_output = totalOutput.toString(),
                fee = (totalInput - totalOutput).toString(), size = btcdTransaction.size,
                block_time = Instant.ofEpochSecond(btcdBlock.time).toString(),
                block_hash = btcdBlock.hash
        )
        transactionCache.put(transaction.txid, transaction)
        return transaction
    }


    fun btcdCoinbaseTxToDao(btcdTransaction: BtcdTransaction, btcdBlock: BtcdBlock): BitcoinTransaction {

        val firstInput = btcdTransaction.vin.first() as CoinbaseTransactionInput
        val firstOutput = btcdTransaction.vout.first()

        val output = BitcoinTransactionOut(
                address = firstOutput.scriptPubKey.addresses.first(), asm = firstOutput.scriptPubKey.asm,
                amount = firstOutput.value, out = firstOutput.n, required_signatures = firstOutput.scriptPubKey.reqSigs
        )

        val transaction = BitcoinTransaction(
                txid = btcdTransaction.txid, block_number = btcdBlock.height, lock_time = btcdTransaction.locktime,
                coinbase = firstInput.coinbase, fee = "0", block_hash = btcdBlock.hash,
                block_time = Instant.ofEpochSecond(btcdBlock.time).toString(),
                ins = emptyList(), total_input = "0",
                outs = listOf(output), total_output = "0", size = btcdTransaction.size
        )

        transactionCache.put(transaction.txid, transaction)
        return transaction
    }


    /**
     * Converts given btcd tx inputs to dao tx inputs.
     * Btcd input do not contains info about address and amount, just txid and n.
     * Dao input contains info about address and amount. So to fulfill missing fields, we should use earlier transactions
     *   outputs defined by txid and n.
     *
     * @param btcdTxIns current transaction inputs
     */
    fun btcdTxInToDao(btcdTxIns: List<BtcdRegularTransactionInput>): List<BitcoinTransactionIn> {

        return btcdTxIns.map { (txid, vout, scriptSig) ->
            val daoTxOut = transactionCache[txid].getOutputByNumber(vout)
            BitcoinTransactionIn(
                    address = daoTxOut.address, amount = daoTxOut.amount, asm = scriptSig.asm, tx_id = txid, tx_out = vout
            )
        }
    }

    fun btcdTxOutToDao(btcdTxOut: BtcdTransactionOutput): BitcoinTransactionOut {

        return BitcoinTransactionOut(
                address = btcdTxOut.scriptPubKey.addresses.joinToString(separator = ","),
                amount = btcdTxOut.value, out = btcdTxOut.n, asm = btcdTxOut.scriptPubKey.asm,
                required_signatures = btcdTxOut.scriptPubKey.reqSigs
        )
    }
}