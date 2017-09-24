package fund.cyber.index.bitcoin.converter

import fund.cyber.index.btcd.*
import fund.cyber.node.common.sumByBigDecimal
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionIn
import fund.cyber.node.model.BitcoinTransactionOut
import java.math.BigDecimal
import java.time.Instant

//todo remove top lvl function

class BitcoinTransactionConverter {


    fun btcdTransactionToDao(
            btcdTransaction: BtcdTransaction, btcdBlock: BtcdBlock,
            inputDaoTransactionById: Map<String, BitcoinTransaction>): BitcoinTransaction {

        val firstInput = btcdTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return btcdCoinbaseTxToDao(btcdTransaction, btcdBlock)
        }

        val inputs = btcdTxInToDao(btcdTransaction.regularInputs(), inputDaoTransactionById)
        val outputs = btcdTransaction.vout.map { btcdTxOut -> btcdTxOutToDao(btcdTxOut) }

        val totalInput = inputs.sumByBigDecimal { input -> input.amount }
        val totalOutput = outputs.sumByBigDecimal { out -> out.amount }

        return BitcoinTransaction(
                txId = btcdTransaction.txid, block_number = btcdBlock.height, lock_time = btcdTransaction.locktime,
                ins = inputs, outs = outputs, total_input = totalInput, total_output = totalOutput,
                fee = totalInput - totalOutput, size = btcdTransaction.size, blockTime = Instant.ofEpochSecond(btcdBlock.time)
        )
    }


    fun btcdCoinbaseTxToDao(btcdTransaction: BtcdTransaction, btcdBlock: BtcdBlock): BitcoinTransaction {

        val firstInput = btcdTransaction.vin.first() as CoinbaseTransactionInput
        val firstOutput = btcdTransaction.vout.first()

        val output = BitcoinTransactionOut(
                address = firstOutput.scriptPubKey.addresses.first(), asm = firstOutput.scriptPubKey.asm,
                amount = firstOutput.value, out = firstOutput.n, required_signatures = firstOutput.scriptPubKey.reqSigs
        )

        return BitcoinTransaction(
                txId = btcdTransaction.txid, block_number = btcdBlock.height, lock_time = btcdTransaction.locktime,
                coinbase = firstInput.coinbase, fee = BigDecimal.ZERO, blockTime = Instant.ofEpochSecond(btcdBlock.time),
                ins = emptyList(), total_input = BigDecimal.ZERO,
                outs = listOf(output), total_output = BigDecimal.ZERO, size = btcdTransaction.size
        )
    }


    /**
     * Converts given btcd tx inputs to dao tx inputs.
     * Btcd input do not contains info about address and amount, just txid and n.
     * Dao input contains info about address and amount. So to fulfill missing fields, we should use earlier transactions
     *   outputs defined by txid and n.
     *
     * @param btcdTxIns current transaction inputs
     * @param inputDaoTransactionById earlier transactions, that used as given input for current transaction.
     */
    fun btcdTxInToDao(btcdTxIns: List<BtcdRegularTransactionInput>,
                      inputDaoTransactionById: Map<String, BitcoinTransaction>): List<BitcoinTransactionIn> {

        return btcdTxIns.map { (txid, vout, scriptSig) ->
            val daoTxOut = inputDaoTransactionById[txid]!!.getOutputByNumber(vout)
            BitcoinTransactionIn(
                    address = daoTxOut.address, amount = daoTxOut.amount, asm = scriptSig.asm, tx_hash = txid, tx_out = vout
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