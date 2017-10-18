package fund.cyber.index.bitcoin.converter

import fund.cyber.node.model.BitcoinAddress
import fund.cyber.node.model.BitcoinAddressTransaction
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionPreviewIO


class BitcoinAddressConverter {

    fun updateAddressesSummary(newTransactions: List<BitcoinTransaction>): List<BitcoinAddress> {
        return emptyList()
    }

    fun transactionsPreviewsForAddresses(transactions: List<BitcoinTransaction>): List<BitcoinAddressTransaction> {

        return transactions
                .flatMap { tx ->

                    val addresses = tx.ins.flatMap { input -> input.addresses } + tx.outs.flatMap { output -> output.addresses }

                    addresses.map { address ->
                        BitcoinAddressTransaction(
                                address = address, fee = tx.fee, hash = tx.txid, block_time = tx.block_time,
                                ins = tx.ins.map { input ->
                                    BitcoinTransactionPreviewIO(addresses = input.addresses, amount = input.amount)
                                },
                                outs = tx.outs.map { out ->
                                    BitcoinTransactionPreviewIO(addresses = out.addresses, amount = out.amount)
                                }
                        )
                    }
                }
    }
}