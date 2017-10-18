package fund.cyber.index.bitcoin.converter

import fund.cyber.node.model.*
import java.math.BigDecimal


class BitcoinAddressConverter {

    fun updateAddressesSummary(
            newTransactions: List<BitcoinTransaction>,
            existingAddressesUsedInBlock: List<BitcoinAddress>): List<BitcoinAddress> {

        val addressLookUp = existingAddressesUsedInBlock.associateBy { address -> address.address }.toMutableMap()

        return newTransactions
                .flatMap { tx ->

                    tx.ins.flatMap { input ->
                        input.addresses.map { address ->
                            updateAddressByTransactionInput(tx.block_number, addressLookUp[address]!!, input)
                                    .apply { addressLookUp.put(address, this) }
                        }
                    } +

                    tx.outs.flatMap { output ->
                        output.addresses.map { address ->
                            val addressSummary = addressLookUp[address]
                            when (addressSummary) {
                                null -> nonExistingAddressFromOutput(tx.block_number, address, output)
                                else -> updateAddressByTransactionOutput(tx.block_number, addressSummary, output)
                            }.apply { addressLookUp.put(address, this) }
                        }
                    }
                }
    }


    private fun nonExistingAddressFromOutput(
            blockNumber: Long, address: String, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                address = address, last_transaction_block = blockNumber, tx_number = 1,
                total_received = output.amount, balance = output.amount
        )
    }


    private fun updateAddressByTransactionOutput(
            blockNumber: Long, addressSummary: BitcoinAddress, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                address = addressSummary.address, last_transaction_block = blockNumber,
                tx_number = addressSummary.tx_number + 1,
                total_received = (BigDecimal(addressSummary.total_received) + BigDecimal(output.amount)).toString(),
                balance = (BigDecimal(addressSummary.balance) + BigDecimal(output.amount)).toString()
        )
    }


    private fun updateAddressByTransactionInput(
            blockNumber: Long, addressSummary: BitcoinAddress, input: BitcoinTransactionIn): BitcoinAddress {

        return BitcoinAddress(
                address = addressSummary.address, last_transaction_block = blockNumber,
                tx_number = addressSummary.tx_number + 1, total_received = addressSummary.total_received,
                balance = (BigDecimal(addressSummary.balance) - BigDecimal(input.amount)).toString()
        )
    }


    fun transactionsPreviewsForAddresses(newTransactions: List<BitcoinTransaction>): List<BitcoinAddressTransaction> {

        return newTransactions
                .flatMap { tx ->

                    tx.allAddressesUsedInTransaction().map { address ->
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