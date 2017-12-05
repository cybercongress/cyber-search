package fund.cyber.index.bitcoin.converter

import fund.cyber.node.model.*
import org.slf4j.LoggerFactory
import java.math.BigDecimal

private val log = LoggerFactory.getLogger(BitcoinAddressConverter::class.java)!!

class BitcoinAddressConverter {

    fun updateAddressesSummary(
            newTransactions: List<BitcoinTransaction>,
            existingAddressesUsedInBlock: List<BitcoinAddress>): List<BitcoinAddress> {

        val addressLookUp = existingAddressesUsedInBlock.associateBy { address -> address.id }.toMutableMap()

        return newTransactions
                .flatMap { tx ->

                    tx.ins.flatMap { input ->
                        input.addresses.map { addressId ->
                            log.trace("looking for address: $addressId")
                            updateAddressByTransactionInput(tx.block_number, addressLookUp[addressId]!!, input)
                                    .apply { addressLookUp.put(addressId, this) }
                        }
                    } +

                    tx.outs.flatMap { output ->
                        output.addresses.map { addressId ->
                            val address = addressLookUp[addressId]
                            when (address) {
                                null -> nonExistingAddressFromOutput(tx.block_number, addressId, output)
                                else -> updateAddressByTransactionOutput(tx.block_number, address, output)
                            }.apply { addressLookUp.put(addressId, this) }
                        }
                    }
                }
    }


    private fun nonExistingAddressFromOutput(
            blockNumber: Long, addressId: String, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                id = addressId, last_transaction_block = blockNumber, tx_number = 1,
                total_received = output.amount, balance = output.amount
        )
    }


    private fun updateAddressByTransactionOutput(
            blockNumber: Long, address: BitcoinAddress, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                id = address.id, last_transaction_block = blockNumber,
                tx_number = address.tx_number + 1,
                total_received = (BigDecimal(address.total_received) + BigDecimal(output.amount)).toString(),
                balance = (BigDecimal(address.balance) + BigDecimal(output.amount)).toString()
        )
    }


    private fun updateAddressByTransactionInput(
            blockNumber: Long, address: BitcoinAddress, input: BitcoinTransactionIn): BitcoinAddress {

        return BitcoinAddress(
                id = address.id, last_transaction_block = blockNumber,
                tx_number = address.tx_number + 1, total_received = address.total_received,
                balance = (BigDecimal(address.balance) - BigDecimal(input.amount)).toString()
        )
    }


    fun transactionsPreviewsForAddresses(newTransactions: List<BitcoinTransaction>): List<BitcoinAddressTransaction> {

        return newTransactions
                .flatMap { tx ->

                    tx.allAddressesUsedInTransaction().map { addressId ->
                        BitcoinAddressTransaction(
                                address = addressId, fee = BigDecimal(tx.fee), hash = tx.hash, block_time = tx.block_time,
                                ins = tx.ins.map { input ->
                                    BitcoinTransactionPreviewIO(addresses = input.addresses, amount = input.amount)
                                },
                                outs = tx.outs.map { out ->
                                    BitcoinTransactionPreviewIO(addresses = out.addresses, amount = out.amount)
                                }, block_number = tx.block_number
                        )
                    }
                }
    }
}