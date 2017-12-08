package fund.cyber.address.bitcoin

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
                            updateAddressByTransactionInput(addressLookUp[addressId]!!, input)
                                    .apply { addressLookUp.put(addressId, this) }
                        }
                    } +

                    tx.outs.flatMap { output ->
                        output.addresses.map { addressId ->
                            val address = addressLookUp[addressId]
                            when (address) {
                                null -> nonExistingAddressFromOutput(addressId, output)
                                else -> updateAddressByTransactionOutput(address, output)
                            }.apply { addressLookUp.put(addressId, this) }
                        }
                    }
                }
    }


    private fun nonExistingAddressFromOutput(addressId: String, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                id = addressId, confirmed_tx_number = 1, confirmed_balance = output.amount,
                confirmed_total_received = BigDecimal(output.amount)
        )
    }


    private fun updateAddressByTransactionOutput(address: BitcoinAddress, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                id = address.id, confirmed_tx_number = address.confirmed_tx_number + 1,
                confirmed_total_received = address.confirmed_total_received + BigDecimal(output.amount),
                confirmed_balance = (BigDecimal(address.confirmed_balance) + BigDecimal(output.amount)).toString()
        )
    }


    private fun updateAddressByTransactionInput(address: BitcoinAddress, input: BitcoinTransactionIn): BitcoinAddress {

        return BitcoinAddress(
                id = address.id, confirmed_tx_number = address.confirmed_tx_number + 1,
                confirmed_total_received = address.confirmed_total_received,
                confirmed_balance = (BigDecimal(address.confirmed_balance) - BigDecimal(input.amount)).toString()
        )
    }
}