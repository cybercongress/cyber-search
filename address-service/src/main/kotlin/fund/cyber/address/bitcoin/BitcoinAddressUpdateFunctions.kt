package fund.cyber.address.bitcoin

import fund.cyber.address.common.AddressDelta
import fund.cyber.address.common.ApplyAddressDeltaFunction
import fund.cyber.address.common.ConvertItemToAddressDeltaFunction
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.model.BitcoinAddress
import fund.cyber.node.model.BitcoinTransaction
import java.math.BigDecimal


class BitcoinTransactionToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<BitcoinTransaction> {


    override fun invoke(tx: BitcoinTransaction): List<AddressDelta> {
        val blockNumber = tx.block_number

        val addressesDeltasByIns = tx.ins.flatMap { input ->
            input.addresses.map { address ->
                AddressDelta(address, BigDecimal(input.amount).negate(), blockNumber, TRANSACTION)
            }
        }

        val addressesDeltasByOuts = tx.outs.flatMap { output ->
            output.addresses.map { address ->
                AddressDelta(address, BigDecimal(output.amount), blockNumber, TRANSACTION)
            }
        }
        return addressesDeltasByIns + addressesDeltasByOuts
    }
}


class ApplyBitcoinAddressDeltaFunction(
        private val repository: BitcoinKeyspaceRepository
) : ApplyAddressDeltaFunction {

    override fun invoke(addressDelta: AddressDelta) {
        val address = repository.addressStore.get(addressDelta.address)

        if (address == null) {
            val newAddress = nonExistingAddressFromDelta(addressDelta)
            repository.addressStore.save(newAddress)
        } else {
            val updatedAddress = updatedAddressByDelta(address, addressDelta)
            repository.addressStore.save(updatedAddress)
        }
    }
}

private fun nonExistingAddressFromDelta(delta: AddressDelta): BitcoinAddress {

    return BitcoinAddress(
            id = delta.address, confirmed_tx_number = 1,
            confirmed_balance = delta.delta.toString(), confirmed_total_received = delta.delta
    )
}


private fun updatedAddressByDelta(address: BitcoinAddress, addressDelta: AddressDelta): BitcoinAddress {

    val sign = addressDelta.delta.signum()

    val totalReceived =
            if (sign > 0) address.confirmed_total_received + addressDelta.delta else address.confirmed_total_received

    val newBalance = (BigDecimal(address.confirmed_balance) + addressDelta.delta).toString()

    return BitcoinAddress(
            id = address.id, confirmed_tx_number = address.confirmed_tx_number + 1,
            confirmed_total_received = totalReceived, confirmed_balance = newBalance
    )
}