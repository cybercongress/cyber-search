package fund.cyber.address.bitcoin

import fund.cyber.address.common.AddressDelta
import fund.cyber.address.common.ApplyAddressDeltaFunction
import fund.cyber.address.common.ConvertItemToAddressDeltaFunction
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.model.BitcoinAddress
import fund.cyber.node.model.BitcoinTransaction
import org.ehcache.Cache
import java.math.BigDecimal


class BitcoinTransactionToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<BitcoinTransaction> {


    override fun invoke(tx: BitcoinTransaction): List<AddressDelta> {
        val blockNumber = tx.block_number

        val addressesDeltasByIns = tx.ins.flatMap { input ->
            input.addresses.map { address ->
                AddressDelta(TRANSACTION, address, blockNumber, BigDecimal(input.amount).negate(), txNumberDelta = 1)
            }
        }

        val addressesDeltasByOuts = tx.outs.flatMap { output ->
            output.addresses.map { address ->
                AddressDelta(TRANSACTION, address, blockNumber, BigDecimal(output.amount), txNumberDelta = 1)
            }
        }
        return addressesDeltasByIns + addressesDeltasByOuts
    }
}


class ApplyBitcoinAddressDeltaFunction(
        private val repository: BitcoinKeyspaceRepository,
        private val addressCache: Cache<String, BitcoinAddress>
) : ApplyAddressDeltaFunction<AddressDelta> {

    override fun invoke(addressDelta: AddressDelta) {

        val address = addressCache[addressDelta.address] ?: repository.addressStore.get(addressDelta.address)

        val updatedAddress =
                if (address == null) nonExistingAddressFromDelta(addressDelta)
                else updatedAddressByDelta(address, addressDelta)

        repository.addressStore.save(updatedAddress)
        addressCache.put(updatedAddress.id, updatedAddress)
    }
}

private fun nonExistingAddressFromDelta(delta: AddressDelta): BitcoinAddress {

    return BitcoinAddress(
            id = delta.address, confirmed_tx_number = 1,
            confirmed_balance = delta.balanceDelta.toString(), confirmed_total_received = delta.balanceDelta
    )
}


private fun updatedAddressByDelta(address: BitcoinAddress, addressDelta: AddressDelta): BitcoinAddress {

    val sign = addressDelta.balanceDelta.signum()

    val totalReceived =
            if (sign > 0) address.confirmed_total_received + addressDelta.balanceDelta else address.confirmed_total_received

    val newBalance = (BigDecimal(address.confirmed_balance) + addressDelta.balanceDelta).toString()

    return BitcoinAddress(
            id = address.id, confirmed_tx_number = address.confirmed_tx_number + 1,
            confirmed_total_received = totalReceived, confirmed_balance = newBalance
    )
}