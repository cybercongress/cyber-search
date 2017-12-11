package fund.cyber.address.ethereum

import fund.cyber.address.common.AddressDelta
import fund.cyber.address.common.ApplyAddressDeltaFunction
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.ChainEntity.*
import fund.cyber.node.model.EthereumAddress
import java.math.BigDecimal


class ApplyEthereumAddressDeltaFunction(
        private val repository: EthereumKeyspaceRepository
) : ApplyAddressDeltaFunction {

    override fun invoke(addressDelta: AddressDelta) {

        val address = repository.addressStore.get(addressDelta.address)

        val updatedAddress = when (addressDelta.source) {
            TRANSACTION, CONTRACT -> updateAddressByTransaction(address, addressDelta)
            BLOCK, UNCLE -> updateAddressByMinerOrUncle(address, addressDelta)
            else -> throw RuntimeException("Unsupported address update source")
        }

        repository.addressStore.save(updatedAddress)
    }
}

private fun updateAddressByMinerOrUncle(address: EthereumAddress?, addressDelta: AddressDelta): EthereumAddress {

    val txNumber = address?.tx_number ?: 0
    val uncleNumber = address?.uncle_number ?: 0
    val minedBlocksNumber = address?.mined_block_number ?: 0
    val balance = BigDecimal(address?.balance ?: "0") + addressDelta.delta
    val totalReceived = BigDecimal(address?.total_received ?: "0") + addressDelta.delta

    return EthereumAddress(
            id = addressDelta.address, last_transaction_block = addressDelta.blockNumber,
            balance = balance.toString(), total_received = totalReceived.toString(),
            contract_address = address?.contract_address ?: false,
            tx_number = txNumber,
            uncle_number = if (addressDelta.source == UNCLE) uncleNumber + 1 else uncleNumber,
            mined_block_number = if (addressDelta.source == BLOCK) minedBlocksNumber + 1 else minedBlocksNumber
    )
}

private fun updateAddressByTransaction(address: EthereumAddress?, addressDelta: AddressDelta): EthereumAddress {

    if (address == null) {
        val value = addressDelta.delta.toString()
        return EthereumAddress(
                id = addressDelta.address, last_transaction_block = addressDelta.blockNumber,
                balance = value, total_received = value,
                contract_address = addressDelta.source == CONTRACT,
                tx_number = 1, mined_block_number = 0, uncle_number = 0
        )
    }
    return updateExistingAddressByTransaction(address, addressDelta)
}


private fun updateExistingAddressByTransaction(address: EthereumAddress, addressDelta: AddressDelta): EthereumAddress {

    val balance = BigDecimal(address.balance) + addressDelta.delta

    val sign = addressDelta.delta.signum()
    val totalReceived =
            if (sign > 0) address.total_received + addressDelta.delta else address.total_received

    return EthereumAddress(
            id = address.id, last_transaction_block = addressDelta.blockNumber, tx_number = address.tx_number + 1,
            balance = balance.toString(), total_received = totalReceived, contract_address = address.contract_address,
            mined_block_number = address.mined_block_number, uncle_number = address.uncle_number
    )
}