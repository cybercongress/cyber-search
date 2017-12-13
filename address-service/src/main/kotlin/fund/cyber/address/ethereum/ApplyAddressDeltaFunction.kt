package fund.cyber.address.ethereum

import fund.cyber.address.common.ApplyAddressDeltaFunction
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.ChainEntity.CONTRACT
import fund.cyber.node.model.EthereumAddress
import org.ehcache.Cache
import java.math.BigDecimal


class ApplyEthereumAddressDeltaFunction(
        private val repository: EthereumKeyspaceRepository,
        private val addressCache: Cache<String, EthereumAddress>
) : ApplyAddressDeltaFunction<EthereumAddressDelta> {

    override fun invoke(addressDelta: EthereumAddressDelta) {

        val address: EthereumAddress?
                = addressCache[addressDelta.address] ?: repository.addressStore.get(addressDelta.address)

        val balanceDeltaSign = addressDelta.balanceDelta.signum()
        val currentTotalReceived = BigDecimal(address?.total_received ?: "0")

        val updatedTotalReceived =
                if (balanceDeltaSign > 0) currentTotalReceived + addressDelta.balanceDelta else currentTotalReceived

        val updatedBalance = BigDecimal(address?.balance ?: "0") + addressDelta.balanceDelta
        val updatedTxNumber = (address?.tx_number ?: 0) + addressDelta.txNumberDelta
        val updatedUnclesNumber = (address?.uncle_number ?: 0) + addressDelta.uncleNumberDelta
        val updatedMinedBlockNumber = (address?.mined_block_number ?: 0) + addressDelta.minedBlockNumberDelta

        val updatedAddress = address?.copy(
                balance = updatedBalance.toString(), total_received = updatedTotalReceived.toString(),
                tx_number = updatedTxNumber, mined_block_number = updatedMinedBlockNumber,
                uncle_number = updatedUnclesNumber
        ) ?: EthereumAddress(
                id = addressDelta.address, last_transaction_block = addressDelta.blockNumber,
                balance = updatedBalance.toString(), total_received = updatedTotalReceived.toString(),
                contract_address = addressDelta.source == CONTRACT,
                tx_number = updatedTxNumber, mined_block_number = updatedMinedBlockNumber,
                uncle_number = updatedUnclesNumber
        )

        repository.addressStore.save(updatedAddress)
        addressCache.put(updatedAddress.id, updatedAddress)
    }
}