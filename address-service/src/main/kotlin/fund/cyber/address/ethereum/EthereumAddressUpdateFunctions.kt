package fund.cyber.address.ethereum

import fund.cyber.address.common.AddressDelta
import fund.cyber.address.common.ConvertItemToAddressDeltaFunction
import fund.cyber.node.common.ChainEntity.*
import fund.cyber.node.model.EthereumAddressMinedBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.node.model.EthereumUncle
import java.math.BigDecimal


class EthereumTransactionToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<EthereumTransaction> {


    override fun invoke(tx: EthereumTransaction): List<AddressDelta> {

        val blockNumber = tx.block_number

         val addressDeltaByInput = EthereumAddressDelta(
                address = tx.from, blockNumber = blockNumber, txNumberDelta = 1,
                balanceDelta = tx.value.toBigDecimal().negate(), source = TRANSACTION
        )

        val addressDeltaByOutput = EthereumAddressDelta(
                address = (tx.to ?: tx.creates)!!, blockNumber = blockNumber, txNumberDelta = 1,
                balanceDelta = tx.value.toBigDecimal().negate(),
                source = if (tx.creates != null) CONTRACT else TRANSACTION
        )

        return listOf(addressDeltaByInput, addressDeltaByOutput)
    }
}


class EthereumUncleToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<EthereumUncle> {

    override fun invoke(uncle: EthereumUncle): List<AddressDelta> {
        val delta = EthereumAddressDelta(
                address = uncle.miner, balanceDelta = BigDecimal(uncle.uncle_reward), source = UNCLE,
                blockNumber = uncle.block_number, uncleNumberDelta = 1
        )
        return listOf(delta)
    }
}

class EthereumMinedBlockToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<EthereumAddressMinedBlock> {

    override fun invoke(block: EthereumAddressMinedBlock): List<AddressDelta> {

        val finalReward = block.block_reward + block.tx_fees + block.uncles_reward

        val delta = EthereumAddressDelta(
                address = block.miner, balanceDelta = finalReward, blockNumber = block.block_number,
                source = BLOCK, minedBlockNumberDelta = 1
        )
        return listOf(delta)
    }
}