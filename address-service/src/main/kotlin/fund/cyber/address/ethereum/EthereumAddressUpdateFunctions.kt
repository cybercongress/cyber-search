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

        val addressDeltaByInput = AddressDelta(
                address = tx.input, blockNumber = blockNumber,
                delta = tx.value.toBigDecimal().negate(), source = TRANSACTION
        )

        val addressDeltaByOutput = AddressDelta(
                address = tx.input, blockNumber = blockNumber, delta = tx.value.toBigDecimal().negate(),
                source = if (tx.creates != null) CONTRACT else TRANSACTION
        )

        return listOf(addressDeltaByInput, addressDeltaByOutput)
    }
}


class EthereumUncleToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<EthereumUncle> {

    override fun invoke(uncle: EthereumUncle): List<AddressDelta> {
        val delta = AddressDelta(
                address = uncle.miner, delta = BigDecimal(uncle.uncle_reward),
                blockNumber = uncle.block_number, source = UNCLE
        )
        return listOf(delta)
    }
}

class EthereumMinedBlockToAddressDeltaFunction : ConvertItemToAddressDeltaFunction<EthereumAddressMinedBlock> {

    override fun invoke(block: EthereumAddressMinedBlock): List<AddressDelta> {

        val finalReward = block.block_reward + block.tx_fees + block.uncles_reward

        val delta = AddressDelta(
                address = block.miner, delta = finalReward, blockNumber = block.block_number, source = BLOCK
        )
        return listOf(delta)
    }
}