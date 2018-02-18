package fund.cyber.pump.ethereum.client

import fund.cyber.pump.common.BlockBundle
import fund.cyber.pump.common.BlockchainInterface
import fund.cyber.search.common.await
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTransaction
import fund.cyber.search.model.ethereum.EthereumUncle
import org.springframework.stereotype.Component
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import java.math.BigInteger

class EthereumBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        val block: EthereumBlock?,
        val uncles: List<EthereumUncle> = emptyList(),
        val transactions: List<EthereumTransaction> = emptyList()
) : BlockBundle


@Component
class EthereumBlockchainInterface(
        val parityClient: Web3j,
        val parityToBundleConverter: ParityToEthereumBundleConverter
) : BlockchainInterface<EthereumBlockBundle> {

    override fun lastNetworkBlock() = parityClient.ethBlockNumber().send().blockNumber.longValueExact()


    override fun blockBundleByNumber(number: Long): EthereumBlockBundle {
        val blockParameter = blockParameter(number.toBigInteger())
        val ethBlock = parityClient.ethGetBlockByNumber(blockParameter, true).send()

        val unclesFutures = ethBlock.block.uncles.mapIndexed { index, _ ->
            parityClient.ethGetUncleByBlockHashAndIndex(ethBlock.block.hash, BigInteger.valueOf(index.toLong())).sendAsync()
        }
        val uncles = unclesFutures.await().map { uncleEthBlock -> uncleEthBlock.block }

        return parityToBundleConverter.convert(ethBlock.block, uncles)
    }

    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
}