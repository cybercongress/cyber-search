package fund.cyber.pump.ethereum.client

import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockchainInterface
import fund.cyber.pump.ethereum.client.genesis.EthereumGenesisDataProvider
import fund.cyber.search.common.await
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTransaction
import fund.cyber.search.model.ethereum.EthereumUncle
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigInteger

class EthereumBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val blockSize: Int,
        val block: EthereumBlock,
        val uncles: List<EthereumUncle>,
        val transactions: List<EthereumTransaction>
) : BlockBundle


@Component
class EthereumBlockchainInterface(
        private val parityClient: Web3j,
        private val parityToBundleConverter: ParityToEthereumBundleConverter,
        private val genesisDataProvider: EthereumGenesisDataProvider,
        monitoring: MeterRegistry
) : BlockchainInterface<EthereumBlockBundle> {

    private val downloadSpeedMonitor = monitoring.timer("pump_bundle_download")

    override fun lastNetworkBlock() = parityClient.ethBlockNumber().send().blockNumber.longValueExact()


    override fun blockBundleByNumber(number: Long): EthereumBlockBundle {


        val (ethBlock, uncles) = downloadSpeedMonitor.recordCallable { downloadBundleData(number) }

        val bundle = parityToBundleConverter.convert(ethBlock.block, uncles)
        return if (number == 0L) genesisDataProvider.provide(bundle) else bundle
    }

    private fun downloadBundleData(number: Long): Pair<EthBlock, List<EthBlock.Block>> {

        val blockParameter = blockParameter(number.toBigInteger())
        val ethBlock = parityClient.ethGetBlockByNumber(blockParameter, true).send()

        val unclesFutures = ethBlock.block.uncles.mapIndexed { index, _ ->
            parityClient.ethGetUncleByBlockHashAndIndex(ethBlock.block.hash, BigInteger.valueOf(index.toLong())).sendAsync()
        }
        val uncles = unclesFutures.await().map { uncleEthBlock -> uncleEthBlock.block }
        return Pair(ethBlock, uncles)
    }

    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
}