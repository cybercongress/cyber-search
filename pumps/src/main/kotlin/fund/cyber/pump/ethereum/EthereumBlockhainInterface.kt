package fund.cyber.pump.ethereum

import fund.cyber.cassandra.migration.Migration
import fund.cyber.cassandra.migration.Migratory
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.ETHEREUM
import fund.cyber.node.common.await
import fund.cyber.node.common.env
import fund.cyber.node.model.*
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface
import fund.cyber.pump.PumpsContext
import org.apache.http.impl.client.CloseableHttpClient
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import java.math.BigInteger

class EthereumBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val chain: Chain,
        val block: EthereumBlock,
        val addressBlock: EthereumAddressMinedBlock,
        val uncles: List<EthereumUncle>,
        val addressUncles: List<EthereumAddressUncle>,
        val blockTxesPreviews: List<EthereumBlockTxPreview>,
        val addressTxesPreviews: List<EthereumAddressTxPreview>,
        val transactions: List<EthereumTransaction>
) : BlockBundle {

    @Suppress("UNCHECKED_CAST")
    override fun elementsMap(): Map<Class<CyberSearchItem>, List<CyberSearchItem>> {

        val map: MutableMap<Class<CyberSearchItem>, List<CyberSearchItem>> = mutableMapOf()
        map.put(EthereumBlock::class.java as Class<CyberSearchItem>, listOf(block))
        map.put(EthereumAddressMinedBlock::class.java as Class<CyberSearchItem>, listOf(addressBlock))
        map.put(EthereumUncle::class.java as Class<CyberSearchItem>, uncles)
        map.put(EthereumAddressUncle::class.java as Class<CyberSearchItem>, addressUncles)
        map.put(EthereumTransaction::class.java as Class<CyberSearchItem>, transactions)
        map.put(EthereumBlockTxPreview::class.java as Class<CyberSearchItem>, blockTxesPreviews)
        map.put(EthereumAddressTxPreview::class.java as Class<CyberSearchItem>, addressTxesPreviews)

        return map
    }
}


open class EthereumBlockchainInterface(
        parityUrl: String = env("PARITY_ETH_URL", "http://127.0.0.1:8545"),
        network: Chain = ETHEREUM,
        httpClient: CloseableHttpClient = PumpsContext.httpClient

) : BlockchainInterface<EthereumBlockBundle>, Migratory {

    override val migrations: List<Migration> = EthereumMigrations.migrations
    override val chain: Chain = network


    private val parityClient: Web3j = Web3j.build(HttpService(parityUrl, httpClient))
    private val parityToBundleConverter = ParityToEthereumBundleConverter(network)


    override fun lastNetworkBlock() = parityClient.ethBlockNumber().send().blockNumber.longValueExact()

    override fun blockBundleByNumber(number: Long): EthereumBlockBundle {

        val blockParameter = blockParameter(BigInteger(number.toString()))
        val ethBlock = parityClient.ethGetBlockByNumber(blockParameter, true).send()

        val unclesFutures = ethBlock.block.uncles.mapIndexed { index, _ ->
            parityClient.ethGetUncleByBlockHashAndIndex(ethBlock.block.hash, index.toBigInteger()).sendAsync()
        }
        val uncles = unclesFutures.await().map { uncleEthBlock -> uncleEthBlock.block }

        return parityToBundleConverter.convertToBundle(ethBlock.block, uncles)
    }

    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
}