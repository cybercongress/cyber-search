package fund.cyber.pump.ethereum

import fund.cyber.dao.migration.Migratory
import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.ETHEREUM
import fund.cyber.node.common.env
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface
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
        val transactions: List<EthereumTransaction>
) : BlockBundle


open class EthereumBlockchainInterface(
        parityUrl: String = env("PARITY_ETHEREUM", "http://cyber:cyber@127.0.0.1:8545"),
        network: Chain = ETHEREUM

) : BlockchainInterface<EthereumBlockBundle>, Migratory {

    override val migrations: List<Migration> = EthereumMigrations.migrations
    override val chain: Chain = network

    private val parityClient: Web3j = Web3j.build(HttpService(parityUrl))
    private val parityToBundleConverter = ParityToEthereumBundleConverter(network)


    override fun lastNetworkBlock() = parityClient.ethBlockNumber().send().blockNumber.longValueExact()

    override fun blockBundleByNumber(number: Long): EthereumBlockBundle {

        val blockParameter = blockParameter(BigInteger(number.toString()))
        val ethBlock = parityClient.ethGetBlockByNumber(blockParameter, true).send()
        return parityToBundleConverter.convertToBundle(ethBlock.block)
    }

    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
}