package fund.cyber.pump.ethereum

import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.node.model.*
import fund.cyber.pump.*
import fund.cyber.pump.ethereum_classic.EthereumClassicMigrations
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.util.concurrent.Executors

class EthereumBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val chain: Chain,
        val block: EthereumBlock,
        val transactions: List<EthereumTransaction>
) : BlockBundle

const val BATCH_SIZE_DEFAULT: Long = 1

val dao = EthereumDaoService(PumpsContext.cassandra)

class EthereumBlockchainInterface(url: String, override val chain: Chain) : BlockchainInterface, Migratory {
    private val parityToDaoConverter = EthereumParityToDaoConverter()
    private var batchSize: BigInteger = BigInteger.valueOf(BATCH_SIZE_DEFAULT)
    private val executorService = Executors.newScheduledThreadPool(batchSize.toInt())
    private val parityClient = Web3j.build(HttpService(url), 15 * 1000, executorService)

    override val migrations: List<Migration>
        get() = if (chain == Chain.ETHEREUM) {
            EthereumMigrations.migrations
        } else {
            EthereumClassicMigrations.migrations
        }

    override fun blockBundleByNumber(number: Long): SimpleBlockBundle<EthereumItem> {
        val _block = parityClient.ethGetBlockByNumber(this.blockParameter(java.math.BigInteger(number.toString())), true).send()
        val block = parityToDaoConverter.parityBlockToDao(_block.block)

        val blockBundle = SimpleBlockBundle<EthereumItem>(
                hash = block.hash,
                parentHash = block.parent_hash,
                number = block.number,
                chain = Chain.ETHEREUM,
                manager = dao.manager
        )

        blockBundle.push(block)
        parityToDaoConverter.parityTransactionsToDao(_block.block).forEach {
            blockBundle.push(it)
        }

        return blockBundle
    }

    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
}

