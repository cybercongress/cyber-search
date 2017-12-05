package fund.cyber.pump.bitcoin_cash

import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.node.model.JsonRpcBitcoinBlock
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface
import fund.cyber.pump.FlowableBlockchain
import fund.cyber.pump.bitcoin.DownloadNextBlockFunction
import io.reactivex.Flowable
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

private val log = LoggerFactory.getLogger(BitcoinCashBlockchainInterface::class.java)!!


class BitcoinCashBlockchainInterface : BlockchainInterface, FlowableBlockchain {
    override val lastNetowrkBlock: Long
        get() = BitcoinCashPumpContext.bitcoinJsonRpcClient.getLastBlockNumber()
    private val downloadNextBlockFunction = DownloadNextBlockFunction(BitcoinCashPumpContext.bitcoinJsonRpcClient)

    override val chain = BITCOIN_CASH

    override fun subscribeBlocks(startBlockNumber: Long) =
            Flowable.generate<List<JsonRpcBitcoinBlock>, Long>(Callable { startBlockNumber }, downloadNextBlockFunction)
                    .flatMapIterable { items -> items }
                    .map(BitcoinCashPumpContext.jsonRpcToDaoBitcoinEntitiesConverter::convertToBundle)!!

    override fun blockBundleByNumber(number: Long): BlockBundle {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}