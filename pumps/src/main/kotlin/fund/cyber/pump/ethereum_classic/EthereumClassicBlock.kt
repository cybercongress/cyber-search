package fund.cyber.pump.ethereum_classic

import fund.cyber.pump.Block
import org.web3j.protocol.core.methods.response.EthBlock

class EthereumClassicBlock(parityBlock: EthBlock): Block() {
    val parityBlock = parityBlock
}