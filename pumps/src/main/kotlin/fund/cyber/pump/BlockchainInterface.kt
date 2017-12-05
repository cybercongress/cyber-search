package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.model.CyberSearchItem

interface Blockchain {
    val chain: Chain
}

interface BlockchainInterface : Blockchain {
    val lastNetowrkBlock: Long
    fun blockBundleByNumber(number: Long): BlockBundle
}


interface BlockBundle {
    val chain: Chain
    val hash: String
    val parentHash: String
    val number: Long
}

class SimpleBlockBundle (
    override val chain: Chain,
    override val hash: String,
    override val parentHash: String,
    override val number: Long
) : BlockBundle {

    var entities: List<out CyberSearchItem> = emptyList()
    var actions: List<Pair<()->Unit, ()->Unit>> = emptyList()

    companion object {
        var actionSourceFactories: List<ActionSourceFactory> = emptyList()
    }

    fun <R: CyberSearchItem>push(value: R, cls: Class<R>) {
        this.entities += value

        actions += SimpleBlockBundle.actionSourceFactories
                .filter { factory -> factory.chain == chain }
                .map {
            it.actionFor(value, cls)
        }
    }

    fun add(action: Pair<()->Unit, ()->Unit>) {
        actions += action
    }
}