package fund.cyber.pump

import fund.cyber.node.common.Chain
import fund.cyber.node.model.CyberSearchItem

interface StorageInterface {
    fun initialize(blockchains: List<Blockchain>)
    fun constructAction(blockBundle: BlockBundle): StorageAction
}

interface StorageAction {
    companion object {
        val empty = EmptyStorageAction
    }

    fun store()
    fun remove()
}

object EmptyStorageAction: StorageAction {
    override fun store() {
        println("StorageAction: empty.store()")
    }

    override fun remove() {
        println("StorageAction: empty.remove()")
    }
}

class SimpleStorageAction(private val bundle: SimpleBlockBundle): StorageAction {
    var dependencies: List<StorageAction> = listOf()

    override fun store() {
        for (sa in this.dependencies) {
            sa.store()
        }

        bundle.actions.forEach {
            it.first()
        }
    }

    override fun remove() {
        for (sa in this.dependencies) {
            sa.remove()
        }

        bundle.actions.forEach {
            it.second()
        }
    }
}

interface ActionSourceFactory {
    val chain: Chain
    fun <R: CyberSearchItem>actionFor(value: R, cls: Class<R>): Pair<()->Unit, ()->Unit>
}