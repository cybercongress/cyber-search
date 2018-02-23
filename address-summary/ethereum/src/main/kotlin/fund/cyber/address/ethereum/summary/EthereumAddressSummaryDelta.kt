package fund.cyber.address.ethereum.summary

import fund.cyber.address.common.delta.AddressSummaryDelta
import fund.cyber.address.common.delta.DeltaMerger
import fund.cyber.address.common.delta.DeltaProcessor
import fund.cyber.cassandra.common.CqlAddressSummary
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import fund.cyber.search.common.sumByDecimal
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import java.math.BigDecimal

data class EthereumAddressSummaryDelta(
        override val address: String,
        val balanceDelta: BigDecimal,
        val contractAddress: Boolean?,
        val totalReceivedDelta: BigDecimal,
        val txNumberDelta: Int,
        val uncleNumberDelta: Int,
        val minedBlockNumberDelta: Int,
        override val topic: String,
        override val partition: Int,
        override val offset: Long
) : AddressSummaryDelta<CqlEthereumAddressSummary> {

    fun revertedDelta(): EthereumAddressSummaryDelta = EthereumAddressSummaryDelta(
            address = address, balanceDelta = -balanceDelta,
            txNumberDelta = -txNumberDelta, contractAddress = contractAddress,
            totalReceivedDelta = -totalReceivedDelta, uncleNumberDelta = -uncleNumberDelta,
            minedBlockNumberDelta = -minedBlockNumberDelta, topic = topic, partition = partition,
            offset = offset
    )

    override fun createSummary(): CqlEthereumAddressSummary {
        return CqlEthereumAddressSummary(
                id = this.address, confirmedBalance = this.balanceDelta, contractAddress = this.contractAddress
                ?: false,
                confirmedTotalReceived = this.totalReceivedDelta, txNumber = this.txNumberDelta,
                minedUncleNumber = this.uncleNumberDelta, minedBlockNumber = this.minedBlockNumberDelta,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = 0
        )
    }

    override fun updateSummary(summary: CqlEthereumAddressSummary): CqlEthereumAddressSummary {
        return CqlEthereumAddressSummary(
                id = summary.id, confirmedBalance = summary.confirmedBalance + this.balanceDelta, contractAddress = summary.contractAddress,
                confirmedTotalReceived = summary.confirmedTotalReceived + this.totalReceivedDelta,
                txNumber = summary.txNumber + this.txNumberDelta, minedUncleNumber = summary.minedUncleNumber + this.uncleNumberDelta,
                minedBlockNumber = summary.minedBlockNumber + this.minedBlockNumberDelta,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = summary.version + 1
        )
    }
}

@Component
class EthereumTxDeltaProcessor : DeltaProcessor<EthereumTx, CqlEthereumAddressSummary, EthereumAddressSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumTx>): List<EthereumAddressSummaryDelta> {

        val tx = record.value()
        val event = record.key()

        val addressDeltaByInput = EthereumAddressSummaryDelta(
                address = tx.from, txNumberDelta = 1, minedBlockNumberDelta = 0, uncleNumberDelta = 0,
                balanceDelta = tx.value.negate(), totalReceivedDelta = tx.value.negate(),
                contractAddress = (tx.createdContract != null), topic = record.topic(), partition = record.partition(),
                offset = record.offset()
        )

        val addressDeltaByOutput = EthereumAddressSummaryDelta(
                address = (tx.to ?: tx.createdContract)!!, txNumberDelta = 1, minedBlockNumberDelta = 0, uncleNumberDelta = 0,
                balanceDelta = tx.value, totalReceivedDelta = tx.value,
                contractAddress = (tx.createdContract != null), topic = record.topic(), partition = record.partition(),
                offset = record.offset()
        )

        return listOf(addressDeltaByInput, addressDeltaByOutput)
                .map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
    }

    override fun affectedAddresses(records: List<ConsumerRecord<PumpEvent, EthereumTx>>): Set<String> {
        val allAddresses: List<String> = records.flatMap { record ->
            val inAddress = record.value().from
            val outAddress = (record.value().to ?: record.value().createdContract)!!
            return@flatMap listOf(inAddress, outAddress)
        }

        return allAddresses.toSet()
    }

}

@Component
class EthereumBlockDeltaProcessor : DeltaProcessor<EthereumBlock, CqlEthereumAddressSummary, EthereumAddressSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumBlock>): List<EthereumAddressSummaryDelta> {

        val block = record.value()
        val event = record.key()

        val finalReward = block.blockReward + block.txFees + block.unclesReward

        val delta = EthereumAddressSummaryDelta(
                address = block.miner, balanceDelta = finalReward, totalReceivedDelta = finalReward,
                txNumberDelta = 0, minedBlockNumberDelta = 1, uncleNumberDelta = 0,
                contractAddress = null, topic = record.topic(), partition = record.partition(), offset = record.offset()
        )
        return listOf(if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta)
    }

    override fun affectedAddresses(records: List<ConsumerRecord<PumpEvent, EthereumBlock>>): Set<String> {
        return records.map { record -> record.value().miner }.toSet()
    }

}

@Component
class EthereumUncleDeltaProcessor : DeltaProcessor<EthereumUncle, CqlEthereumAddressSummary, EthereumAddressSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumUncle>): List<EthereumAddressSummaryDelta> {

        val uncle = record.value()
        val event = record.key()

        val delta = EthereumAddressSummaryDelta(
                address = uncle.miner, balanceDelta = uncle.uncleReward, totalReceivedDelta = uncle.uncleReward,
                txNumberDelta = 0, minedBlockNumberDelta = 0, uncleNumberDelta = 1,
                contractAddress = null, topic = record.topic(), partition = record.partition(), offset = record.offset()
        )
        return listOf(if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta)
    }

    override fun affectedAddresses(records: List<ConsumerRecord<PumpEvent, EthereumUncle>>): Set<String> {
        return records.map { record -> record.value().miner }.toSet()
    }

}

@Component
class EthereumDeltaMerger : DeltaMerger<EthereumAddressSummaryDelta> {

    override fun mergeDeltas(deltas: Iterable<EthereumAddressSummaryDelta>,
                             currentAddresses: Map<String, CqlAddressSummary>): EthereumAddressSummaryDelta? {

        val first = deltas.first()
        val existingSummary = currentAddresses[first.address]


        val deltasToApply = deltas.filterNot { delta ->
            existingSummary != null && existingSummary.kafkaDeltaTopic == delta.topic
                    && existingSummary.kafkaDeltaPartition == delta.partition && delta.offset <= existingSummary.kafkaDeltaOffset
        }
        val balance = deltasToApply.sumByDecimal { delta -> delta.balanceDelta }
        val totalReceived = deltasToApply.sumByDecimal { delta -> delta.totalReceivedDelta }
        val txNumber = deltasToApply.sumBy { delta -> delta.txNumberDelta }
        val blockNumber = deltasToApply.sumBy { delta -> delta.minedBlockNumberDelta }
        val uncleNumber = deltasToApply.sumBy { delta -> delta.uncleNumberDelta }

        return if (deltasToApply.isEmpty()) null else EthereumAddressSummaryDelta(
                address = first.address, balanceDelta = balance, totalReceivedDelta = totalReceived,
                txNumberDelta = txNumber, minedBlockNumberDelta = blockNumber, uncleNumberDelta = uncleNumber,
                contractAddress = deltasToApply.any { delta -> delta.contractAddress ?: false },
                topic = first.topic, partition = first.partition, offset = deltasToApply.maxBy { it -> it.offset }!!.offset
        )
    }
}