package fund.cyber.contract.ethereum.summary

import fund.cyber.contract.common.delta.ContractSummaryDelta
import fund.cyber.contract.common.delta.DeltaMerger
import fund.cyber.contract.common.delta.DeltaProcessor
import fund.cyber.cassandra.common.CqlContractSummary
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractSummary
import fund.cyber.common.sumByDecimal
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Instant

data class EthereumContractSummaryDelta(
        override val contract: String,
        val balanceDelta: BigDecimal,
        val smartContract: Boolean?,
        val totalReceivedDelta: BigDecimal,
        val txNumberDelta: Int,
        val uncleNumberDelta: Int,
        val minedBlockNumberDelta: Int,
        val time: Instant,
        override val topic: String,
        override val partition: Int,
        override val offset: Long
) : ContractSummaryDelta<CqlEthereumContractSummary> {

    fun revertedDelta(): EthereumContractSummaryDelta = EthereumContractSummaryDelta(
            contract = contract, balanceDelta = -balanceDelta,
            txNumberDelta = -txNumberDelta, smartContract = smartContract,
            totalReceivedDelta = -totalReceivedDelta, uncleNumberDelta = -uncleNumberDelta,
            minedBlockNumberDelta = -minedBlockNumberDelta, topic = topic, partition = partition,
            offset = offset, time = time
    )

    override fun createSummary(): CqlEthereumContractSummary {
        return CqlEthereumContractSummary(
                hash = this.contract, confirmedBalance = this.balanceDelta, smartContract = this.smartContract
                ?: false,
                confirmedTotalReceived = this.totalReceivedDelta, txNumber = this.txNumberDelta,
                minedUncleNumber = this.uncleNumberDelta, minedBlockNumber = this.minedBlockNumberDelta,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = 0,
                firstActivityDate = this.time, lastActivityDate = this.time
        )
    }

    override fun updateSummary(summary: CqlEthereumContractSummary): CqlEthereumContractSummary {
        return CqlEthereumContractSummary(
                hash = summary.hash, confirmedBalance = summary.confirmedBalance + this.balanceDelta,
                smartContract = summary.smartContract,
                confirmedTotalReceived = summary.confirmedTotalReceived + this.totalReceivedDelta,
                txNumber = summary.txNumber + this.txNumberDelta,
                minedUncleNumber = summary.minedUncleNumber + this.uncleNumberDelta,
                minedBlockNumber = summary.minedBlockNumber + this.minedBlockNumberDelta,
                kafkaDeltaOffset = this.offset, kafkaDeltaTopic = this.topic,
                kafkaDeltaPartition = this.partition, version = summary.version + 1,
                firstActivityDate = summary.firstActivityDate, lastActivityDate = this.time
        )
    }
}

@Component
class EthereumTxDeltaProcessor : DeltaProcessor<EthereumTx, CqlEthereumContractSummary, EthereumContractSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumTx>): List<EthereumContractSummaryDelta> {

        val tx = record.value()
        val event = record.key()

        val contractDeltaByInput = EthereumContractSummaryDelta(
                contract = tx.from, txNumberDelta = 1, minedBlockNumberDelta = 0, uncleNumberDelta = 0,
                balanceDelta = tx.value.negate() - tx.fee, totalReceivedDelta = BigDecimal.ZERO,
                smartContract = (tx.createdSmartContract != null), topic = record.topic(), partition = record.partition(),
                offset = record.offset(), time = tx.blockTime
        )

        val contractDeltaByOutput = EthereumContractSummaryDelta(
                contract = (tx.to ?: tx.createdSmartContract)!!, txNumberDelta = 1, minedBlockNumberDelta = 0,
                uncleNumberDelta = 0, balanceDelta = tx.value, totalReceivedDelta = tx.value,
                smartContract = (tx.createdSmartContract != null), topic = record.topic(), partition = record.partition(),
                offset = record.offset(), time = tx.blockTime
        )

        return listOf(contractDeltaByInput, contractDeltaByOutput)
                .map { delta -> if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta }
    }

    override fun affectedContracts(records: List<ConsumerRecord<PumpEvent, EthereumTx>>): Set<String> {
        val allContracts: List<String> = records.flatMap { record ->
            val inContract = record.value().from
            val outContract = (record.value().to ?: record.value().createdSmartContract)!!
            return@flatMap listOf(inContract, outContract)
        }

        return allContracts.filter { contract -> contract.isNotEmpty() }.toSet()
    }

}

@Component
class EthereumBlockDeltaProcessor
    : DeltaProcessor<EthereumBlock, CqlEthereumContractSummary, EthereumContractSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumBlock>): List<EthereumContractSummaryDelta> {

        val block = record.value()
        val event = record.key()

        val finalReward = block.blockReward + block.txFees + block.unclesReward

        val delta = EthereumContractSummaryDelta(
                contract = block.minerContractHash, balanceDelta = finalReward, totalReceivedDelta = finalReward,
                txNumberDelta = 0, minedBlockNumberDelta = 1, uncleNumberDelta = 0,
                smartContract = null, topic = record.topic(), partition = record.partition(), offset = record.offset(),
                time = block.timestamp
        )
        return listOf(if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta)
    }

    override fun affectedContracts(records: List<ConsumerRecord<PumpEvent, EthereumBlock>>): Set<String> {
        return records.map { record -> record.value().minerContractHash }.toSet()
    }

}

@Component
class EthereumUncleDeltaProcessor
    : DeltaProcessor<EthereumUncle, CqlEthereumContractSummary, EthereumContractSummaryDelta> {

    override fun recordToDeltas(record: ConsumerRecord<PumpEvent, EthereumUncle>): List<EthereumContractSummaryDelta> {

        val uncle = record.value()
        val event = record.key()

        val delta = EthereumContractSummaryDelta(
                contract = uncle.miner, balanceDelta = uncle.uncleReward, totalReceivedDelta = uncle.uncleReward,
                txNumberDelta = 0, minedBlockNumberDelta = 0, uncleNumberDelta = 1,
                smartContract = null, topic = record.topic(), partition = record.partition(), offset = record.offset(),
                time = uncle.blockTime
        )
        return listOf(if (event == PumpEvent.DROPPED_BLOCK) delta.revertedDelta() else delta)
    }

    override fun affectedContracts(records: List<ConsumerRecord<PumpEvent, EthereumUncle>>): Set<String> {
        return records.map { record -> record.value().miner }.toSet()
    }

}

@Component
class EthereumDeltaMerger : DeltaMerger<EthereumContractSummaryDelta> {

    override fun mergeDeltas(deltas: Iterable<EthereumContractSummaryDelta>,
                             currentContracts: Map<String, CqlContractSummary>): EthereumContractSummaryDelta? {

        val first = deltas.first()
        val existingSummary = currentContracts[first.contract]


        val deltasToApply = deltas.filterNot { delta ->
            existingSummary != null && existingSummary.kafkaDeltaTopic == delta.topic
                    && existingSummary.kafkaDeltaPartition == delta.partition
                    && delta.offset <= existingSummary.kafkaDeltaOffset
        }
        val balance = deltasToApply.sumByDecimal { delta -> delta.balanceDelta }
        val totalReceived = deltasToApply.sumByDecimal { delta -> delta.totalReceivedDelta }
        val txNumber = deltasToApply.sumBy { delta -> delta.txNumberDelta }
        val blockNumber = deltasToApply.sumBy { delta -> delta.minedBlockNumberDelta }
        val uncleNumber = deltasToApply.sumBy { delta -> delta.uncleNumberDelta }

        return if (deltasToApply.isEmpty()) null else EthereumContractSummaryDelta(
                contract = first.contract, balanceDelta = balance, totalReceivedDelta = totalReceived,
                txNumberDelta = txNumber, minedBlockNumberDelta = blockNumber, uncleNumberDelta = uncleNumber,
                smartContract = deltasToApply.any { delta -> delta.smartContract ?: false },
                topic = first.topic, partition = first.partition,
                offset = deltasToApply.maxBy { it -> it.offset }!!.offset,
                time = deltasToApply.sortedByDescending { it -> it.time }.first().time
        )
    }
}
