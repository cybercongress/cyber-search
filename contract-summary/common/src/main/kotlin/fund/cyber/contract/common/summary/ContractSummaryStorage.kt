package fund.cyber.contract.common.summary

import fund.cyber.cassandra.common.CqlContractSummary
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface ContractSummaryStorage<S: CqlContractSummary> {
    fun findById(id: String): Mono<S>
    fun findAllByIdIn(ids: Iterable<String>): Flux<S>
    fun update(summary: S, oldVersion: Long): Mono<Boolean>
    fun insertIfNotRecord(summary: S): Mono<Boolean>
    fun commitUpdate(address: String, newVersion: Long): Mono<Boolean>
    fun update(summary: S): Mono<S>
    fun remove(address: String): Mono<Void>
}
