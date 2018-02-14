package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import org.springframework.data.repository.reactive.ReactiveCrudRepository


interface BitcoinAddressSummaryRepository : ReactiveCrudRepository<CqlBitcoinAddressSummary, String>


