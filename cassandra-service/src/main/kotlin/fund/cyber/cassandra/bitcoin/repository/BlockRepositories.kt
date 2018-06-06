package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository

interface BitcoinBlockRepository : RoutingReactiveCassandraRepository<CqlBitcoinBlock, Long>
