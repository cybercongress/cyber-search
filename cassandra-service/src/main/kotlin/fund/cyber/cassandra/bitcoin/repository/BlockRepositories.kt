package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository

interface BitcoinBlockRepository : ReactiveCassandraRepository<CqlBitcoinBlock, Long>
