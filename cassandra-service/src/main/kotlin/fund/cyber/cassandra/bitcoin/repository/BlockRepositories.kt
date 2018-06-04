package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinBlock
import fund.cyber.cassandra.common.SearchRepository

interface BitcoinBlockRepository : SearchRepository<CqlBitcoinBlock, Long>
