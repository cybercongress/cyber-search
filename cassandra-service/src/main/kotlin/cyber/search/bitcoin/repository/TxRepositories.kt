package cyber.search.bitcoin.repository

import cyber.search.bitcoin.model.BitcoinTransaction
import org.springframework.data.repository.reactive.ReactiveCrudRepository



interface BitcoinTxRepository : ReactiveCrudRepository<BitcoinTransaction, String>