package fund.cyber.pump

import rx.Observable

interface BlockchainInterface {
    val blocks: Observable<Block>
}