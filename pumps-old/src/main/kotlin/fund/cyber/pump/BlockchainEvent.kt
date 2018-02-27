package fund.cyber.pump

interface BlockchainEvent

class Error: BlockchainEvent

data class CommitBlock(val bundle: BlockBundle): BlockchainEvent

data class RevertBlock(val bundle: BlockBundle): BlockchainEvent

class UpToDate: BlockchainEvent

class ChainReorganizationBegin: BlockchainEvent

class ChainReorganizationEnd: BlockchainEvent
