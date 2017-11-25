import fund.cyber.dao.pump.PumpsDaoService
import fund.cyber.pump.CS_START_BLOCK_DEFAULT
import fund.cyber.pump.PumpsConfiguration


fun getStartBlockNumber(
        applicationId: String, pumpsDaoService: PumpsDaoService): Long {

    return if (PumpsConfiguration.startBlock == CS_START_BLOCK_DEFAULT)
        pumpsDaoService.indexingProgressStore.get(applicationId)?.block_number ?: 0
    else
        PumpsConfiguration.startBlock
}