import fund.cyber.dao.pump.PumpsDaoService
import fund.cyber.pump.CS_START_BLOCK_DEFAULT
import fund.cyber.pump.PumpConfiguration


fun getStartBlockNumber(
        applicationId: String, pumpsDaoService: PumpsDaoService, configuration: PumpConfiguration): Long {

    return if (configuration.startBlock == CS_START_BLOCK_DEFAULT)
        pumpsDaoService.indexingProgressStore.get(applicationId)?.block_number ?: 0
    else
        configuration.startBlock
}