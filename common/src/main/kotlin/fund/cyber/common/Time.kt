package fund.cyber.common

import java.time.Instant
import java.time.temporal.ChronoUnit


fun Instant.millisFromNow() = ChronoUnit.MILLIS.between(Instant.now(), this)