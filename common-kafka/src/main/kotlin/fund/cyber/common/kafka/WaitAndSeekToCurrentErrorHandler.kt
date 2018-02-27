package fund.cyber.common.kafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import java.lang.Exception
import java.util.concurrent.TimeUnit

//todo add to error handler exponential wait before retries
class WaitAndSeekToCurrentErrorHandler : SeekToCurrentErrorHandler() {

    override fun handle(
            thrownException: Exception, records: MutableList<ConsumerRecord<*, *>>?,
            consumer: Consumer<*, *>?, container: MessageListenerContainer) {

        TimeUnit.SECONDS.sleep(10)
        super.handle(thrownException, records, consumer, container)
    }
}