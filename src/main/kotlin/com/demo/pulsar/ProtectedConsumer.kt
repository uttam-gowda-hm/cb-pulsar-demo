package com.demo.pulsar

import com.demo.consumer.StatsRegistry
import com.demo.vendor.VendorClient
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.runBlocking
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionType
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Same pool size (5), same incoming load, same broker - the only difference
 * is the vendor call goes through the circuit breaker + timeout + bulkhead.
 *
 * Important bit for a Pulsar consumer specifically: on a DEFERRED result we
 * negativeAcknowledge instead of acknowledging. That tells Pulsar to
 * redeliver the message later (per the negative-ack redelivery delay), so a
 * message that got fast-failed by the breaker isn't lost - it just comes
 * back around once the vendor (and therefore the breaker) has recovered.
 */
@Component
class ProtectedConsumer(
    @Qualifier("protectedPulsarClient") client: PulsarClient,
    private val vendorClient: VendorClient,
    private val stats: StatsRegistry,
) {
    private val listener = MessageListener<ByteArray> { c, msg ->
        stats.protectedBusy.incrementAndGet()
        try {
            val result = runBlocking { vendorClient.callProtected() }
            if (result == "DEFERRED") {
                c.negativeAcknowledge(msg) // redelivered later, not lost
                stats.protectedDeferred.incrementAndGet()
            } else {
                c.acknowledge(msg)
                stats.protectedSuccess.incrementAndGet()
            }
        } catch (e: Exception) {
            c.negativeAcknowledge(msg)
            stats.protectedFailed.incrementAndGet()
        } finally {
            stats.protectedBusy.decrementAndGet()
        }
    }

    // Same reasoning as the unprotected side: 5 separate Consumer instances,
    // not one, so we get genuine concurrent processing.
    private val consumers = (1..5).map { i ->
        client.newConsumer()
            .topic(PROTECTED_TOPIC)
            .subscriptionName("protected-sub")
            .subscriptionType(SubscriptionType.Shared)
            .consumerName("protected-consumer-$i")
            .negativeAckRedeliveryDelay(5, java.util.concurrent.TimeUnit.SECONDS)
            .messageListener(listener)
            .subscribe()
    }

    @PreDestroy
    fun stop() = consumers.forEach { it.close() }
}
