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
 * Real Pulsar consumers, real listener thread pool (5 threads, set on the
 * client in PulsarConfig). NOTE: a single Consumer object always invokes its
 * MessageListener sequentially, one message at a time - listenerThreads only
 * lets *separate* consumer objects (or partitions) run concurrently. So to
 * get genuine 5-way parallelism on a Shared subscription, we open 5 separate
 * Consumer instances rather than one. This is also realistic: it's the same
 * lever you'd pull in a real service (consumer/partition count), not
 * something specific to this demo.
 *
 * Each message triggers a plain blocking call to the vendor with no timeout -
 * exactly the shape of the real incident. When the vendor goes slow, every
 * one of these 5 consumers ends up parked, and the pool is fully saturated.
 */
@Component
class UnprotectedConsumer(
    @Qualifier("unprotectedPulsarClient") client: PulsarClient,
    private val vendorClient: VendorClient,
    private val stats: StatsRegistry,
) {
    private val listener = MessageListener<ByteArray> { c, msg ->
        stats.unprotectedBusy.incrementAndGet()
        updateSaturation()
        try {
            runBlocking { vendorClient.callDirect() }
            c.acknowledge(msg)
            stats.unprotectedSuccess.incrementAndGet()
        } catch (e: Exception) {
            c.negativeAcknowledge(msg)
            stats.unprotectedFailed.incrementAndGet()
        } finally {
            stats.unprotectedBusy.decrementAndGet()
            updateSaturation()
        }
    }

    private val consumers = (1..5).map { i ->
        client.newConsumer()
            .topic(UNPROTECTED_TOPIC)
            .subscriptionName("unprotected-sub")
            .subscriptionType(SubscriptionType.Shared)
            .consumerName("unprotected-consumer-$i")
            .messageListener(listener)
            .subscribe()
    }

    @PreDestroy
    fun stop() = consumers.forEach { it.close() }

    private fun updateSaturation() {
        if (stats.unprotectedBusy.get() >= 5) {
            if (stats.unprotectedSaturatedSinceMillis == null) {
                stats.unprotectedSaturatedSinceMillis = System.currentTimeMillis()
            }
        } else {
            stats.unprotectedSaturatedSinceMillis = null
        }
    }
}
