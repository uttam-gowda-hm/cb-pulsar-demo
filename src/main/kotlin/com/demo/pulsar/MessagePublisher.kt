package com.demo.pulsar

import com.demo.consumer.StatsRegistry
import org.apache.pulsar.client.api.PulsarClient
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean

const val UNPROTECTED_TOPIC = "persistent://public/default/unprotected-verification-requests"
const val PROTECTED_TOPIC = "persistent://public/default/protected-verification-requests"

/** Plays the role of customer requests flowing onto the topics. */
@Component
class MessagePublisher(
    @Qualifier("producerPulsarClient") client: PulsarClient,
    private val stats: StatsRegistry,
) {
    private val unprotectedProducer = client.newProducer()
        .topic(UNPROTECTED_TOPIC)
        .create()

    private val protectedProducer = client.newProducer()
        .topic(PROTECTED_TOPIC)
        .create()

    private val running = AtomicBoolean(false)

    fun start() = running.set(true)
    fun stop() = running.set(false)
    fun isRunning() = running.get()

    // Same traffic hits both topics at the same time, so the comparison is fair.
    @Scheduled(fixedDelay = 300)
    fun tick() {
        if (!running.get()) return
        val payload = "verify-request-${System.currentTimeMillis()}".toByteArray()
        unprotectedProducer.sendAsync(payload)
        protectedProducer.sendAsync(payload)
        stats.published.incrementAndGet()
    }
}
