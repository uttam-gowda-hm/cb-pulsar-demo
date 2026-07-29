package com.demo.pulsar

import org.apache.pulsar.client.api.PulsarClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

const val SERVICE_URL = "pulsar://localhost:6650"

/**
 * Two isolated clients, each with a small (5-thread) listener pool - this is
 * the direct equivalent of `.listenerThreads(n)` on the real Pulsar client,
 * which is exactly the pool that got exhausted in the real incident. Kept
 * separate here so you can see the unprotected and protected pools behave
 * independently; in production these often share one pool, which is *why*
 * one bad topic can take other consumers down with it.
 */
@Configuration
class PulsarConfig {

    @Bean(name = ["unprotectedPulsarClient"], destroyMethod = "close")
    fun unprotectedClient(): PulsarClient =
        PulsarClient.builder()
            .serviceUrl(SERVICE_URL)
            .listenerThreads(5)
            .build()

    @Bean(name = ["protectedPulsarClient"], destroyMethod = "close")
    fun protectedClient(): PulsarClient =
        PulsarClient.builder()
            .serviceUrl(SERVICE_URL)
            .listenerThreads(5)
            .build()

    @Bean(name = ["producerPulsarClient"], destroyMethod = "close")
    fun producerClient(): PulsarClient =
        PulsarClient.builder()
            .serviceUrl(SERVICE_URL)
            .build()
}
