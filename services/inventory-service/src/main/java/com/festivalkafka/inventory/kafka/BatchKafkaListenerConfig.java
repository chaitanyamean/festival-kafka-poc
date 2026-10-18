package com.festivalkafka.inventory.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Dedicated batch-mode listener container factory, used by OrderCreatedListener.
 * PaymentFailedListener stays record-mode because the compensation path volume
 * is tiny (< 100 msg/s even at Phase 3 peaks) — batching gains nothing there.
 */
@Configuration
public class BatchKafkaListenerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ConsumerRecord<String, ?>> batchKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            @Value("${spring.kafka.listener.concurrency:1}") int concurrency) {

        @SuppressWarnings({"unchecked", "rawtypes"})
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        return factory;
    }
}
