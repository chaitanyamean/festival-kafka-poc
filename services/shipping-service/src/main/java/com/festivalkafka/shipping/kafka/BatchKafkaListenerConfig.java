package com.festivalkafka.shipping.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Dedicated listener container factory for Phase 3 batch mode. We don't flip
 * {@code spring.kafka.listener.type=batch} globally because other listeners
 * (if any) may still want record-mode semantics; this factory is referenced
 * explicitly from {@code @KafkaListener(containerFactory=...)}.
 *
 * <p>Concurrency honours {@code KAFKA_LISTENER_CONCURRENCY} so the Phase 3
 * profile can scale consumer threads to match the (also Phase 3) 12-partition
 * {@code payment.captured} topic.
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
