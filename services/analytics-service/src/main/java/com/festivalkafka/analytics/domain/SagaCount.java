package com.festivalkafka.analytics.domain;

import java.time.OffsetDateTime;

public record SagaCount(String eventType, long eventCount, OffsetDateTime updatedAt) {}
