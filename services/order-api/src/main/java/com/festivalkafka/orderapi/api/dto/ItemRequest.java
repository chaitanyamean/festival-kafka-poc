package com.festivalkafka.orderapi.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record ItemRequest(
        @NotBlank @Size(max = 64) String skuId,
        @Positive int quantity,
        @Positive long unitPriceCents) {
}
