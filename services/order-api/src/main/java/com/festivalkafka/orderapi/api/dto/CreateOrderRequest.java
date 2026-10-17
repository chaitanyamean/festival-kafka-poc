package com.festivalkafka.orderapi.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;

public record CreateOrderRequest(
        @NotNull UUID customerId,
        @NotEmpty @Size(max = 50) @Valid List<ItemRequest> items,
        @NotBlank @Size(max = 64) String city,
        @NotBlank @Size(max = 32) String paymentMethod) {
}
