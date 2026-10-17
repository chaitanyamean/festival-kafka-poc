package com.festivalkafka.orderapi.api;

import com.festivalkafka.orderapi.api.dto.CreateOrderRequest;
import com.festivalkafka.orderapi.api.dto.OrderResponse;
import com.festivalkafka.orderapi.service.OrderService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest req) {
        OrderResponse resp = orderService.create(req);
        // 202 Accepted: the DB write committed and the order.created event is in
        // flight. Terminal state (CONFIRMED or CANCELLED) is determined
        // asynchronously by the saga; clients poll GET /orders/{id} to observe it.
        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .location(URI.create("/orders/" + resp.orderId()))
                .body(resp);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID orderId) {
        return orderService.find(orderId)
                .map(OrderResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
