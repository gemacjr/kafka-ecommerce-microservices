package com.kafkademo.order.controller;

import com.kafkademo.order.model.CreateOrderRequest;
import com.kafkademo.order.model.Order;
import com.kafkademo.order.service.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
@RequestMapping("/api/orders")
@Tag(name = "Orders", description = "Create, retrieve, and cancel orders. Each order triggers an OrderPlacedEvent on the orders.placed Kafka topic.")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @Operation(
        summary = "Create a new order",
        description = "Creates an order with PENDING status and publishes an OrderPlacedEvent to the orders.placed topic. " +
                      "Payment and inventory services will each consume the event and respond via their respective topics.",
        responses = {
            @ApiResponse(responseCode = "201", description = "Order created",
                content = @Content(schema = @Schema(implementation = Order.class))),
        }
    )
    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody CreateOrderRequest request) {
        log.info("Received request to create order for customerId: {}, productId: {}",
                request.getCustomerId(), request.getProductId());
        Order order = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @Operation(
        summary = "Get order by ID",
        description = "Returns the current state of an order. Status progresses from PENDING → PAYMENT_CONFIRMED / STOCK_CONFIRMED → CONFIRMED.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Order found",
                content = @Content(schema = @Schema(implementation = Order.class))),
            @ApiResponse(responseCode = "404", description = "Order not found", content = @Content),
        }
    )
    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(
            @Parameter(description = "UUID of the order", example = "550e8400-e29b-41d4-a716-446655440000")
            @PathVariable String orderId) {
        log.info("Received request to get order with id: {}", orderId);
        Optional<Order> order = orderService.getOrder(orderId);
        return order
                .map(ResponseEntity::ok)
                .orElseGet(() -> {
                    log.warn("Order not found for orderId: {}", orderId);
                    return ResponseEntity.notFound().build();
                });
    }

    @Operation(
        summary = "Cancel an order",
        description = "Cancels the order and publishes an OrderCancelledEvent to the orders.cancelled topic. " +
                      "Inventory service will release any reserved stock.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Order cancelled"),
            @ApiResponse(responseCode = "404", description = "Order not found", content = @Content),
        }
    )
    @PostMapping("/{orderId}/cancel")
    public ResponseEntity<Void> cancelOrder(
            @Parameter(description = "UUID of the order to cancel")
            @PathVariable String orderId) {
        log.info("Received request to cancel order with id: {}", orderId);
        Optional<Order> order = orderService.getOrder(orderId);
        if (order.isEmpty()) {
            log.warn("Cannot cancel: order not found for orderId: {}", orderId);
            return ResponseEntity.notFound().build();
        }
        orderService.cancelOrder(orderId);
        return ResponseEntity.ok().build();
    }
}
