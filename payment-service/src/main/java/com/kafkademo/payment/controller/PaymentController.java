package com.kafkademo.payment.controller;

import com.kafkademo.payment.model.Payment;
import com.kafkademo.payment.service.PaymentProcessingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/payments")
@Tag(name = "Payments", description = "Query payment results and trigger DLQ scenarios. Payments are processed asynchronously via the orders.placed Kafka topic.")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentProcessingService paymentProcessingService;

    public PaymentController(PaymentProcessingService paymentProcessingService) {
        this.paymentProcessingService = paymentProcessingService;
    }

    @Operation(
        summary = "Get payment by order ID",
        description = "Returns the payment result for a given order. Payment is processed asynchronously — " +
                      "poll this endpoint after placing an order to see the result.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Payment found",
                content = @Content(schema = @Schema(implementation = Payment.class))),
            @ApiResponse(responseCode = "404", description = "Payment not yet processed or order not found", content = @Content),
        }
    )
    @GetMapping("/{orderId}")
    public ResponseEntity<Payment> getPayment(
            @Parameter(description = "UUID of the order", example = "550e8400-e29b-41d4-a716-446655440000")
            @PathVariable String orderId) {
        log.debug("GET /api/payments/{}", orderId);
        Optional<Payment> payment = paymentProcessingService.getPayment(orderId);
        return payment
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(
        summary = "Register an order ID for forced payment failure",
        description = "Pre-registers an order ID so that when its OrderPlacedEvent arrives on Kafka, " +
                      "the payment processor throws a PaymentProcessingException immediately (non-retryable). " +
                      "The original Kafka record is routed to payments.dlq without any retry attempts. " +
                      "Use this to demo the DLQ flow: call this endpoint first, then POST /api/orders with the same orderId.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Order ID registered for forced failure"),
        }
    )
    @PostMapping("/force-fail/{orderId}")
    public ResponseEntity<Map<String, String>> forceFail(
            @Parameter(description = "UUID to register for forced failure — use the same ID when creating the order")
            @PathVariable String orderId) {
        log.info("POST /api/payments/force-fail/{}", orderId);
        paymentProcessingService.forceFail(orderId);
        return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "message", "Order ID '" + orderId + "' has been registered for forced failure. " +
                           "When the next OrderPlacedEvent with this ID is consumed, a " +
                           "PaymentProcessingException will be thrown immediately (no retries) " +
                           "and the original Kafka record will be routed to payments.dlq."
        ));
    }
}
