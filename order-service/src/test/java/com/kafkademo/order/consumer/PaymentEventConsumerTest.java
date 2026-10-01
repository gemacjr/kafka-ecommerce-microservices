package com.kafkademo.order.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafkademo.order.model.CreateOrderRequest;
import com.kafkademo.order.model.Order;
import com.kafkademo.order.model.OrderStatus;
import com.kafkademo.order.model.PaymentProcessedEvent;
import com.kafkademo.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"payments.processed", "orders.placed", "orders.cancelled", "inventory.reserved"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:19093", "port=19093"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext
@ActiveProfiles("test")
class PaymentEventConsumerTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private OrderService orderService;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Test
    void consumePaymentEvent_withSuccess_shouldUpdateOrderStatusToPaymentConfirmed() throws Exception {
        // Step 1: Create an order so it exists in the service's in-memory store
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-test-001");
        request.setProductId("product-test-001");
        request.setQuantity(1);
        request.setTotalAmount(new BigDecimal("29.99"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);

        // Step 2: Build and publish a PaymentProcessedEvent as JSON to payments.processed
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                orderId,
                "payment-" + System.currentTimeMillis(),
                "SUCCESS",
                null,
                LocalDateTime.now()
        );

        String payloadJson = objectMapper.writeValueAsString(paymentEvent);
        kafkaTemplate.send("payments.processed", orderId, payloadJson);

        // Step 3: Wait for the consumer to process the event
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // Step 4: Assert the order status was updated
        Optional<Order> updatedOrder = orderService.getOrder(orderId);
        assertThat(updatedOrder).isPresent();
        assertThat(updatedOrder.get().getStatus())
                .isIn(OrderStatus.PAYMENT_CONFIRMED, OrderStatus.CONFIRMED);
    }

    @Test
    void consumePaymentEvent_withFailure_shouldUpdateOrderStatusToPaymentFailed() throws Exception {
        // Step 1: Create an order
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-test-002");
        request.setProductId("product-test-002");
        request.setQuantity(2);
        request.setTotalAmount(new BigDecimal("99.99"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        // Step 2: Publish a failed payment event
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                orderId,
                "payment-failed-" + System.currentTimeMillis(),
                "FAILED",
                "Insufficient funds",
                LocalDateTime.now()
        );

        String payloadJson = objectMapper.writeValueAsString(paymentEvent);
        kafkaTemplate.send("payments.processed", orderId, payloadJson);

        // Step 3: Wait for the consumer
        Thread.sleep(3000);

        // Step 4: Assert the order status
        Optional<Order> updatedOrder = orderService.getOrder(orderId);
        assertThat(updatedOrder).isPresent();
        assertThat(updatedOrder.get().getStatus()).isEqualTo(OrderStatus.PAYMENT_FAILED);
    }
}
