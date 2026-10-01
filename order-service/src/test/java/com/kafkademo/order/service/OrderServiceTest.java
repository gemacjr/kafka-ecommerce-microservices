package com.kafkademo.order.service;

import com.kafkademo.order.model.CreateOrderRequest;
import com.kafkademo.order.model.Order;
import com.kafkademo.order.model.OrderStatus;
import com.kafkademo.order.model.PaymentProcessedEvent;
import com.kafkademo.order.model.StockReservedEvent;
import com.kafkademo.order.producer.OrderEventProducer;
import com.kafkademo.order.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@ActiveProfiles("test")
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventProducer orderEventProducer;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository, orderEventProducer);
        // save() returns the same order passed to it
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void createOrder_shouldReturnOrderWithPendingStatusAndPublishEvent() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-001");
        request.setProductId("product-001");
        request.setQuantity(3);
        request.setTotalAmount(new BigDecimal("75.00"));

        // Act
        Order order = orderService.createOrder(request);

        // Assert: order is created correctly
        assertThat(order).isNotNull();
        assertThat(order.getOrderId()).isNotNull().isNotEmpty();
        assertThat(order.getCustomerId()).isEqualTo("customer-001");
        assertThat(order.getProductId()).isEqualTo("product-001");
        assertThat(order.getQuantity()).isEqualTo(3);
        assertThat(order.getTotalAmount()).isEqualByComparingTo(new BigDecimal("75.00"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getPlacedAt()).isNotNull();

        // Assert: event was published
        verify(orderEventProducer, times(1)).publishOrderPlaced(any());
    }

    @Test
    void cancelOrder_shouldUpdateStatusToCancelledAndPublishEvent() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-002");
        request.setProductId("product-002");
        request.setQuantity(1);
        request.setTotalAmount(new BigDecimal("20.00"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // Act
        orderService.cancelOrder(orderId);

        // Assert: order status is CANCELLED
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);

        Optional<Order> cancelledOrder = orderService.getOrder(orderId);
        assertThat(cancelledOrder).isPresent();
        assertThat(cancelledOrder.get().getStatus()).isEqualTo(OrderStatus.CANCELLED);

        // Assert: cancel event was published
        verify(orderEventProducer, times(1)).publishOrderCancelled(any());
    }

    @Test
    void handlePaymentProcessed_withSuccess_shouldUpdateStatusToPaymentConfirmed() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-003");
        request.setProductId("product-003");
        request.setQuantity(1);
        request.setTotalAmount(new BigDecimal("50.00"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                orderId,
                "payment-001",
                "SUCCESS",
                null,
                LocalDateTime.now()
        );

        // Act
        orderService.handlePaymentProcessed(paymentEvent);

        // Assert: status is PAYMENT_CONFIRMED (stock not yet received)
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_CONFIRMED);

        Optional<Order> updatedOrder = orderService.getOrder(orderId);
        assertThat(updatedOrder).isPresent();
        assertThat(updatedOrder.get().getStatus()).isEqualTo(OrderStatus.PAYMENT_CONFIRMED);
    }

    @Test
    void handleStockReserved_afterPaymentConfirmed_shouldUpdateStatusToConfirmed() {
        // Arrange: create order
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-004");
        request.setProductId("product-004");
        request.setQuantity(2);
        request.setTotalAmount(new BigDecimal("100.00"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // Step 1: payment confirmed
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                orderId,
                "payment-002",
                "SUCCESS",
                null,
                LocalDateTime.now()
        );
        orderService.handlePaymentProcessed(paymentEvent);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_CONFIRMED);

        Optional<Order> afterPayment = orderService.getOrder(orderId);
        assertThat(afterPayment).isPresent();
        assertThat(afterPayment.get().getStatus()).isEqualTo(OrderStatus.PAYMENT_CONFIRMED);

        // Step 2: stock reserved
        StockReservedEvent stockEvent = new StockReservedEvent(
                orderId,
                "product-004",
                "RESERVED",
                10,
                LocalDateTime.now()
        );
        orderService.handleStockReserved(stockEvent);

        // Assert: now both confirmed → status should be CONFIRMED
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);

        Optional<Order> fullyConfirmedOrder = orderService.getOrder(orderId);
        assertThat(fullyConfirmedOrder).isPresent();
        assertThat(fullyConfirmedOrder.get().getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void handlePaymentProcessed_withFailure_shouldUpdateStatusToPaymentFailed() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-005");
        request.setProductId("product-005");
        request.setQuantity(1);
        request.setTotalAmount(new BigDecimal("200.00"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                orderId,
                "payment-003",
                "FAILED",
                "Card declined",
                LocalDateTime.now()
        );

        // Act
        orderService.handlePaymentProcessed(paymentEvent);

        // Assert
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_FAILED);

        Optional<Order> updatedOrder = orderService.getOrder(orderId);
        assertThat(updatedOrder).isPresent();
        assertThat(updatedOrder.get().getStatus()).isEqualTo(OrderStatus.PAYMENT_FAILED);
    }

    @Test
    void handleStockReserved_withInsufficient_shouldUpdateStatusToStockInsufficient() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("customer-006");
        request.setProductId("product-006");
        request.setQuantity(100);
        request.setTotalAmount(new BigDecimal("500.00"));

        Order order = orderService.createOrder(request);
        String orderId = order.getOrderId();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        StockReservedEvent stockEvent = new StockReservedEvent(
                orderId,
                "product-006",
                "INSUFFICIENT",
                5,
                LocalDateTime.now()
        );

        // Act
        orderService.handleStockReserved(stockEvent);

        // Assert
        assertThat(order.getStatus()).isEqualTo(OrderStatus.STOCK_INSUFFICIENT);

        Optional<Order> updatedOrder = orderService.getOrder(orderId);
        assertThat(updatedOrder).isPresent();
        assertThat(updatedOrder.get().getStatus()).isEqualTo(OrderStatus.STOCK_INSUFFICIENT);
    }

    @Test
    void getOrder_withUnknownId_shouldReturnEmpty() {
        when(orderRepository.findById("non-existent-order-id")).thenReturn(Optional.empty());
        Optional<Order> result = orderService.getOrder("non-existent-order-id");
        assertThat(result).isEmpty();
    }
}
