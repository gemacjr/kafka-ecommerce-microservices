package com.kafkademo.order.service;

import com.kafkademo.order.model.*;
import com.kafkademo.order.producer.OrderEventProducer;
import com.kafkademo.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        String orderId = (request.getOrderId() != null && !request.getOrderId().isBlank())
                ? request.getOrderId()
                : UUID.randomUUID().toString();

        Order order = new Order(orderId, request.getCustomerId(), request.getProductId(),
                request.getQuantity(), request.getTotalAmount(), OrderStatus.PENDING, false, false, LocalDateTime.now());
        orderRepository.save(order);

        OrderPlacedEvent event = new OrderPlacedEvent(orderId, request.getCustomerId(),
                request.getProductId(), request.getQuantity(), request.getTotalAmount(), order.getPlacedAt());
        orderEventProducer.publishOrderPlaced(event);

        log.info("Order created: orderId={}, status=PENDING", orderId);
        return order;
    }

    public Optional<Order> getOrder(String orderId) {
        return orderRepository.findById(orderId);
    }

    @Transactional
    public void cancelOrder(String orderId) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.setStatus(OrderStatus.CANCELLED);
            orderRepository.save(order);
            orderEventProducer.publishOrderCancelled(new OrderCancelledEvent(orderId, LocalDateTime.now()));
            log.info("Order cancelled: orderId={}", orderId);
        });
    }

    @Transactional
    public void handlePaymentProcessed(PaymentProcessedEvent event) {
        orderRepository.findById(event.getOrderId()).ifPresent(order -> {
            if ("SUCCESS".equals(event.getStatus())) {
                order.setStatus(OrderStatus.PAYMENT_CONFIRMED);
                order.setPaymentConfirmed(true);
            } else {
                order.setStatus(OrderStatus.PAYMENT_FAILED);
            }
            checkAndConfirm(order);
            orderRepository.save(order);
            log.info("Payment processed: orderId={}, paymentStatus={}", event.getOrderId(), event.getStatus());
        });
    }

    @Transactional
    public void handleStockReserved(StockReservedEvent event) {
        orderRepository.findById(event.getOrderId()).ifPresent(order -> {
            if ("RESERVED".equals(event.getStatus())) {
                order.setStatus(OrderStatus.STOCK_CONFIRMED);
                order.setStockConfirmed(true);
            } else {
                order.setStatus(OrderStatus.STOCK_INSUFFICIENT);
            }
            checkAndConfirm(order);
            orderRepository.save(order);
            log.info("Stock reserved: orderId={}, stockStatus={}", event.getOrderId(), event.getStatus());
        });
    }

    private void checkAndConfirm(Order order) {
        if (order.isPaymentConfirmed() && order.isStockConfirmed()) {
            order.setStatus(OrderStatus.CONFIRMED);
            log.info("Order fully confirmed: orderId={}", order.getOrderId());
        }
    }
}
