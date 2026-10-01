package com.kafkademo.payment.service;

import com.kafkademo.payment.exception.PaymentProcessingException;
import com.kafkademo.payment.model.Payment;
import com.kafkademo.payment.model.PaymentStatus;
import com.kafkademo.payment.model.OrderPlacedEvent;
import com.kafkademo.payment.model.PaymentProcessedEvent;
import com.kafkademo.payment.producer.PaymentEventProducer;
import com.kafkademo.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PaymentProcessingService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessingService.class);

    private final PaymentRepository paymentRepository;
    private final PaymentEventProducer paymentEventProducer;

    // In-memory test hook only — not domain state
    private final Set<String> forceFail = ConcurrentHashMap.newKeySet();

    public PaymentProcessingService(PaymentRepository paymentRepository, PaymentEventProducer paymentEventProducer) {
        this.paymentRepository = paymentRepository;
        this.paymentEventProducer = paymentEventProducer;
    }

    @Transactional
    public void processPayment(OrderPlacedEvent event) {
        String orderId = event.getOrderId();

        // Idempotency check via DB — prevents double-processing on consumer rebalance
        if (paymentRepository.existsByOrderId(orderId)) {
            log.warn("Duplicate OrderPlacedEvent detected for orderId={}. Skipping.", orderId);
            return;
        }

        // Force-fail check (test hook)
        if (forceFail.contains(orderId)) {
            forceFail.remove(orderId);
            Payment failed = new Payment(UUID.randomUUID().toString(), orderId,
                    PaymentStatus.FAILED, "Forced failure via test hook", LocalDateTime.now());
            paymentRepository.save(failed);
            PaymentProcessedEvent result = new PaymentProcessedEvent(orderId, failed.getPaymentId(),
                    "FAILED", failed.getFailureReason(), failed.getProcessedAt());
            paymentEventProducer.publishPaymentProcessed(result);
            throw new PaymentProcessingException("Payment force-failed for orderId=" + orderId);
        }

        // Simulate 10% random failure
        if (Math.random() < 0.1) {
            String reason = "Random payment decline";
            Payment failed = new Payment(UUID.randomUUID().toString(), orderId,
                    PaymentStatus.FAILED, reason, LocalDateTime.now());
            paymentRepository.save(failed);
            PaymentProcessedEvent result = new PaymentProcessedEvent(orderId, failed.getPaymentId(),
                    "FAILED", reason, failed.getProcessedAt());
            paymentEventProducer.publishPaymentProcessed(result);
            throw new PaymentProcessingException(reason);
        }

        // Success path
        Payment payment = new Payment(UUID.randomUUID().toString(), orderId,
                PaymentStatus.SUCCESS, null, LocalDateTime.now());
        paymentRepository.save(payment);
        PaymentProcessedEvent result = new PaymentProcessedEvent(orderId, payment.getPaymentId(),
                "SUCCESS", null, payment.getProcessedAt());
        paymentEventProducer.publishPaymentProcessed(result);
        log.info("Payment processed successfully: orderId={}, paymentId={}", orderId, payment.getPaymentId());
    }

    public void forceFail(String orderId) {
        forceFail.add(orderId);
        log.info("Registered force-fail for orderId={}", orderId);
    }

    public Optional<Payment> getPayment(String orderId) {
        return paymentRepository.findAll().stream()
                .filter(p -> p.getOrderId().equals(orderId))
                .findFirst();
    }
}
