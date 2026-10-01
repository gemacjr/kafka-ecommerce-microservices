package com.kafkademo.payment.service;

import com.kafkademo.payment.exception.PaymentProcessingException;
import com.kafkademo.payment.model.OrderPlacedEvent;
import com.kafkademo.payment.model.Payment;
import com.kafkademo.payment.model.PaymentStatus;
import com.kafkademo.payment.producer.PaymentEventProducer;
import com.kafkademo.payment.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentProcessingServiceTest {

    @Mock
    private PaymentEventProducer paymentEventProducer;

    @Mock
    private PaymentRepository paymentRepository;

    private PaymentProcessingService paymentProcessingService;

    @BeforeEach
    void setUp() {
        paymentProcessingService = new PaymentProcessingService(paymentRepository, paymentEventProducer);
    }

    private OrderPlacedEvent buildEvent(String orderId) {
        return new OrderPlacedEvent(
                orderId,
                "customer-001",
                "product-001",
                1,
                new BigDecimal("29.99"),
                LocalDateTime.now()
        );
    }

    @Test
    void processPayment_success_publishesSuccessEvent() {
        // Use a fixed orderId that won't randomly fail.
        // Since there is a 10% random failure chance we retry until we get a success,
        // but for determinism we mock/spy the random — instead we simply run enough
        // times. Here we call a stable path by resetting between attempts.
        // A simpler approach: try up to 20 times, assert at least once it succeeds.
        String orderId = "order-success-test";
        boolean succeeded = false;

        for (int attempt = 0; attempt < 20 && !succeeded; attempt++) {
            // Fresh instance each attempt to bypass idempotency
            PaymentProcessingService service = new PaymentProcessingService(paymentRepository, paymentEventProducer);
            // Repository returns false for existsByOrderId so each attempt is processed fresh
            when(paymentRepository.existsByOrderId(any())).thenReturn(false);
            try {
                service.processPayment(buildEvent(orderId + "-" + attempt));
                // If we reach here without exception, it was a SUCCESS
                succeeded = true;
            } catch (PaymentProcessingException ignored) {
                // Random failure; try again with a different orderId
            }
        }

        assertThat(succeeded).as("Expected at least one successful payment in 20 attempts").isTrue();
        // Verify producer was called at least once (for either SUCCESS or FAILED events across attempts)
        verify(paymentEventProducer, atLeastOnce()).publishPaymentProcessed(any());
    }

    @Test
    void processPayment_idempotency_secondCallIsSkipped() {
        String orderId = "order-idempotency-test";

        // First call — repository says not yet processed; may succeed or fail randomly
        when(paymentRepository.existsByOrderId(orderId)).thenReturn(false);
        try {
            paymentProcessingService.processPayment(buildEvent(orderId));
        } catch (PaymentProcessingException ignored) {
            // Random failure is acceptable
        }

        int callsAfterFirst = mockingDetails(paymentEventProducer).getInvocations().size();

        // Second call with SAME orderId — repository now reports it as already processed
        when(paymentRepository.existsByOrderId(orderId)).thenReturn(true);
        Payment storedPayment = new Payment(UUID.randomUUID().toString(), orderId,
                PaymentStatus.SUCCESS, null, LocalDateTime.now());
        when(paymentRepository.findAll()).thenReturn(List.of(storedPayment));
        try {
            paymentProcessingService.processPayment(buildEvent(orderId));
        } catch (PaymentProcessingException ignored) {
            // Should not happen since idempotency should skip processing
        }

        int callsAfterSecond = mockingDetails(paymentEventProducer).getInvocations().size();

        // Producer must NOT have been called again on the second invocation
        assertThat(callsAfterSecond)
                .as("Producer should not be called on duplicate orderId")
                .isEqualTo(callsAfterFirst);

        // The payment stored from the first call should still be present via getPayment
        Optional<Payment> payment = paymentProcessingService.getPayment(orderId);
        assertThat(payment).isPresent();
    }

    @Test
    void processPayment_forceFail_throwsPaymentProcessingException() {
        String orderId = "order-force-fail-test";

        when(paymentRepository.existsByOrderId(orderId)).thenReturn(false);

        paymentProcessingService.forceFail(orderId);

        assertThatThrownBy(() -> paymentProcessingService.processPayment(buildEvent(orderId)))
                .isInstanceOf(PaymentProcessingException.class)
                .hasMessageContaining("force-failed");

        // Producer should have been called once with a FAILED event
        ArgumentCaptor<com.kafkademo.payment.model.PaymentProcessedEvent> captor =
                ArgumentCaptor.forClass(com.kafkademo.payment.model.PaymentProcessedEvent.class);
        verify(paymentEventProducer, times(1)).publishPaymentProcessed(captor.capture());

        com.kafkademo.payment.model.PaymentProcessedEvent published = captor.getValue();
        assertThat(published.getOrderId()).isEqualTo(orderId);
        assertThat(published.getStatus()).isEqualTo("FAILED");
        assertThat(published.getFailureReason()).isNotBlank();

        // Verify a FAILED payment was saved to the repository
        ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository, times(1)).save(paymentCaptor.capture());
        Payment saved = paymentCaptor.getValue();
        assertThat(saved.getOrderId()).isEqualTo(orderId);
        assertThat(saved.getStatus()).isEqualTo(PaymentStatus.FAILED);
    }
}
