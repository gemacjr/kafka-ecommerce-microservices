package com.kafkademo.payment.repository;

import com.kafkademo.payment.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, String> {
    boolean existsByOrderId(String orderId);
}
