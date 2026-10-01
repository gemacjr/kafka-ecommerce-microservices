package com.kafkademo.inventory.repository;

import com.kafkademo.inventory.model.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ReservationRepository extends JpaRepository<Reservation, String> {
}
