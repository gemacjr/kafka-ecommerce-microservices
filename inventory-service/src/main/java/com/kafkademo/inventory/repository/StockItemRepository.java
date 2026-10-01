package com.kafkademo.inventory.repository;

import com.kafkademo.inventory.model.StockItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StockItemRepository extends JpaRepository<StockItem, String> {
}
