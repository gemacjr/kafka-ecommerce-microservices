package com.kafkademo.inventory.service;

import com.kafkademo.inventory.model.OrderCancelledEvent;
import com.kafkademo.inventory.model.OrderPlacedEvent;
import com.kafkademo.inventory.model.Reservation;
import com.kafkademo.inventory.model.StockItem;
import com.kafkademo.inventory.model.StockReservedEvent;
import com.kafkademo.inventory.producer.InventoryEventProducer;
import com.kafkademo.inventory.repository.ReservationRepository;
import com.kafkademo.inventory.repository.StockItemRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class InventoryManagementService {

    private static final Logger log = LoggerFactory.getLogger(InventoryManagementService.class);

    private final StockItemRepository stockItemRepository;
    private final ReservationRepository reservationRepository;
    private final InventoryEventProducer inventoryEventProducer;

    public InventoryManagementService(StockItemRepository stockItemRepository,
                                      ReservationRepository reservationRepository,
                                      InventoryEventProducer inventoryEventProducer) {
        this.stockItemRepository = stockItemRepository;
        this.reservationRepository = reservationRepository;
        this.inventoryEventProducer = inventoryEventProducer;
    }

    @Transactional
    public void reserveStock(OrderPlacedEvent event) {
        String orderId = event.getOrderId();
        String productId = event.getProductId();
        int quantity = event.getQuantity();

        // Idempotency: skip if already processed
        if (reservationRepository.existsById(orderId)) {
            log.warn("Duplicate OrderPlacedEvent for orderId={}. Skipping.", orderId);
            return;
        }

        Optional<StockItem> stockItemOpt = stockItemRepository.findById(productId);
        int currentStock = stockItemOpt.map(StockItem::getAvailableStock).orElse(0);

        StockReservedEvent result;
        if (currentStock >= quantity) {
            // Reserve: decrement stock and save reservation
            StockItem stockItem = stockItemOpt.get();
            stockItem.setAvailableStock(currentStock - quantity);
            stockItemRepository.save(stockItem);
            reservationRepository.save(new Reservation(orderId, productId, quantity));

            result = new StockReservedEvent(orderId, productId, "RESERVED",
                    stockItem.getAvailableStock(), LocalDateTime.now());
            log.info("Stock reserved: orderId={}, productId={}, quantity={}, remaining={}",
                    orderId, productId, quantity, stockItem.getAvailableStock());
        } else {
            result = new StockReservedEvent(orderId, productId, "INSUFFICIENT",
                    currentStock, LocalDateTime.now());
            log.info("Insufficient stock: orderId={}, productId={}, requested={}, available={}",
                    orderId, productId, quantity, currentStock);
        }

        inventoryEventProducer.publishStockReserved(result);
    }

    @Transactional
    public void releaseStock(OrderCancelledEvent event) {
        String orderId = event.getOrderId();

        reservationRepository.findById(orderId).ifPresent(reservation -> {
            stockItemRepository.findById(reservation.getProductId()).ifPresent(stockItem -> {
                stockItem.setAvailableStock(stockItem.getAvailableStock() + reservation.getQuantity());
                stockItemRepository.save(stockItem);
                log.info("Stock released: orderId={}, productId={}, quantity={}",
                        orderId, reservation.getProductId(), reservation.getQuantity());
            });
            reservationRepository.delete(reservation);
        });
    }

    @Transactional
    public void restockProduct(String productId, int quantity) {
        StockItem item = stockItemRepository.findById(productId)
                .orElse(new StockItem(productId, 0));
        item.setAvailableStock(item.getAvailableStock() + quantity);
        stockItemRepository.save(item);
        log.info("Restocked: productId={}, addedQuantity={}, newStock={}", productId, quantity, item.getAvailableStock());
    }

    public int getStockLevel(String productId) {
        return stockItemRepository.findById(productId)
                .map(StockItem::getAvailableStock)
                .orElse(0);
    }
}
