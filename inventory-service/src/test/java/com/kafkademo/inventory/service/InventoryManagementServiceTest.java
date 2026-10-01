package com.kafkademo.inventory.service;

import com.kafkademo.inventory.model.OrderCancelledEvent;
import com.kafkademo.inventory.model.OrderPlacedEvent;
import com.kafkademo.inventory.model.Reservation;
import com.kafkademo.inventory.model.StockItem;
import com.kafkademo.inventory.model.StockReservedEvent;
import com.kafkademo.inventory.producer.InventoryEventProducer;
import com.kafkademo.inventory.repository.ReservationRepository;
import com.kafkademo.inventory.repository.StockItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryManagementServiceTest {

    @Mock
    private InventoryEventProducer inventoryEventProducer;

    @Mock
    private StockItemRepository stockItemRepository;

    @Mock
    private ReservationRepository reservationRepository;

    private InventoryManagementService inventoryManagementService;

    @BeforeEach
    void setUp() {
        inventoryManagementService = new InventoryManagementService(
                stockItemRepository,
                reservationRepository,
                inventoryEventProducer
        );
    }

    // -------------------------------------------------------------------------
    // reserveStock — sufficient stock
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("reserveStock: sufficient stock → produces RESERVED event and decrements stock")
    void whenReserveStock_withSufficientStock_thenPublishesReservedEvent() {
        // Arrange — prod-1 has 100 units in stock
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent event = new OrderPlacedEvent(
                orderId, "customer-1", "prod-1", 5,
                new BigDecimal("49.99"), LocalDateTime.now()
        );

        when(reservationRepository.existsById(orderId)).thenReturn(false);
        when(stockItemRepository.findById("prod-1")).thenReturn(Optional.of(new StockItem("prod-1", 100)));

        // Act
        inventoryManagementService.reserveStock(event);

        // Assert — producer was called once with a RESERVED event
        ArgumentCaptor<StockReservedEvent> captor = ArgumentCaptor.forClass(StockReservedEvent.class);
        verify(inventoryEventProducer, times(1)).publishStockReserved(captor.capture());

        StockReservedEvent published = captor.getValue();
        assertThat(published.getOrderId()).isEqualTo(orderId);
        assertThat(published.getProductId()).isEqualTo("prod-1");
        assertThat(published.getStatus()).isEqualTo("RESERVED");
        assertThat(published.getAvailableStock()).isEqualTo(95); // 100 - 5

        // Verify the updated stock item was saved
        ArgumentCaptor<StockItem> stockCaptor = ArgumentCaptor.forClass(StockItem.class);
        verify(stockItemRepository).save(stockCaptor.capture());
        assertThat(stockCaptor.getValue().getAvailableStock()).isEqualTo(95);

        // Verify reservation was saved
        ArgumentCaptor<Reservation> reservationCaptor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository).save(reservationCaptor.capture());
        assertThat(reservationCaptor.getValue().getOrderId()).isEqualTo(orderId);
        assertThat(reservationCaptor.getValue().getQuantity()).isEqualTo(5);
    }

    // -------------------------------------------------------------------------
    // reserveStock — insufficient stock
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("reserveStock: insufficient stock → produces INSUFFICIENT event, no decrement")
    void whenReserveStock_withInsufficientStock_thenPublishesInsufficientEvent() {
        // Arrange — prod-2 has only 5 units, we request 10
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent event = new OrderPlacedEvent(
                orderId, "customer-2", "prod-2", 10,
                new BigDecimal("99.99"), LocalDateTime.now()
        );

        when(reservationRepository.existsById(orderId)).thenReturn(false);
        when(stockItemRepository.findById("prod-2")).thenReturn(Optional.of(new StockItem("prod-2", 5)));

        // Act
        inventoryManagementService.reserveStock(event);

        // Assert — producer was called with an INSUFFICIENT event
        ArgumentCaptor<StockReservedEvent> captor = ArgumentCaptor.forClass(StockReservedEvent.class);
        verify(inventoryEventProducer, times(1)).publishStockReserved(captor.capture());

        StockReservedEvent published = captor.getValue();
        assertThat(published.getOrderId()).isEqualTo(orderId);
        assertThat(published.getProductId()).isEqualTo("prod-2");
        assertThat(published.getStatus()).isEqualTo("INSUFFICIENT");
        assertThat(published.getAvailableStock()).isEqualTo(5);

        // Stock should NOT be decremented — no save to stockItemRepository for stock update
        verify(stockItemRepository, never()).save(any(StockItem.class));
        // No reservation should be saved
        verify(reservationRepository, never()).save(any(Reservation.class));
    }

    // -------------------------------------------------------------------------
    // reserveStock — idempotency (duplicate event)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("reserveStock: same orderId twice → producer called only once (idempotency)")
    void whenReserveStock_calledTwiceWithSameOrderId_thenProducerCalledOnlyOnce() {
        // Arrange
        String orderId = UUID.randomUUID().toString();
        OrderPlacedEvent event = new OrderPlacedEvent(
                orderId, "customer-3", "prod-3", 2,
                new BigDecimal("19.99"), LocalDateTime.now()
        );

        // First call: not yet processed; second call: already processed
        when(reservationRepository.existsById(orderId))
                .thenReturn(false)
                .thenReturn(true);
        when(stockItemRepository.findById("prod-3")).thenReturn(Optional.of(new StockItem("prod-3", 50)));

        // Act — process the same event twice (simulates duplicate Kafka delivery)
        inventoryManagementService.reserveStock(event);
        inventoryManagementService.reserveStock(event); // duplicate

        // Assert — producer was called exactly ONCE despite two calls
        verify(inventoryEventProducer, times(1)).publishStockReserved(
                org.mockito.ArgumentMatchers.any(StockReservedEvent.class)
        );
        verifyNoMoreInteractions(inventoryEventProducer);
    }

    // -------------------------------------------------------------------------
    // releaseStock — restores stock on order cancellation
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("releaseStock: cancelling a reserved order restores stock")
    void whenReleaseStock_afterSuccessfulReservation_thenStockRestored() {
        // Arrange
        String orderId = UUID.randomUUID().toString();
        Reservation reservation = new Reservation(orderId, "prod-1", 10);
        StockItem stockItem = new StockItem("prod-1", 90); // stock already decremented after reservation

        when(reservationRepository.findById(orderId)).thenReturn(Optional.of(reservation));
        when(stockItemRepository.findById("prod-1")).thenReturn(Optional.of(stockItem));

        // Act
        OrderCancelledEvent cancelledEvent = new OrderCancelledEvent(orderId, LocalDateTime.now());
        inventoryManagementService.releaseStock(cancelledEvent);

        // Assert — stock should be restored (90 + 10 = 100)
        ArgumentCaptor<StockItem> stockCaptor = ArgumentCaptor.forClass(StockItem.class);
        verify(stockItemRepository).save(stockCaptor.capture());
        assertThat(stockCaptor.getValue().getAvailableStock()).isEqualTo(100);

        // Reservation should be deleted
        verify(reservationRepository).delete(reservation);
    }

    @Test
    @DisplayName("releaseStock: cancelling an order with no prior reservation does nothing")
    void whenReleaseStock_withNoReservation_thenStockUnchanged() {
        // Arrange — cancel an order that was never reserved
        String unknownOrderId = "non-existent-order-" + UUID.randomUUID();

        when(reservationRepository.findById(unknownOrderId)).thenReturn(Optional.empty());

        // Act
        OrderCancelledEvent cancelledEvent = new OrderCancelledEvent(unknownOrderId, LocalDateTime.now());
        inventoryManagementService.releaseStock(cancelledEvent);

        // Assert — no stock changes and no deletion
        verify(stockItemRepository, never()).save(any(StockItem.class));
        verify(reservationRepository, never()).delete(any(Reservation.class));
    }

    // -------------------------------------------------------------------------
    // restockProduct
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("restockProduct: adds quantity to existing product stock")
    void whenRestockProduct_thenStockIncreased() {
        // prod-2 starts at 5
        when(stockItemRepository.findById("prod-2")).thenReturn(Optional.of(new StockItem("prod-2", 5)));

        inventoryManagementService.restockProduct("prod-2", 20);

        ArgumentCaptor<StockItem> captor = ArgumentCaptor.forClass(StockItem.class);
        verify(stockItemRepository).save(captor.capture());
        assertThat(captor.getValue().getAvailableStock()).isEqualTo(25);
    }

    @Test
    @DisplayName("getStockLevel: returns 0 for unknown product")
    void whenGetStockLevel_forUnknownProduct_thenReturnsZero() {
        when(stockItemRepository.findById("unknown-product")).thenReturn(Optional.empty());
        assertThat(inventoryManagementService.getStockLevel("unknown-product")).isEqualTo(0);
    }
}
