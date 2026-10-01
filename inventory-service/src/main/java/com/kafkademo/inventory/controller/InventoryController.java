package com.kafkademo.inventory.controller;

import com.kafkademo.inventory.service.InventoryManagementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/inventory")
@Tag(name = "Inventory", description = "Query stock levels and manually restock products. Stock reservations are driven by the orders.placed Kafka topic.")
public class InventoryController {

    private static final Logger log = LoggerFactory.getLogger(InventoryController.class);

    private final InventoryManagementService inventoryManagementService;

    public InventoryController(InventoryManagementService inventoryManagementService) {
        this.inventoryManagementService = inventoryManagementService;
    }

    @Operation(
        summary = "Get current stock level",
        description = "Returns the available stock for the given product. " +
                      "Stock is decremented automatically when an OrderPlacedEvent is consumed from Kafka. " +
                      "Initial stock: prod-1=100, prod-2=5 (low — triggers INSUFFICIENT), prod-3=50.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Stock level returned"),
        }
    )
    @GetMapping("/{productId}")
    public ResponseEntity<Map<String, Object>> getStockLevel(
            @Parameter(description = "Product ID", example = "prod-1")
            @PathVariable String productId) {
        int stock = inventoryManagementService.getStockLevel(productId);
        log.info("Stock level query for productId={}: stock={}", productId, stock);
        return ResponseEntity.ok(Map.of(
                "productId", productId,
                "stock", stock
        ));
    }

    @Operation(
        summary = "Restock a product",
        description = "Manually adds the given quantity to the product's available stock. " +
                      "Useful for resetting the demo after stock is depleted.",
        responses = {
            @ApiResponse(responseCode = "200", description = "Stock updated successfully"),
            @ApiResponse(responseCode = "404", description = "Product not found", content = @Content),
        }
    )
    @PostMapping("/{productId}/restock")
    public ResponseEntity<Map<String, Object>> restockProduct(
            @Parameter(description = "Product ID", example = "prod-2")
            @PathVariable String productId,
            @Parameter(description = "Number of units to add", example = "50")
            @RequestParam(defaultValue = "1") int quantity) {
        log.info("Restock request for productId={}, quantity={}", productId, quantity);
        inventoryManagementService.restockProduct(productId, quantity);
        int newStock = inventoryManagementService.getStockLevel(productId);
        return ResponseEntity.ok(Map.of(
                "productId", productId,
                "addedQuantity", quantity,
                "newStock", newStock,
                "message", "Stock updated successfully"
        ));
    }
}
