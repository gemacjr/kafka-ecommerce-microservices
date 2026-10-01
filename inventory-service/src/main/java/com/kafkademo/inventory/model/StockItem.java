package com.kafkademo.inventory.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock_items")
public class StockItem {

    @Id
    @Column(name = "product_id")
    private String productId;

    @Column(name = "available_stock", nullable = false)
    private int availableStock;

    public StockItem() {
    }

    public StockItem(String productId, int availableStock) {
        this.productId = productId;
        this.availableStock = availableStock;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getAvailableStock() {
        return availableStock;
    }

    public void setAvailableStock(int availableStock) {
        this.availableStock = availableStock;
    }

    @Override
    public String toString() {
        return "StockItem{" +
                "productId='" + productId + '\'' +
                ", availableStock=" + availableStock +
                '}';
    }
}
