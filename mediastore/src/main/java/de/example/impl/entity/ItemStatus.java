package de.example.impl.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "item_status")
public class ItemStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "item_status_id")
    private Integer itemStatusId;

    @ManyToOne
    @JoinColumn(name = "asin", nullable = false)
    private Item item;

    @Column(name = "item_status", nullable = false, length = 20)
    private String status;

    @Column(name = "price", precision = 5, scale = 2, nullable = false)
    private BigDecimal price;

    @Column(name = "currency", length = 10, nullable = false)
    private String currency;

    // --- Konstruktoren ---
    public ItemStatus() {}

    public ItemStatus(Item item, String status, BigDecimal price, String currency) {
        this.item = item;
        this.status = status;
        this.price = price;
        this.currency = currency;
    }

    // --- Getter/Setter ---
    public Integer getItemStatusId() {
        return itemStatusId;
    }

    public void setItemStatusId(Integer itemStatusId) {
        this.itemStatusId = itemStatusId;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    @Override
    public String toString() {
        return "ItemStatus{" +
                "id=" + itemStatusId +
                ", item=" + (item != null ? item.getTitle() : null) +
                ", status='" + status + '\'' +
                ", price=" + price +
                ", currency='" + currency + '\'' +
                '}';
    }
}
