package com.eventplatform.consumerservice.entity;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "orders")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {

    @Id
    @JsonProperty("orderId")
    @JsonAlias({"id", "order_id", "aggregate_id", "aggregateId"})
    private Long orderId;

    private String product;

    public Order() {
    }

    public Order(Long orderId, String product) {
        this.orderId = orderId;
        this.product = product;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }
}