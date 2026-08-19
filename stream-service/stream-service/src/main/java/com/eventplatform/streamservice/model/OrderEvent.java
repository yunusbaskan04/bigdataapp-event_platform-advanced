package com.eventplatform.streamservice.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    @JsonProperty("orderId")
    @JsonAlias({"id", "order_id", "aggregate_id"})
    private Long orderId;

    private String product;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String product) {
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

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", product='" + product + '\'' +
                '}';
    }
}
