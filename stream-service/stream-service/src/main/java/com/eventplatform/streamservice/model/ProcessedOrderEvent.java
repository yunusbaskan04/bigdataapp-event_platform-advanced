package com.eventplatform.streamservice.model;

public class ProcessedOrderEvent {

    private Long orderId;
    private String product;
    private String category;
    private String processedAt;

    public ProcessedOrderEvent() {
    }

    public ProcessedOrderEvent(Long orderId, String product, String category, String processedAt) {
        this.orderId = orderId;
        this.product = product;
        this.category = category;
        this.processedAt = processedAt;
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(String processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public String toString() {
        return "ProcessedOrderEvent{" +
                "orderId=" + orderId +
                ", product='" + product + '\'' +
                ", category='" + category + '\'' +
                ", processedAt='" + processedAt + '\'' +
                '}';
    }
}
