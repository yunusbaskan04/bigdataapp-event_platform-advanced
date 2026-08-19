package com.eventplatform.streamservice.topology;

import com.eventplatform.streamservice.model.OrderEvent;
import com.eventplatform.streamservice.model.ProcessedOrderEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class OrderStreamTopology {

    private final ObjectMapper objectMapper;

    public OrderStreamTopology(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // 1. Source KStream: "orders" konusunu dinle (Stateless Input)
        KStream<String, String> rawOrderStream = streamsBuilder.stream(
                "orders",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 2. Filter (Stateless): Boş veya parse edilemeyen siparişleri filtrele
        KStream<String, String> validOrdersStream = rawOrderStream
                .peek((key, value) -> System.out.println("[KStream Input] Key: " + key + ", Value: " + value))
                .filter((key, value) -> value != null && !value.trim().isEmpty());

        // 3. Map / Transform (Stateless): Gelen JSON verisini zenginleştir (Enrichment)
        KStream<String, String> processedOrdersStream = validOrdersStream.mapValues(value -> {
            OrderEvent order = parseOrderEvent(value);
            if (order == null) {
                return value;
            }

            String category = resolveCategory(order.getProduct());
            String timestamp = Instant.now().toString();

            ProcessedOrderEvent processedOrder = new ProcessedOrderEvent(
                    order.getOrderId(),
                    order.getProduct(),
                    category,
                    timestamp
            );

            try {
                return objectMapper.writeValueAsString(processedOrder);
            } catch (Exception e) {
                return value;
            }
        });

        // 4. Sink: Zenginleştirilmiş verileri "processed-orders" konusuna aktar
        processedOrdersStream.to("processed-orders", Produced.with(Serdes.String(), Serdes.String()));

        // 5. SelectKey & GroupBy & Aggregation (Stateful): Ürün bazında sipariş sayımı
        KStream<String, String> productKeyedStream = validOrdersStream.selectKey((key, value) -> {
            OrderEvent order = parseOrderEvent(value);
            return (order != null && order.getProduct() != null) ? order.getProduct() : "UNKNOWN";
        });

        // GroupBy + Count + RocksDB State Store
        KTable<String, Long> productOrderCounts = productKeyedStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("product-counts-store"));

        // 6. KTable'ı KStream'e dönüştürüp "product-counts" konusuna yaz
        productOrderCounts
                .toStream()
                .peek((product, count) -> System.out.println("[KTable Output] Product: " + product + " -> Total Orders: " + count))
                .mapValues(count -> "{\"totalOrders\": " + count + "}")
                .to("product-counts", Produced.with(Serdes.String(), Serdes.String()));
    }

    private OrderEvent parseOrderEvent(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return null;
        }
        try {
            JsonNode rootNode = objectMapper.readTree(rawValue);

            // Debezium CDC Envelope Unpacking: Check if JSON has a "payload" wrapper
            if (rootNode.has("payload")) {
                JsonNode payloadNode = rootNode.get("payload");
                if (payloadNode.isTextual()) {
                    // payload is a JSON string e.g. "{\"orderId\":600,\"product\":\"Gaming Chair\"}"
                    return objectMapper.readValue(payloadNode.asText(), OrderEvent.class);
                } else if (payloadNode.isObject()) {
                    // payload is a JSON object e.g. {"orderId":600,"product":"Gaming Chair"}
                    return objectMapper.treeToValue(payloadNode, OrderEvent.class);
                }
            }

            // Direct JSON
            return objectMapper.readValue(rawValue, OrderEvent.class);
        } catch (Exception e) {
            System.err.println("[KStream Parsing Warning] Could not parse OrderEvent: " + e.getMessage());
            return null;
        }
    }

    private String resolveCategory(String product) {
        if (product == null) return "GENERAL";
        String lower = product.toLowerCase();
        if (lower.contains("phone") || lower.contains("laptop") || lower.contains("tv") || lower.contains("chair") || lower.contains("mouse") || lower.contains("keyboard") || lower.contains("monitor")) {
            return "ELECTRONICS";
        } else if (lower.contains("book")) {
            return "BOOKS";
        }
        return "GENERAL";
    }
}
