package com.eventplatform.consumerservice.consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    @KafkaListener(topics = "orders")
    public void consume(ConsumerRecord<String, String> record,
                        Acknowledgment acknowledgment) {

        // İş mantığı
        System.out.println("------------");
        System.out.println("Message : " + record.value());
        System.out.println("Topic      : " + record.topic());
        System.out.println("Partition  : " + record.partition());
        System.out.println("Offset     : " + record.offset());
        System.out.println("Timestamp  : " + record.timestamp());
        System.out.println("Key        : " + record.key());


        int x = 10 / 0;
        // Mesaj başarıyla işlendi
        acknowledgment.acknowledge();
        System.out.println("Offset committed.");
    }

}