package com.weather.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class WeatherConsumer {
    private static final Logger logger = LoggerFactory.getLogger(WeatherConsumer.class); // логгер
    private static final String BOOTSTRAP_SERVERS = "kafka:9092"; // хост кафки
    private static final String TOPIC = "weather-topic"; // топик кафки
    private static final String GROUP_ID = "weather-consumer-group";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = getStringStringKafkaConsumer();

        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            org.apache.kafka.clients.consumer.ConsumerRecords<String, String> records = consumer.poll(1000); // Полим 1000 миллисекунд

            for (ConsumerRecord<String, String> record : records) {
                String currentTime = getCurrentTime();

                //System.out.println(String.format("{\"%s\"} - Received weather data: {\"%s\"}", currentTime, record.value()));
                logger.info("{} - Received weather data: {}", currentTime, record.value());
            }
        }
    }

    private static KafkaConsumer<String, String> getStringStringKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

    private static String getCurrentTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
        return LocalDateTime.now().format(formatter);
    }
}
