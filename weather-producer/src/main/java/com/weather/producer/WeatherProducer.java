package com.weather.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class WeatherProducer {
    private static final Logger logger = LoggerFactory.getLogger(WeatherProducer.class); // логгер
    private static final String BOOTSTRAP_SERVERS = "kafka:9092"; // хост кафки
    private static final String TOPIC = "weather-topic"; // топик кафки

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = getStringStringKafkaProducer();

        Random random = new Random();

        // тут должно было быть чето прикольное, но гпт не умеет в шутки(
        HashSet<String> weatherPhrases = getWeatherStrings();

        while (true) {
            String temperature = String.format("%.2f", -20 + random.nextFloat() * 60); // Температура от -20 до 40 градусов
            String humidity = String.format("%.2f", random.nextFloat() * 100); // Влажность от 0 до 100%
            String windSpeed = String.format("%.2f", random.nextFloat() * 20); // Скорость ветра от 0 до 20 м/с

            String randomPhrase = (String) weatherPhrases.toArray()[random.nextInt(weatherPhrases.size())];

            String message = String.format("{" +
                            "\"temperature\": \"%s\", " +
                            "\"humidity\": \"%s\", " +
                            "\"windSpeed\": \"%s\", " +
                            "\"weatherPhrase\": \"%s\"}",
                    temperature, humidity, windSpeed, randomPhrase);

            // сообщение в топик кафки
            producer.send(new ProducerRecord<>(TOPIC, "weather", message));

            String currentTime = getCurrentTime();

            //System.out.println(String.format("{\"%s\"} - Sent weather data: {\"%s\"}", currentTime, message));
            logger.info("{} - Sent weather data: {}", currentTime, message);

            // каждые 5 сек
            TimeUnit.SECONDS.sleep(5);
        }
    }

    private static HashSet<String> getWeatherStrings() {
        HashSet<String> weatherPhrases = new HashSet<>();
        weatherPhrases.add("Сегодня солнечно, не забудьте крем от загара!");
        weatherPhrases.add("Ожидаются дожди, берите зонт!");
        weatherPhrases.add("Ветрено! Будьте осторожны на улице.");
        weatherPhrases.add("Температура стремительно падает. Надевайте теплую одежду.");
        weatherPhrases.add("Погода без изменений, а настроение в шоке.");
        weatherPhrases.add("Жарко, как в аду, но без удовольствия.");
        weatherPhrases.add("Погода не такая уж плохая... если не смотреть на нее.");
        return weatherPhrases;
    }

    private static KafkaProducer<String, String> getStringStringKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private static String getCurrentTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
        return LocalDateTime.now().format(formatter);
    }
}
