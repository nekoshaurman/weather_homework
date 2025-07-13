FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/weather-consumer-1.0-SNAPSHOT.jar weather-consumer.jar

EXPOSE 8081

CMD ["java", "-jar", "weather-consumer.jar"]
