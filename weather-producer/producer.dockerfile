FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/weather-producer-1.0-SNAPSHOT.jar weather-producer.jar

EXPOSE 8080

CMD ["java", "-jar", "weather-producer.jar"]
