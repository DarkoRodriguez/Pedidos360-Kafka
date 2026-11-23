# Etapa 1: Compilación con Maven y Java 21
FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

# Etapa 2: Imagen liviana de ejecución (Optimizado para instancia t3.small)
FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache libstdc++ gcompat
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar

EXPOSE 8083

# Heap optimizado para instancias pequeñas (t3.small)
ENV JAVA_OPTS="-Xmx384m -Xms128m -XX:+UseG1GC"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
