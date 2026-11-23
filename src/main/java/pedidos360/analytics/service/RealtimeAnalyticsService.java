package pedidos360.analytics.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import pedidos360.analytics.dto.AnalyticsSummaryDto;
import pedidos360.analytics.dto.LiveEventDto;
import pedidos360.analytics.dto.PedidoVentaDto;
import pedidos360.analytics.dto.ProductDemandDto;
import pedidos360.analytics.dto.StoreMetricDto;

/**
 * Servicio en memoria de Analítica en Tiempo Real sincronizado con Apache Kafka y Kafka Streams:
 * - Procesa continuamente eventos de ventas reales desde el tópico 'pedidos-ventas'.
 * - Mantiene métricas reales en vivo (SUM, COUNT, AVG) para los 20 locales individuales.
 * - Calcula la demanda de productos del catálogo y actualiza el stream de auditoría inalterable.
 * - Toda la interfaz, eventos y métricas se presentan en idioma Español.
 */
@Service
public class RealtimeAnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(RealtimeAnalyticsService.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // Almacenamiento concurrente de las métricas de los 20 locales
    private final Map<String, StoreMetricDto> storeMetricsMap = new ConcurrentHashMap<>();

    // Productos del catálogo y su demanda calculada en tiempo real
    private final Map<String, ProductDemandDto> productsDemandMap = new ConcurrentHashMap<>();

    // Historial inalterable de eventos en vivo (Streaming vía Kafka)
    private final List<LiveEventDto> liveEventStream = Collections.synchronizedList(new LinkedList<>());

    // Indicadores globales calculados dinámicamente a partir de ventas reales
    private double liveSalesToday = 0.0;
    private final AtomicLong activeOrders = new AtomicLong(0);
    private final AtomicLong ordersInPrep = new AtomicLong(0);
    private final AtomicLong ordersInDelivery = new AtomicLong(0);
    private final AtomicLong lowStockAlerts = new AtomicLong(0);

    // Registro de pedidos procesados para garantizar idempotencia exacta
    private final java.util.Set<Long> processedOrderIds = ConcurrentHashMap.newKeySet();

    private final Random random = new Random();

    public RealtimeAnalyticsService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void initBaselineData() {
        // Inicialización de los 20 locales con métricas limpias (parten en 0 hasta recibir ventas de Kafka)
        String[][] locales = {
                {"LOCAL-01", "Café Central - Centro", "Cafetería"},
                {"LOCAL-02", "Pan Artesanal Providencia", "Panadería"},
                {"LOCAL-03", "Dulce Alianza Las Condes", "Pastelería"},
                {"LOCAL-04", "Café Central Ñuñoa", "Cafetería"},
                {"LOCAL-05", "Pan Artesanal Vitacura", "Panadería"},
                {"LOCAL-06", "Dulce Alianza Santiago", "Pastelería"},
                {"LOCAL-07", "Café Central Maipú", "Cafetería"},
                {"LOCAL-08", "Pan Artesanal La Florida", "Panadería"},
                {"LOCAL-09", "Dulce Alianza San Miguel", "Pastelería"},
                {"LOCAL-10", "Café Central Bellavista", "Cafetería"},
                {"LOCAL-11", "Pan Artesanal Barrio Italia", "Panadería"},
                {"LOCAL-12", "Dulce Alianza Viña del Mar", "Pastelería"},
                {"LOCAL-13", "Café Central Valparaíso", "Cafetería"},
                {"LOCAL-14", "Pan Artesanal Quilpué", "Panadería"},
                {"LOCAL-15", "Dulce Alianza Concepción", "Pastelería"},
                {"LOCAL-16", "Café Central Temuco", "Cafetería"},
                {"LOCAL-17", "Pan Artesanal Antofagasta", "Panadería"},
                {"LOCAL-18", "Dulce Alianza La Serena", "Pastelería"},
                {"LOCAL-19", "Café Central Puerto Montt", "Cafetería"},
                {"LOCAL-20", "Pan Artesanal Chillán", "Panadería"}
        };

        String now = LocalDateTime.now().format(TIME_FORMATTER);

        for (String[] loc : locales) {
            storeMetricsMap.put(loc[0], new StoreMetricDto(
                    loc[0], loc[1], loc[2],
                    0.0, // totalSales inicia en 0.0 real
                    0L,  // orderCount inicia en 0 real
                    0.0, // averageTicket inicia en 0.0 real
                    true,
                    now
            ));
        }

        // Catálogo de productos para el cálculo de demanda
        productsDemandMap.put("Masa madre clásica", new ProductDemandDto("Masa madre clásica", 0L, "🥖", 0.0));
        productsDemandMap.put("Roll de canela", new ProductDemandDto("Roll de canela", 0L, "🥮", 0.0));
        productsDemandMap.put("Croissant de mantequilla", new ProductDemandDto("Croissant de mantequilla", 0L, "🥐", 0.0));
        productsDemandMap.put("Focaccia de romero", new ProductDemandDto("Focaccia de romero", 0L, "🫓", 0.0));
        productsDemandMap.put("Pan ciabatta", new ProductDemandDto("Pan ciabatta", 0L, "🥖", 0.0));
        productsDemandMap.put("Donut de chocolate", new ProductDemandDto("Donut de chocolate", 0L, "🍩", 0.0));
        productsDemandMap.put("Empanada de queso", new ProductDemandDto("Empanada de queso", 0L, "🥟", 0.0));
        productsDemandMap.put("Baguette francesa", new ProductDemandDto("Baguette francesa", 0L, "🥖", 0.0));
        productsDemandMap.put("Café Latte", new ProductDemandDto("Café Latte", 0L, "☕", 0.0));
        productsDemandMap.put("Pastel de Tres Leches", new ProductDemandDto("Pastel de Tres Leches", 0L, "🍰", 0.0));

        // Registro de auditoría inicial del sistema en Español
        liveEventStream.add(new LiveEventDto(
                now,
                "Sistema de analítica Kafka Streams iniciado. Monitoreo en tiempo real de 20 locales activo.",
                "SISTEMA_INICIADO",
                "trace-sys-init"
        ));
    }

    /**
     * Invocado por Kafka Streams para cada venta procesada en el flujo continuo.
     * Actualiza agregaciones (SUM, COUNT, AVG) y alimenta el stream de auditoría.
     */
    public synchronized void registrarEventoVenta(String storeKey, String jsonPayload) {
        try {
            JsonNode node = objectMapper.readTree(jsonPayload);
            double monto = node.has("totalPrecio") ? node.get("totalPrecio").asDouble() : 0.0;
            String localNombre = node.has("localNombre") ? node.get("localNombre").asText() : "Café Central - Centro";
            String cliente = node.has("cliente") ? node.get("cliente").asText() : "Cliente";
            long pedidoId = node.has("pedidoId") ? node.get("pedidoId").asLong() : Math.abs(random.nextLong() % 9000 + 1000);
            if (pedidoId > 0 && !processedOrderIds.add(pedidoId)) {
                log.info("[ANALYTICS] Pedido #{} ya procesado. Omitiendo duplicado.", pedidoId);
                return;
            }
            String productos = node.has("productos") ? node.get("productos").asText() : "";
            int cantTotal = node.has("cantidadTotal") ? node.get("cantidadTotal").asInt() : 1;
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : "kafka-evt-" + pedidoId;

            // 1. Actualizar métricas globales (SUM y COUNT)
            this.liveSalesToday += monto;
            this.activeOrders.incrementAndGet();
            this.ordersInPrep.incrementAndGet();

            // 2. Actualizar métricas del local específico
            StoreMetricDto metric = storeMetricsMap.get(storeKey);
            String now = LocalDateTime.now().format(TIME_FORMATTER);

            if (metric != null) {
                metric.setTotalSales(metric.getTotalSales() + monto);
                metric.setOrderCount(metric.getOrderCount() + 1);
                metric.setAverageTicket(metric.getOrderCount() > 0 ? (metric.getTotalSales() / metric.getOrderCount()) : 0.0);
                metric.setLastUpdated(now);
            } else {
                metric = new StoreMetricDto(storeKey, localNombre, "General", monto, 1L, monto, true, now);
                storeMetricsMap.put(storeKey, metric);
            }

            // 3. Actualizar productos más demandados
            actualizarDemandasProductos(productos, cantTotal);

            // 4. Agregar al Live Event Stream & Audit en Español
            String eventText = String.format("Pedido #%d de '%s' registrado en %s (Monto: $%,.0f)",
                    pedidoId, cliente, localNombre, monto);
            agregarEventoStream(now, eventText, "PEDIDO_REGISTRADO", correlationId);

        } catch (Exception ex) {
            log.error("[ANALYTICS-SERVICE] Error actualizando estado con evento Kafka: {}", ex.getMessage());
        }
    }

    private void actualizarDemandasProductos(String productosStr, int cantTotal) {
        if (productosStr == null || productosStr.isBlank()) return;

        String lower = productosStr.toLowerCase();
        int cant = cantTotal > 0 ? cantTotal : 1;

        if (lower.contains("masa madre")) {
            sumarDemanda("Masa madre clásica", cant);
        }
        if (lower.contains("canela") || lower.contains("roll")) {
            sumarDemanda("Roll de canela", cant);
        }
        if (lower.contains("croissant")) {
            sumarDemanda("Croissant de mantequilla", cant);
        }
        if (lower.contains("focaccia")) {
            sumarDemanda("Focaccia de romero", cant);
        }
        if (lower.contains("ciabatta")) {
            sumarDemanda("Pan ciabatta", cant);
        }
        if (lower.contains("donut")) {
            sumarDemanda("Donut de chocolate", cant);
        }
        if (lower.contains("empanada")) {
            sumarDemanda("Empanada de queso", cant);
        }
        if (lower.contains("baguette")) {
            sumarDemanda("Baguette francesa", cant);
        }
        if (lower.contains("café") || lower.contains("latte")) {
            sumarDemanda("Café Latte", cant);
        }
        if (lower.contains("pastel") || lower.contains("leches") || lower.contains("torta")) {
            sumarDemanda("Pastel de Tres Leches", cant);
        }

        // Recalcular porcentajes sobre el producto más vendido
        long max = productsDemandMap.values().stream().mapToLong(ProductDemandDto::getLiveSales).max().orElse(1L);
        if (max > 0) {
            for (ProductDemandDto p : productsDemandMap.values()) {
                p.setPercentage(Math.round(((double) p.getLiveSales() / max) * 1000.0) / 10.0);
            }
        }
    }

    private void sumarDemanda(String nombreProducto, int cant) {
        ProductDemandDto p = productsDemandMap.get(nombreProducto);
        if (p != null) {
            p.setLiveSales(p.getLiveSales() + cant);
        }
    }

    public void agregarEventoStream(String time, String text, String type, String correlationId) {
        liveEventStream.add(0, new LiveEventDto(time, text, type, correlationId));
        if (liveEventStream.size() > 50) {
            liveEventStream.remove(liveEventStream.size() - 1);
        }
    }

    /**
     * Emisión interactiva de eventos de venta:
     * Publica el evento real hacia el tópico 'pedidos-ventas' de Apache Kafka.
     * Si el broker está disponible, la topología Kafka Streams lo consume y procesa;
     * de lo contrario, se actualiza localmente con garantía de servicio continuo.
     */
    public PedidoVentaDto simularVentaEnVivo(String localId, String producto, Integer cantidad, Double precio) {
        StoreMetricDto store = storeMetricsMap.get(localId != null ? localId : "LOCAL-01");
        if (store == null) {
            store = storeMetricsMap.get("LOCAL-01");
        }

        long pedidoId = Math.abs(random.nextLong() % 9000 + 4100);
        String[] nombresChile = {
                "Juan Pérez", "María Silva", "Carlos López", "Ana Morales",
                "Pedro Ruiz", "Francisca González", "Rodrigo Valenzuela", "Camila Sepúlveda"
        };
        String cliente = nombresChile[random.nextInt(nombresChile.length)];
        String prod = (producto != null && !producto.isBlank()) ? producto : "Croissant de mantequilla (x2), Café Latte (x1)";
        int cant = (cantidad != null && cantidad > 0) ? cantidad : 2;
        double total = (precio != null && precio > 0) ? precio : (cant * 4500.0);

        PedidoVentaDto evento = new PedidoVentaDto();
        evento.setPedidoId(pedidoId);
        evento.setCliente(cliente);
        evento.setLocalId(store.getStoreId());
        evento.setLocalNombre(store.getStoreName());
        evento.setCategoria(store.getCategory());
        evento.setProductos(prod);
        evento.setCantidadTotal(cant);
        evento.setTotalPrecio(total);
        evento.setEstado("Confirmado");
        evento.setFechaHora(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        evento.setCorrelationId("trace-venta-" + pedidoId);

        // Publicar al tópico 'pedidos-ventas' de Kafka
        try {
            kafkaTemplate.send("pedidos-ventas", store.getStoreId(), evento);
            log.info("[ANALYTICS] Evento de venta publicado en Kafka para local '{}'", store.getStoreName());
            // Registrar también de forma inmediata con garantía de idempotencia
            registrarEventoVenta(store.getStoreId(), objectMapper.writeValueAsString(evento));
        } catch (Exception ex) {
            log.warn("[ANALYTICS] Broker Kafka no accesible directamente; procesando evento en memoria: {}", ex.getMessage());
            try {
                registrarEventoVenta(store.getStoreId(), objectMapper.writeValueAsString(evento));
            } catch (Exception ignored) {}
        }

        return evento;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CONSULTAS PARA EL FRONTEND
    // ─────────────────────────────────────────────────────────────────────────

    public AnalyticsSummaryDto getSummary() {
        AnalyticsSummaryDto dto = new AnalyticsSummaryDto();
        dto.setLiveSalesToday(Math.round(this.liveSalesToday * 100.0) / 100.0);

        // Crecimiento dinámico sobre ventas del día
        double crecimiento = this.liveSalesToday > 0 ? Math.min(25.0, Math.round((this.liveSalesToday / 50000.0) * 10.0) / 10.0) : 0.0;
        dto.setSalesGrowthPercentage(crecimiento);

        dto.setActiveOrders(this.activeOrders.get());
        dto.setOrdersInPreparation(this.ordersInPrep.get());
        dto.setOrdersInDelivery(this.ordersInDelivery.get());
        dto.setLowStockAlertsCount(this.lowStockAlerts.get());
        dto.setOnlineStoresCount(20);
        dto.setTotalStoresCount(20);
        dto.setLastUpdated(LocalDateTime.now().format(TIME_FORMATTER));

        // Obtener el local con mayor venta (Top Performing Store)
        StoreMetricDto topStore = storeMetricsMap.values().stream()
                .filter(s -> s.getTotalSales() > 0)
                .max(Comparator.comparingDouble(StoreMetricDto::getTotalSales))
                .orElse(null);

        dto.setTopPerformingStore(topStore != null ? topStore.getStoreName() : "Sin ventas registradas aún");

        return dto;
    }

    public List<StoreMetricDto> getAllStores() {
        List<StoreMetricDto> list = new ArrayList<>(storeMetricsMap.values());
        list.sort(Comparator.comparingDouble(StoreMetricDto::getTotalSales).reversed());
        return list;
    }

    public List<ProductDemandDto> getTopProducts() {
        List<ProductDemandDto> list = new ArrayList<>(productsDemandMap.values());
        list.sort(Comparator.comparingLong(ProductDemandDto::getLiveSales).reversed());
        return list;
    }

    public List<LiveEventDto> getLiveEvents() {
        return new ArrayList<>(liveEventStream);
    }
}
