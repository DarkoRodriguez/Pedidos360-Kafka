package pedidos360.analytics.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import pedidos360.analytics.dto.AnalyticsSummaryDto;
import pedidos360.analytics.dto.ApiResponseDto;
import pedidos360.analytics.dto.LiveEventDto;
import pedidos360.analytics.dto.PedidoVentaDto;
import pedidos360.analytics.dto.ProductDemandDto;
import pedidos360.analytics.dto.StoreMetricDto;
import pedidos360.analytics.service.RealtimeAnalyticsService;

@RestController
@RequestMapping("/api/analytics")
@CrossOrigin(origins = "*")
public class RealtimeAnalyticsController {

    private final RealtimeAnalyticsService analyticsService;

    public RealtimeAnalyticsController(RealtimeAnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @GetMapping("/realtime")
    public ResponseEntity<ApiResponseDto<AnalyticsSummaryDto>> getSummary() {
        return ResponseEntity.ok(ApiResponseDto.ok("Resumen en tiempo real recuperado con éxito", analyticsService.getSummary()));
    }

    @GetMapping("/stores")
    public ResponseEntity<ApiResponseDto<List<StoreMetricDto>>> getStores() {
        return ResponseEntity.ok(ApiResponseDto.ok("Métricas de los 20 locales recuperadas con éxito", analyticsService.getAllStores()));
    }

    @GetMapping("/products")
    public ResponseEntity<ApiResponseDto<List<ProductDemandDto>>> getProducts() {
        return ResponseEntity.ok(ApiResponseDto.ok("Comportamiento de productos más demandados", analyticsService.getTopProducts()));
    }

    @GetMapping("/events")
    public ResponseEntity<ApiResponseDto<List<LiveEventDto>>> getEvents() {
        return ResponseEntity.ok(ApiResponseDto.ok("Flujo continuo de auditoría y trazabilidad inalterable", analyticsService.getLiveEvents()));
    }

    @PostMapping("/simulate-sale")
    public ResponseEntity<ApiResponseDto<PedidoVentaDto>> simulateSale(@RequestBody(required = false) Map<String, Object> body) {
        String storeId = body != null && body.containsKey("storeId") ? body.get("storeId").toString() : "LOCAL-01";
        String product = body != null && body.containsKey("product") ? body.get("product").toString() : "Croissant de Almendra";
        Integer quantity = body != null && body.containsKey("quantity") ? Integer.parseInt(body.get("quantity").toString()) : 2;
        Double price = body != null && body.containsKey("price") ? Double.parseDouble(body.get("price").toString()) : 8900.0;

        PedidoVentaDto dto = analyticsService.simularVentaEnVivo(storeId, product, quantity, price);
        return ResponseEntity.ok(ApiResponseDto.ok("Venta simulada y emitida con éxito a Kafka (pedidos-ventas)", dto));
    }
}
