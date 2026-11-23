package pedidos360.analytics.dto;

import java.io.Serializable;

public class AnalyticsSummaryDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private Double liveSalesToday;         // SUM total global
    private Double salesGrowthPercentage;  // e.g. 14.0%
    private Long activeOrders;             // COUNT pedidos activos
    private Long ordersInPreparation;      // e.g. 48
    private Long ordersInDelivery;         // e.g. 32
    private Long lowStockAlertsCount;      // e.g. 3
    private String topPerformingStore;     // e.g. "Café Central - Centro"
    private Integer onlineStoresCount;     // e.g. 20
    private Integer totalStoresCount;      // 20
    private String lastUpdated;

    public AnalyticsSummaryDto() {}

    public Double getLiveSalesToday() { return liveSalesToday; }
    public void setLiveSalesToday(Double liveSalesToday) { this.liveSalesToday = liveSalesToday; }

    public Double getSalesGrowthPercentage() { return salesGrowthPercentage; }
    public void setSalesGrowthPercentage(Double salesGrowthPercentage) { this.salesGrowthPercentage = salesGrowthPercentage; }

    public Long getActiveOrders() { return activeOrders; }
    public void setActiveOrders(Long activeOrders) { this.activeOrders = activeOrders; }

    public Long getOrdersInPreparation() { return ordersInPreparation; }
    public void setOrdersInPreparation(Long ordersInPreparation) { this.ordersInPreparation = ordersInPreparation; }

    public Long getOrdersInDelivery() { return ordersInDelivery; }
    public void setOrdersInDelivery(Long ordersInDelivery) { this.ordersInDelivery = ordersInDelivery; }

    public Long getLowStockAlertsCount() { return lowStockAlertsCount; }
    public void setLowStockAlertsCount(Long lowStockAlertsCount) { this.lowStockAlertsCount = lowStockAlertsCount; }

    public String getTopPerformingStore() { return topPerformingStore; }
    public void setTopPerformingStore(String topPerformingStore) { this.topPerformingStore = topPerformingStore; }

    public Integer getOnlineStoresCount() { return onlineStoresCount; }
    public void setOnlineStoresCount(Integer onlineStoresCount) { this.onlineStoresCount = onlineStoresCount; }

    public Integer getTotalStoresCount() { return totalStoresCount; }
    public void setTotalStoresCount(Integer totalStoresCount) { this.totalStoresCount = totalStoresCount; }

    public String getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(String lastUpdated) { this.lastUpdated = lastUpdated; }
}
