package pedidos360.analytics.dto;

import java.io.Serializable;

public class StoreMetricDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private String storeId;
    private String storeName;
    private String category;
    private Double totalSales;     // SUM de montos
    private Long orderCount;       // COUNT de pedidos
    private Double averageTicket;  // AVG (totalSales / orderCount)
    private boolean isOnline;
    private String lastUpdated;

    public StoreMetricDto() {}

    public StoreMetricDto(String storeId, String storeName, String category, Double totalSales, Long orderCount, Double averageTicket, boolean isOnline, String lastUpdated) {
        this.storeId = storeId;
        this.storeName = storeName;
        this.category = category;
        this.totalSales = totalSales;
        this.orderCount = orderCount;
        this.averageTicket = averageTicket;
        this.isOnline = isOnline;
        this.lastUpdated = lastUpdated;
    }

    public String getStoreId() { return storeId; }
    public void setStoreId(String storeId) { this.storeId = storeId; }

    public String getStoreName() { return storeName; }
    public void setStoreName(String storeName) { this.storeName = storeName; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public Double getTotalSales() { return totalSales; }
    public void setTotalSales(Double totalSales) { this.totalSales = totalSales; }

    public Long getOrderCount() { return orderCount; }
    public void setOrderCount(Long orderCount) { this.orderCount = orderCount; }

    public Double getAverageTicket() { return averageTicket; }
    public void setAverageTicket(Double averageTicket) { this.averageTicket = averageTicket; }

    public boolean isOnline() { return isOnline; }
    public void setOnline(boolean online) { isOnline = online; }

    public String getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(String lastUpdated) { this.lastUpdated = lastUpdated; }
}
