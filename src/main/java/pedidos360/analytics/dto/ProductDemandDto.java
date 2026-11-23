package pedidos360.analytics.dto;

import java.io.Serializable;

public class ProductDemandDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private Long liveSales;      // SUM / COUNT de unidades vendidas
    private String icon;
    private Double percentage;

    public ProductDemandDto() {}

    public ProductDemandDto(String name, Long liveSales, String icon, Double percentage) {
        this.name = name;
        this.liveSales = liveSales;
        this.icon = icon;
        this.percentage = percentage;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Long getLiveSales() { return liveSales; }
    public void setLiveSales(Long liveSales) { this.liveSales = liveSales; }

    public String getIcon() { return icon; }
    public void setIcon(String icon) { this.icon = icon; }

    public Double getPercentage() { return percentage; }
    public void setPercentage(Double percentage) { this.percentage = percentage; }
}
