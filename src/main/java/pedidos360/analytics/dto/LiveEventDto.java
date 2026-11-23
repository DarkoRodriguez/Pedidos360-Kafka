package pedidos360.analytics.dto;

import java.io.Serializable;

public class LiveEventDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private String timestamp;
    private String text;
    private String type; // "ORDER_ACCEPTED", "ORDER_PLACED", "STOCK_UPDATE", "ALERT"
    private String correlationId;

    public LiveEventDto() {}

    public LiveEventDto(String timestamp, String text, String type, String correlationId) {
        this.timestamp = timestamp;
        this.text = text;
        this.type = type;
        this.correlationId = correlationId;
    }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getText() { return text; }
    public void setText(String text) { this.text = text; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
}
