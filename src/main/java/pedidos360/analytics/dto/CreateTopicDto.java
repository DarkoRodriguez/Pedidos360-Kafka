package pedidos360.analytics.dto;

import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public class CreateTopicDto {

    @NotBlank(message = "El nombre del tópico es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre del tópico solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String name;

    @Min(value = 1, message = "Debe tener al menos 1 partición")
    @Max(value = 50, message = "Máximo 50 particiones")
    private int partitions = 3;

    @Min(value = 1, message = "Factor de replicación mínimo es 1")
    @Max(value = 5, message = "Factor de replicación máximo es 5")
    private short replicationFactor = 1;

    private Long retentionMs = 604800000L; // 7 días por defecto
    private String cleanupPolicy = "delete"; // delete o compact
    private Map<String, String> additionalConfigs;

    public CreateTopicDto() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }

    public Long getRetentionMs() { return retentionMs; }
    public void setRetentionMs(Long retentionMs) { this.retentionMs = retentionMs; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }

    public Map<String, String> getAdditionalConfigs() { return additionalConfigs; }
    public void setAdditionalConfigs(Map<String, String> additionalConfigs) { this.additionalConfigs = additionalConfigs; }
}
