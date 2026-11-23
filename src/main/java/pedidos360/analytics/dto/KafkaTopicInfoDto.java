package pedidos360.analytics.dto;

import java.util.Map;

public class KafkaTopicInfoDto {

    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean isInternal;
    private Map<String, String> configs;

    public KafkaTopicInfoDto() {}

    public KafkaTopicInfoDto(String name, int partitions, int replicationFactor, boolean isInternal, Map<String, String> configs) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.isInternal = isInternal;
        this.configs = configs;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public boolean isInternal() { return isInternal; }
    public void setInternal(boolean internal) { isInternal = internal; }

    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}
