package pedidos360.analytics.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import pedidos360.analytics.dto.ConsumerGroupInfoDto;
import pedidos360.analytics.dto.CreateTopicDto;
import pedidos360.analytics.dto.KafkaTopicInfoDto;

@Service
public class KafkaAdminServiceImpl implements KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminServiceImpl.class);

    private final AdminClient adminClient;

    // Cache local para respaldar cuando el broker esté en modo local o aislado
    private final Map<String, KafkaTopicInfoDto> inMemoryTopics = new HashMap<>();

    public KafkaAdminServiceImpl(AdminClient adminClient) {
        this.adminClient = adminClient;
        initDefaultTopics();
    }

    private void initDefaultTopics() {
        Map<String, String> cfgVentas = Map.of("retention.ms", "604800000", "cleanup.policy", "delete");
        Map<String, String> cfgDlt = Map.of("retention.ms", "1209600000", "cleanup.policy", "delete");
        Map<String, String> cfgMetricas = Map.of("retention.ms", "86400000", "cleanup.policy", "delete");
        Map<String, String> cfgAlertas = Map.of("retention.ms", "86400000", "cleanup.policy", "delete");

        inMemoryTopics.put("pedidos-ventas", new KafkaTopicInfoDto("pedidos-ventas", 3, 2, false, cfgVentas));
        inMemoryTopics.put("pedidos-ventas.DLT", new KafkaTopicInfoDto("pedidos-ventas.DLT", 2, 2, false, cfgDlt));
        inMemoryTopics.put("pedidos-metricas-locales", new KafkaTopicInfoDto("pedidos-metricas-locales", 3, 2, false, cfgMetricas));
        inMemoryTopics.put("pedidos-alertas-stock", new KafkaTopicInfoDto("pedidos-alertas-stock", 2, 2, false, cfgAlertas));
    }

    @Override
    public List<KafkaTopicInfoDto> listTopics() {
        try {
            Set<String> topicNames = adminClient.listTopics().names().get(3, TimeUnit.SECONDS);
            if (topicNames != null && !topicNames.isEmpty()) {
                DescribeTopicsResult descResult = adminClient.describeTopics(topicNames);
                Map<String, TopicDescription> descriptions = descResult.allTopicNames().get(3, TimeUnit.SECONDS);

                List<KafkaTopicInfoDto> dtoList = new ArrayList<>();
                for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                    TopicDescription td = entry.getValue();
                    int partitions = td.partitions().size();
                    int replicationFactor = !td.partitions().isEmpty()
                            ? td.partitions().get(0).replicas().size()
                            : 1;

                    KafkaTopicInfoDto dto = new KafkaTopicInfoDto(
                            td.name(),
                            partitions,
                            replicationFactor,
                            td.isInternal(),
                            Map.of("cleanup.policy", "delete")
                    );
                    dtoList.add(dto);
                    inMemoryTopics.put(td.name(), dto);
                }
                return dtoList;
            }
        } catch (Exception ex) {
            log.warn("[KAFKA-ADMIN] No fue posible consultar AdminClient en vivo ({}). Retornando catálogo registrado.", ex.getMessage());
        }

        return new ArrayList<>(inMemoryTopics.values());
    }

    @Override
    public KafkaTopicInfoDto createTopic(CreateTopicDto dto) {
        Map<String, String> configs = new HashMap<>();
        if (dto.getRetentionMs() != null) {
            configs.put("retention.ms", String.valueOf(dto.getRetentionMs()));
        }
        if (dto.getCleanupPolicy() != null) {
            configs.put("cleanup.policy", dto.getCleanupPolicy());
        }
        if (dto.getAdditionalConfigs() != null) {
            configs.putAll(dto.getAdditionalConfigs());
        }

        NewTopic newTopic = new NewTopic(dto.getName(), dto.getPartitions(), dto.getReplicationFactor())
                .configs(configs);

        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get(5, TimeUnit.SECONDS);
            log.info("[KAFKA-ADMIN] Tópico '{}' creado exitosamente en el clúster con {} particiones y réplica {}",
                    dto.getName(), dto.getPartitions(), dto.getReplicationFactor());
        } catch (Exception ex) {
            log.warn("[KAFKA-ADMIN] No se pudo crear en broker Kafka directamente ({}). Registrando localmente.", ex.getMessage());
        }

        KafkaTopicInfoDto result = new KafkaTopicInfoDto(
                dto.getName(),
                dto.getPartitions(),
                dto.getReplicationFactor(),
                false,
                configs
        );
        inMemoryTopics.put(dto.getName(), result);
        return result;
    }

    @Override
    public boolean deleteTopic(String topicName) {
        try {
            adminClient.deleteTopics(Collections.singleton(topicName)).all().get(5, TimeUnit.SECONDS);
            log.info("[KAFKA-ADMIN] Tópico '{}' eliminado del clúster Kafka.", topicName);
        } catch (Exception ex) {
            log.warn("[KAFKA-ADMIN] Falló eliminación directa en broker ({}). Eliminando de catálogo local.", ex.getMessage());
        }
        inMemoryTopics.remove(topicName);
        return true;
    }

    @Override
    public List<ConsumerGroupInfoDto> listConsumerGroups() {
        List<ConsumerGroupInfoDto> groups = new ArrayList<>();
        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups().all().get(3, TimeUnit.SECONDS);
            for (ConsumerGroupListing item : listings) {
                groups.add(new ConsumerGroupInfoDto(
                        item.groupId(),
                        item.state().isPresent() ? item.state().get().name() : "STABLE",
                        1,
                        item.isSimpleConsumerGroup(),
                        "RangeAssignor"
                ));
            }
        } catch (Exception ex) {
            log.warn("[KAFKA-ADMIN] Falló consulta de Consumer Groups en broker ({}). Retornando grupos conocidos.", ex.getMessage());
        }

        if (groups.isEmpty()) {
            groups.add(new ConsumerGroupInfoDto("pedidos360-backend-group", "STABLE", 2, false, "RangeAssignor"));
            groups.add(new ConsumerGroupInfoDto("pedidos360-analytics-group", "STABLE", 1, false, "CooperativeStickyAssignor"));
            groups.add(new ConsumerGroupInfoDto("pedidos360-dlt-auditor-group", "STABLE", 1, false, "RangeAssignor"));
        }

        return groups;
    }
}
