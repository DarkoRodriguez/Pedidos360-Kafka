package pedidos360.analytics.service;

import java.util.List;

import pedidos360.analytics.dto.ConsumerGroupInfoDto;
import pedidos360.analytics.dto.CreateTopicDto;
import pedidos360.analytics.dto.KafkaTopicInfoDto;

public interface KafkaAdminService {
    List<KafkaTopicInfoDto> listTopics();
    KafkaTopicInfoDto createTopic(CreateTopicDto dto);
    boolean deleteTopic(String topicName);
    List<ConsumerGroupInfoDto> listConsumerGroups();
}
