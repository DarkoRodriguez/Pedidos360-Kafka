package pedidos360.analytics.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import pedidos360.analytics.dto.ApiResponseDto;
import pedidos360.analytics.dto.ConsumerGroupInfoDto;
import pedidos360.analytics.dto.CreateTopicDto;
import pedidos360.analytics.dto.KafkaTopicInfoDto;
import pedidos360.analytics.service.KafkaAdminService;

@RestController
@RequestMapping("/api/admin/kafka")
@CrossOrigin(origins = "*")
public class KafkaAdminController {

    private final KafkaAdminService adminService;

    public KafkaAdminController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping("/topics")
    public ResponseEntity<ApiResponseDto<List<KafkaTopicInfoDto>>> listTopics() {
        List<KafkaTopicInfoDto> topics = adminService.listTopics();
        return ResponseEntity.ok(ApiResponseDto.ok("Tópicos listados correctamente", topics));
    }

    @PostMapping("/topics")
    public ResponseEntity<ApiResponseDto<KafkaTopicInfoDto>> createTopic(@Valid @RequestBody CreateTopicDto dto) {
        KafkaTopicInfoDto created = adminService.createTopic(dto);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponseDto.ok("Tópico '" + dto.getName() + "' creado con éxito", created));
    }

    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<ApiResponseDto<Void>> deleteTopic(@PathVariable String topicName) {
        adminService.deleteTopic(topicName);
        return ResponseEntity.ok(ApiResponseDto.ok("Tópico '" + topicName + "' eliminado correctamente", null));
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<ApiResponseDto<List<ConsumerGroupInfoDto>>> listConsumerGroups() {
        List<ConsumerGroupInfoDto> groups = adminService.listConsumerGroups();
        return ResponseEntity.ok(ApiResponseDto.ok("Grupos de consumidores consultados con éxito", groups));
    }
}
