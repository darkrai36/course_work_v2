package com.sentinel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.model.UserActivity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
@Slf4j
@RequiredArgsConstructor
@EnableScheduling
public class GeneratorService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private final Random random = new Random();

    private static final String TOPIC = "user-activities";

    @Scheduled(fixedRate = 1000)
    public void generateActivity() {
        try {
            // 30% шанс, что сейчас действует мошенник
            boolean isSuspicious = random.nextInt(10) < 3;
            String userId = isSuspicious ? "user-suspicious-1" : "user-" + random.nextInt(100);

            // Если это мошенник, он брутфорсит (всегда LOGIN). Иначе - случайное действие.
            UserActivity.ActionType action = isSuspicious ? UserActivity.ActionType.LOGIN : getRandomAction();

            UserActivity activity = new UserActivity(
                    userId,
                    action,
                    "192.168.1." + random.nextInt(255),
                    System.currentTimeMillis()
            );

            String payload = objectMapper.writeValueAsString(activity);
            kafkaTemplate.send(TOPIC, activity.getUserId(), payload);

            log.info("Produced activity: {}", payload);
        } catch (Exception e) {
            log.error("Error producing activity", e);
        }
    }

    private UserActivity.ActionType getRandomAction() {
        UserActivity.ActionType[] actions = UserActivity.ActionType.values();
        return actions[random.nextInt(actions.length)];
    }
}