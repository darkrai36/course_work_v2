package com.sentinel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.model.UserActivity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
@Slf4j
@RequiredArgsConstructor
public class GeneratorService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();
    private static final String TOPIC = "user-activities";

    @Scheduled(fixedRate = 100)
    public void generateActivity() {
        int batchSize = 5 + random.nextInt(6);

        for (int i = 0; i < batchSize; i++) {
            int scenario = random.nextInt(10000);

            if (scenario < 9940) {
                int normalUserId = random.nextInt(1_000_000);

                String staticIp = "192.168." + (normalUserId % 255) + "." + ((normalUserId / 255) % 255);

                sendEvent("user-" + normalUserId, getRandomNormalAction(), staticIp);

            } else if (scenario < 9955) {
                // 0.15% - Брутфорс
                sendEvent("hacker-bruteforce", UserActivity.ActionType.LOGIN, "10.0.0.5");
            } else if (scenario < 9970) {
                // 0.15% - Кардер
                sendEvent("hacker-carder", UserActivity.ActionType.PAYMENT, "10.0.0.6");
            } else if (scenario < 9980) {
                // 0.1% - Угон аккаунта
                sendEvent("hacker-takeover", UserActivity.ActionType.PASSWORD_CHANGE, "10.0.0.7");
                sendEvent("hacker-takeover", UserActivity.ActionType.PAYMENT, "10.0.0.7");
            } else {
                // 0.2% - Распределенная атака
                sendEvent("hacker-distributed", UserActivity.ActionType.LOGIN, "172.16." + random.nextInt(255) + "." + random.nextInt(255));
            }
        }
    }

    private void sendEvent(String userId, UserActivity.ActionType type, String ip) {
        try {
            UserActivity activity = new UserActivity(userId, type, ip, System.currentTimeMillis());
            String payload = objectMapper.writeValueAsString(activity);
            kafkaTemplate.send(TOPIC, userId, payload);
        } catch (Exception e) {
            log.error("Ошибка генерации", e);
        }
    }

    private UserActivity.ActionType getRandomNormalAction() {
        int r = random.nextInt(100);
        // Обычные пользователи просто заходят (45%), выходят (45%) или иногда делают покупки (10%).
        // Мы убрали PROFILE_UPDATE, чтобы они случайно не триггерили логику "Угона аккаунта".
        if (r < 45) return UserActivity.ActionType.LOGIN;
        if (r < 90) return UserActivity.ActionType.LOGOUT;
        return UserActivity.ActionType.PAYMENT;
    }
}