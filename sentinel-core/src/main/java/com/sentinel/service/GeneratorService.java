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

    // Просыпаемся 10 раз в секунду (каждые 100 мс)
    @Scheduled(fixedRate = 100)
    public void generateActivity() {
        // Генерируем пачку от 5 до 10 событий за один "тик" (итого 50-100 в сек)
        int batchSize = 5 + random.nextInt(6);

        for (int i = 0; i < batchSize; i++) {
            // Расширяем диапазон вероятностей до 1000 для более тонкой настройки
            int scenario = random.nextInt(1000);

            if (scenario < 985) {
                // 98.5% трафика - Обычные пользователи (создаем больше уникальных ID)
                sendEvent("user-" + random.nextInt(10000), getRandomNormalAction(), "192.168.1." + random.nextInt(255));
            } else if (scenario < 988) {
                // 0.3% - Брутфорс
                sendEvent("hacker-bruteforce", UserActivity.ActionType.LOGIN, "10.0.0.5");
            } else if (scenario < 991) {
                // 0.3% - Кардер
                sendEvent("hacker-carder", UserActivity.ActionType.PAYMENT, "10.0.0.6");
            } else if (scenario < 994) {
                // 0.3% - Угон аккаунта (Смена пароля и сразу платеж)
                sendEvent("hacker-takeover", UserActivity.ActionType.PASSWORD_CHANGE, "10.0.0.7");
                sendEvent("hacker-takeover", UserActivity.ActionType.PAYMENT, "10.0.0.7");
            } else {
                // 0.6% - Распределенная атака
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
        // Обычные пользователи чаще логинятся и выходят, реже платят
        int r = random.nextInt(10);
        if (r < 4) return UserActivity.ActionType.LOGIN;
        if (r < 7) return UserActivity.ActionType.LOGOUT;
        if (r < 9) return UserActivity.ActionType.PROFILE_UPDATE;
        return UserActivity.ActionType.PAYMENT;
    }
}