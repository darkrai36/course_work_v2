package com.sentinel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.model.Alert;
import com.sentinel.model.UserActivity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class AnalyzerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SimpMessagingTemplate messagingTemplate;

    private static final String ALERTS_TOPIC = "alerts";

    private static final long WINDOW_MS = 30000;
    private static final int THRESHOLD = 3;

    private final Map<String, List<Long>> activityWindows = new ConcurrentHashMap<>();

    @KafkaListener(topics = "user-activities", groupId = "sentinel-group")
    public void consume(String message) {
        try {
            UserActivity activity = objectMapper.readValue(message, UserActivity.class);
            log.debug("Обработка события: {}", activity.getUserId());

            if (activity.getActionType() == UserActivity.ActionType.LOGIN) {
                analyzeLogin(activity);
            }
        } catch (Exception e) {
            log.error("Ошибка при анализе активности", e);
        }
    }

    private void analyzeLogin(UserActivity activity) {
        String userId = activity.getUserId();
        long now = activity.getTimestamp();

        activityWindows.compute(userId, (id, timestamps) -> {
            if (timestamps == null) timestamps = new ArrayList<>();

            timestamps.add(now);

            timestamps.removeIf(t -> t < now - WINDOW_MS);

            if (timestamps.size() > THRESHOLD) {
                sendAlert(userId, timestamps.size());
                timestamps.clear();
            }

            return timestamps;
        });
    }

    private void sendAlert(String userId, int count) {
        try {
            Alert alert = new Alert(
                    userId,
                    "SUSPICIOUS_LOGIN",
                    String.format("Обнаружена аномалия! Пользователь совершил %d попыток входа за 30 секунд.", count),
                    System.currentTimeMillis()
            );

            String payload = objectMapper.writeValueAsString(alert);

            kafkaTemplate.send(ALERTS_TOPIC, userId, payload);

            log.warn("🚨 АНОМАЛИЯ ОБНАРУЖЕНА: {}", payload);

            messagingTemplate.convertAndSend("/topic/alerts", alert);
        } catch (Exception e) {
            log.error("Ошибка при отправке алерта", e);
        }
    }
}