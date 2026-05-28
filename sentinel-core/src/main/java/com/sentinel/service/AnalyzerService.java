package com.sentinel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.model.Alert;
import com.sentinel.model.UserActivity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class AnalyzerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SimpMessagingTemplate messagingTemplate;

    // НОВОЕ: Клиент для работы с Redis
    private final StringRedisTemplate redisTemplate;

    private static final String ALERTS_TOPIC = "alerts";
    private static final long WINDOW_MS = 5 * 60 * 1000; // Окно в 5 минут

    @KafkaListener(topics = "user-activities", groupId = "sentinel-group")
    public void consume(String message) {
        try {
            UserActivity activity = objectMapper.readValue(message, UserActivity.class);
            String userId = activity.getUserId();
            // Уникальный ключ для каждого юзера в Redis
            String redisKey = "user:window:" + userId;

            // 1. Добавляем событие в Redis Sorted Set (Score = время)
            redisTemplate.opsForZSet().add(redisKey, message, activity.getTimestamp());

            // 2. СДВИГАЕМ ОКНО: удаляем всё, что старше 5 минут
            long minValidTime = activity.getTimestamp() - WINDOW_MS;
            redisTemplate.opsForZSet().removeRangeByScore(redisKey, 0, minValidTime);

            // 3. Обновляем время жизни ключа (TTL), чтобы старые данные сами исчезали
            redisTemplate.expire(redisKey, 5, TimeUnit.MINUTES);

            // 4. Достаем актуальную историю за 5 минут для анализа
            Set<String> historyJsons = redisTemplate.opsForZSet().range(redisKey, 0, -1);
            if (historyJsons != null && !historyJsons.isEmpty()) {
                // Превращаем JSON-строки обратно в объекты Java
                List<UserActivity> history = historyJsons.stream()
                        .map(json -> {
                            try { return objectMapper.readValue(json, UserActivity.class); }
                            catch (Exception e) { return null; }
                        })
                        .filter(java.util.Objects::nonNull)
                        .toList();

                analyzePatterns(userId, history);
            }

        } catch (Exception e) {
            log.error("Ошибка при анализе потока из Kafka", e);
        }
    }

    private void analyzePatterns(String userId, List<UserActivity> history) {
        long now = System.currentTimeMillis();

        // Сценарий 1: Brute-force (> 5 логинов за 1 минуту)
        long oneMinAgo = now - 60_000;
        long loginCount = history.stream()
                .filter(a -> a.getActionType() == UserActivity.ActionType.LOGIN && a.getTimestamp() > oneMinAgo)
                .count();
        if (loginCount > 5) {
            sendAlert(userId, "BRUTE_FORCE", "Перебор паролей: " + loginCount + " попыток входа за минуту.");
            return; // Прерываем анализ, чтобы не спамить алертами
        }

        // Сценарий 2: Carding (> 3 платежей за 2 минуты)
        long twoMinAgo = now - 120_000;
        long paymentCount = history.stream()
                .filter(a -> a.getActionType() == UserActivity.ActionType.PAYMENT && a.getTimestamp() > twoMinAgo)
                .count();
        if (paymentCount > 3) {
            sendAlert(userId, "CARDING", "Аномалия транзакций: " + paymentCount + " платежей подряд.");
            return;
        }

        // Сценарий 3: Account Takeover (Смена данных + Платеж)
        boolean hasSecurityChange = history.stream().anyMatch(a ->
                a.getActionType() == UserActivity.ActionType.PASSWORD_CHANGE ||
                        a.getActionType() == UserActivity.ActionType.PROFILE_UPDATE);
        boolean hasRecentPayment = history.stream().anyMatch(a -> a.getActionType() == UserActivity.ActionType.PAYMENT);

        if (hasSecurityChange && hasRecentPayment) {
            sendAlert(userId, "ACCOUNT_TAKEOVER", "Угон аккаунта: смена учетных данных и немедленный платеж!");
            return;
        }

        // Сценарий 4: Distributed Attack (много разных IP)
        long uniqueIps = history.stream().map(UserActivity::getIpAddress).distinct().count();
        if (uniqueIps > 3) {
            sendAlert(userId, "DISTRIBUTED_ATTACK", "Подозрительная география: активность с " + uniqueIps + " разных IP.");
        }
    }

    private void sendAlert(String userId, String type, String message) {
        try {
            // Чтобы не дублировать один и тот же алерт каждую секунду, мы "сбрасываем" историю мошенника после поимки
            redisTemplate.delete("user:window:" + userId);

            Alert alert = new Alert(userId, type, message, System.currentTimeMillis());
            String payload = objectMapper.writeValueAsString(alert);

            kafkaTemplate.send(ALERTS_TOPIC, userId, payload);
            messagingTemplate.convertAndSend("/topic/alerts", alert);

            log.warn("АЛЕРТ [{}]: {}", type, message);
        } catch (Exception e) {
            log.error("Ошибка при отправке алерта", e);
        }
    }
}