package com.sentinel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.model.UserActivity;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;

@Service
@Slf4j
@RequiredArgsConstructor
public class ArchiveService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    // Этот метод выполнится автоматически один раз при запуске приложения
    @PostConstruct
    public void initTable() {
        log.info("Проверяем наличие таблицы в ClickHouse...");
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS user_activities (
                user_id String,
                action_type String,
                ip_address String,
                event_time DateTime,
                timestamp UInt64
            ) ENGINE = MergeTree()
            ORDER BY (event_time, user_id)
        """;
        jdbcTemplate.execute(createTableSql);
        log.info("Таблица user_activities готова к работе.");
    }

    // НОВЫЙ GROUP ID! Это значит, что этот сервис получит свою независимую копию потока
    @KafkaListener(topics = "user-activities", groupId = "archive-group")
    public void consumeAndArchive(String message) {
        try {
            UserActivity activity = objectMapper.readValue(message, UserActivity.class);

            // В реальных высоконагруженных системах ClickHouse принимает данные "пачками" (батчами).
            // Для упрощения курсовой мы вставляем по одной строке, но база справится с нашей нагрузкой легко.
            String insertSql = "INSERT INTO user_activities (user_id, action_type, ip_address, event_time, timestamp) VALUES (?, ?, ?, ?, ?)";

            jdbcTemplate.update(insertSql,
                    activity.getUserId(),
                    activity.getActionType().name(),
                    activity.getIpAddress(),
                    new Timestamp(activity.getTimestamp()), // ClickHouse любит стандартный DateTime
                    activity.getTimestamp()
            );

            // Закомментируй лог ниже, если он будет слишком сильно спамить в консоль
            // log.debug("Событие сохранено в архив: {}", activity.getUserId());

        } catch (Exception e) {
            log.error("Ошибка при сохранении в ClickHouse", e);
        }
    }
}