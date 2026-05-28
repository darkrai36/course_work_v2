package com.sentinel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.model.UserActivity;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@Slf4j
@RequiredArgsConstructor
public class ArchiveService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    // Потокобезопасный буфер для накопления событий
    private final List<UserActivity> buffer = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void initTable() {
        log.info("Очищаем и пересоздаем таблицу в ClickHouse...");
        try {
            jdbcTemplate.execute("DROP TABLE IF EXISTS user_activities");
            String createTableSql = """
                CREATE TABLE user_activities (
                    user_id String,
                    action_type String,
                    ip_address String,
                    event_time DateTime,
                    timestamp UInt64
                ) ENGINE = MergeTree()
                ORDER BY (event_time, user_id)
            """;
            jdbcTemplate.execute(createTableSql);
            log.info("Таблица user_activities успешно создана.");
        } catch (Exception e) {
            log.error("Ошибка инициализации ClickHouse", e);
        }
    }

    @KafkaListener(topics = "user-activities", groupId = "archive-group")
    public void consumeAndArchive(String message) {
        try {
            UserActivity activity = objectMapper.readValue(message, UserActivity.class);
            // Вместо записи в БД, просто кладем событие в буфер
            buffer.add(activity);
        } catch (Exception e) {
            log.error("Ошибка десериализации", e);
        }
    }

    // Выполняется каждую 1 секунду (1000 мс)
    @Scheduled(fixedRate = 1000)
    public void flushBuffer() {
        if (buffer.isEmpty()) return;

        // Забираем накопившиеся за секунду события (около 100 шт) и очищаем буфер
        List<UserActivity> batchToInsert = new ArrayList<>(buffer);
        buffer.clear();

        String sql = "INSERT INTO user_activities (user_id, action_type, ip_address, event_time, timestamp) VALUES (?, ?, ?, ?, ?)";

        try {
            // Отправляем всю пачку в ClickHouse за один запрос!
            jdbcTemplate.batchUpdate(sql, batchToInsert, batchToInsert.size(),
                    (PreparedStatement ps, UserActivity activity) -> {
                        ps.setString(1, activity.getUserId());
                        ps.setString(2, activity.getActionType().name());
                        ps.setString(3, activity.getIpAddress());
                        ps.setTimestamp(4, new Timestamp(activity.getTimestamp()));
                        ps.setLong(5, activity.getTimestamp());
                    });

            // Если раскомментировать, будешь видеть, как данные улетают пачками
            // log.info("Успешный Batch-Insert: {} событий", batchToInsert.size());
        } catch (Exception e) {
            log.error("Ошибка батч-вставки в ClickHouse", e);
            // Если база недоступна, возвращаем события в буфер, чтобы не потерять
            buffer.addAll(batchToInsert);
        }
    }
}