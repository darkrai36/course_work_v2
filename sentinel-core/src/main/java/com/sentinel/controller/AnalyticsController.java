package com.sentinel.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
@RequiredArgsConstructor
@Slf4j
public class AnalyticsController {

    // Используем тот же JdbcTemplate, который настроен на ClickHouse
    private final JdbcTemplate jdbcTemplate;

    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        try {
            // 1. Общее количество обработанных событий
            Long totalEvents = jdbcTemplate.queryForObject("SELECT count() FROM user_activities", Long.class);
            stats.put("totalEvents", totalEvents);

            // 2. Распределение по типам (сколько было логинов, платежей и т.д.)
            List<Map<String, Object>> eventsByType = jdbcTemplate.queryForList(
                    "SELECT action_type as name, count() as value FROM user_activities GROUP BY action_type"
            );
            stats.put("eventsByType", eventsByType);

            // 3. Топ-3 самых активных (или подозрительных) IP-адресов
            List<Map<String, Object>> topIps = jdbcTemplate.queryForList(
                    "SELECT ip_address as ip, count() as count FROM user_activities GROUP BY ip_address ORDER BY count DESC LIMIT 3"
            );
            stats.put("topIps", topIps);

        } catch (Exception e) {
            log.error("Ошибка при запросе статистики", e);
            stats.put("error", "Не удалось загрузить статистику");
        }

        return stats;
    }
}