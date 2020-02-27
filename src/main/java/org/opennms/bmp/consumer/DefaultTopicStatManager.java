/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2020 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2020 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.bmp.consumer;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DefaultTopicStatManager implements TopicStatManager {
    private final Map<String, Map<String, TopicStatsImpl>> topicStatsByContext = new ConcurrentHashMap<>();

    @Override
    public List<String> getContexts() {
        return topicStatsByContext.keySet().stream()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getTopicNames(String context) {
        return topicStatsByContext.get(context).keySet().stream()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
    }

    @Override
    public TopicStats getTopicStats(String context, String topic) {
        return topicStatsByContext.get(context).get(topic);
    }

    @Override
    public void logMessageForTopic(String context, String topic, Object k, Object v) {
        final TopicStatsImpl stats = topicStatsByContext.computeIfAbsent(context, c -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, t -> new TopicStatsImpl());
        stats.msgCount.incrementAndGet();
        stats.lastConsumedMessageTimestampMs = System.currentTimeMillis();
        stats.lastMsg = new AbstractMap.SimpleEntry<>(k, v);
    }

    @Override
    public void clearStats() {
        topicStatsByContext.clear();
    }

    private static class TopicStatsImpl implements TopicStats {
        private final AtomicLong msgCount = new AtomicLong();
        private long lastConsumedMessageTimestampMs;
        private Map.Entry<Object, Object> lastMsg;

        @Override
        public long getMsgCount() {
            return msgCount.get();
        }

        @Override
        public long getLastConsumedMsgTimestampMs() {
            return lastConsumedMessageTimestampMs;
        }

        @Override
        public Map.Entry<Object, Object> getLastMsg() {
            return lastMsg;
        }
    }

}
