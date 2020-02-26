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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MessageStatsImpl implements MessageStats {
    private static final AtomicLong ZERO = new AtomicLong();
    private final Map<String, AtomicLong> msgCount = new ConcurrentHashMap<>();
    private final Map<String, Long> msgTimestamp = new ConcurrentHashMap<>();

    @Override
    public List<String> getTopicNames() {
        return msgCount.keySet().stream()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
    }

    @Override
    public long getNumMessagesForTopic(String topic) {
        return msgCount.getOrDefault(topic, ZERO).get();
    }

    @Override
    public long getLastMessageForTopic(String topic) {
        return msgTimestamp.get(topic);
    }

    @Override
    public void incrementNumMessageForTopic(String topic) {
        msgCount.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
        msgTimestamp.put(topic, System.currentTimeMillis());
    }

    @Override
    public void clearStats() {
        msgCount.clear();
        msgTimestamp.clear();
    }
}
