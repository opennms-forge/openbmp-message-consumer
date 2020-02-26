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

package org.opennms.bmp.consumer.shell;

import java.util.Map;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.bmp.consumer.TopicStatManager;
import org.opennms.bmp.consumer.TopicStats;

@Service
@Command(scope = "bmp", name = "debug", description = "Debug")
public class Debug implements Action {

    @Reference
    private TopicStatManager topicStatManager;

    @Option(name = "-c", aliases = "--clear", description = "Clear statistics for topics")
    private boolean clear = false;

    @Option(name = "-s", aliases = "--show", description = "Show last message on topics")
    private boolean show = false;

    @Override
    public Object execute() {
        System.out.println("Number of messages by topic:");
        long now = System.currentTimeMillis();
        for (String topic : topicStatManager.getTopicNames()) {
            final TopicStats topicStats = topicStatManager.getTopicStats(topic);
            if (topicStats == null) {
                System.out.printf("%s: no messages\n", topic);
                continue;
            }

            long secsSinceLastMessage = Math.max(Math.floorDiv(now - topicStats.getLastConsumedMsgTimestampMs(), 1000L), 0);
            System.out.printf("%s: %d (last consumed %d seconds ago)\n", topic, topicStats.getMsgCount(), secsSinceLastMessage);
            if (show) {
                Map.Entry<Object,Object> entry = topicStats.getLastMsg();
                System.out.printf("K: %s\nV: %s\n\n", entry.getKey(), entry.getValue());
            }
        }
        if (clear) {
            topicStatManager.clearStats();
        }
        return null;
    }
}
