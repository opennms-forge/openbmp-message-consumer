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
import org.opennms.bmp.consumer.MessageStats;

@Service
@Command(scope = "bmp", name = "debug", description = "Debug")
public class Debug implements Action {

    @Reference
    private MessageStats messageStats;

    @Option(name = "clear")
    private boolean clear = false;

    @Option(name = "show")
    private boolean show = false;

    @Override
    public Object execute() {
        System.out.println("Number of messages by topic:");
        long now = System.currentTimeMillis();
        for (String topic : messageStats.getTopicNames()) {
            long lastMessageMs = messageStats.getLastMessageTimestampForTopic(topic);
            long secsSinceLastMessage = Math.floorDiv(now - lastMessageMs, 1000L);
            System.out.printf("%s: %d (%d seconds ago)\n", topic, messageStats.getNumMessagesForTopic(topic), secsSinceLastMessage);
            if (show) {
                Map.Entry<Object,Object> entry = messageStats.getLastMessageForTopic(topic);
                System.out.printf("K: %s\nV: %s\n\n", entry.getKey(), entry.getValue());
            }
        }
        if (clear) {
            messageStats.clearStats();
        }
        return null;
    }
}
