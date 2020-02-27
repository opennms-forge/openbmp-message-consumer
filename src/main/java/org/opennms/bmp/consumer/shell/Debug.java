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
import org.openbmp.api.parsed.message.BaseAttribute;
import org.openbmp.api.parsed.message.BmpStat;
import org.openbmp.api.parsed.message.Collector;
import org.openbmp.api.parsed.message.Message;
import org.openbmp.api.parsed.message.Peer;
import org.openbmp.api.parsed.message.Router;
import org.openbmp.api.parsed.message.UnicastPrefix;
import org.opennms.bmp.consumer.TopicStatManager;
import org.opennms.bmp.consumer.TopicStats;

@Service
@Command(scope = "bmp", name = "debug", description = "Debug")
public class Debug implements Action {

    @Reference
    private TopicStatManager topicStatManager;

    @Option(name = "-c", aliases = "--context", description = "Use a specific context")
    private String contextFilter = null;

    @Option(name = "-t", aliases = "--topic", description = "Use a specific topic")
    private String topicFilter = null;

    @Option(name = "-p", aliases = "--parse", description = "Parse the messages")
    private boolean parse = false;

    @Option(name = "-x", aliases = "--clear", description = "Clear statistics for topics")
    private boolean clear = false;

    @Option(name = "-s", aliases = "--show", description = "Show last message on topics")
    private boolean show = false;

    @Override
    public Object execute() {
        long now = System.currentTimeMillis();
        for (String context : topicStatManager.getContexts()) {
            if (contextFilter != null && !contextFilter.equalsIgnoreCase(context)) {
                continue;
            }

            System.out.printf("\n\nTopic statistics for context: %s\n", context);
            for (String topic : topicStatManager.getTopicNames(context)) {
                if (topicFilter != null && !topicFilter.equalsIgnoreCase(topic)) {
                    continue;
                }

                final TopicStats topicStats = topicStatManager.getTopicStats(context, topic);
                if (topicStats == null) {
                    System.out.printf("%s: no messages\n", topic);
                    continue;
                }

                long secsSinceLastMessage = Math.max(Math.floorDiv(now - topicStats.getLastConsumedMsgTimestampMs(), 1000L), 0);
                System.out.printf("%s: %d (last consumed %d seconds ago)\n", topic, topicStats.getMsgCount(), secsSinceLastMessage);
                Map.Entry<Object,Object> entry = topicStats.getLastMsg();
                if (show) {
                    System.out.printf("K: %s\nV: %s\n\n", entry.getKey(), entry.getValue().toString().replace("\t", "░"));
                }

                if (parse) {
                    try {
                        Message msg = new Message(entry.getValue().toString());
                        System.out.printf("Parsed message with type: %s\n", msg.getType());
                        switch (msg.getType()) {
                            case "collector":
                                Collector collector = new Collector(msg.getContent());
                                System.out.printf("Collector: %s\n", collector.getRowMap());
                                break;
                            case "router":
                                Router router = new Router(msg.getContent());
                                System.out.printf("Router: %s\n", router.getRowMap());
                                break;
                            case "bmp_stat":
                                BmpStat bmpStat = new BmpStat(msg.getContent());
                                System.out.printf("BmpStat: %s\n", bmpStat.getRowMap());
                                break;
                            case "peer":
                                Peer peer = new Peer(msg.getContent());
                                System.out.printf("Peer: %s\n", peer.getRowMap());
                                break;
                            case "base_attribute":
                                BaseAttribute baseAttribute = new BaseAttribute(msg.getContent());
                                System.out.printf("BaseAttribute: %s\n", baseAttribute.getRowMap());
                                break;
                            case "unicast_prefix":
                                UnicastPrefix unicastPrefix = new UnicastPrefix(msg.getContent());
                                System.out.printf("UnicastPrefix: %s\n", unicastPrefix.getRowMap());
                                break;
                            default:
                                System.out.printf("Unsupported type: %s\n", msg.getType());
                        }
                    } catch (Throwable t) {
                        System.out.printf("Error while parsing: %s\n", entry.getValue());
                        t.printStackTrace();
                    }
                }
            }
        }

        if (clear) {
            topicStatManager.clearStats();
        }
        return null;
    }
}
