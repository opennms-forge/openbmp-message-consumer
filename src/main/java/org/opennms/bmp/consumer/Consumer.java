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

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class Consumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    private ScheduledExecutorService scheduler;
    private KafkaStreams streams;
    private final AtomicBoolean closed = new AtomicBoolean(true);

    public void init() {
        LOG.info("init()");

        final Properties streamProperties = loadStreamsProperties();
        final StreamsBuilder builder = new StreamsBuilder();
        final List<String> topics = Arrays.asList("openbmp.parsed.base_attribute", "openbmp.parsed.bmp_stat", "openbmp.parsed.collector", "openbmp.parsed.peer", "openbmp.parsed.router", "openbmp.parsed.unicast_prefix");
        for (String topic : topics) {
            builder.stream(topic).foreach((k,v) -> {
                System.out.printf("[OpenBMP] Message received at time: %s\nKey: %s\n, Value: %s\n\n", new Date(), k, v);
            });
        }
        for (String topic : topics) {
            builder.stream("opennms." + topic).foreach((k,v) -> {
                System.out.printf("[OpenNMS] Message received at time: %s\nKey: %s\n, Value: %s\n\n", new Date(), k, v);
            });
        }
        final Topology topology = builder.build();

        // Use the class-loader for the KStream class, since the kafka-client bundle
        // does not import the required classes from the kafka-streams bundle
        streams = Utils.runWithGivenClassLoader(() -> new KafkaStreams(topology, streamProperties), KStream.class.getClassLoader());

        streams.setUncaughtExceptionHandler((t, e) -> LOG.error(
                String.format("Stream error on thread: %s", t.getName()), e));

        // Defer startup to another thread
        scheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("openbmp-message-consumer-%d")
                .build()
        );
        closed.set(false);
        scheduler.execute(this);
    }

    public void destroy() {
        LOG.info("Destroying consumer...");
        closed.set(true);
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (streams != null) {
            streams.close(2, TimeUnit.MINUTES);
        }
        LOG.info("Destroyed.");
    }

    private Properties loadStreamsProperties() {
        final Properties streamsProperties = new Properties();
        // Default values
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "openbmp-message-consumer");
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return streamsProperties;
    }

    @Override
    public void run() {
        try {
            LOG.info("Starting consumer stream.");
            streams.start();
            LOG.info("Consumer started.");
        } catch (StreamsException | IllegalStateException e) {
            LOG.error("Failed to start consumer stream", e);
        }
    }
}
