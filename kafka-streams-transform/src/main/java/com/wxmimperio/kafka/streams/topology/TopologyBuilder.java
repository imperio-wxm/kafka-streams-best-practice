package com.wxmimperio.kafka.streams.topology;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface TopologyBuilder {
    Topology get(String sources, String sinks);

    Properties config();
}
