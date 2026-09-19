package com.smartship.edge.collector;

import com.smartship.edge.collector.model.ConfigAlarm;
import com.smartship.edge.collector.model.ConfigDevice;
import com.smartship.edge.collector.model.ConfigVariable;
import com.smartship.edge.collector.model.ICollectService;

import java.io.IOException;
import java.util.List;

public interface Collectable {
    void init(ICollectService collectService, ConfigDevice device, List<ConfigVariable> variables, List<ConfigAlarm> alarms);

    void connect();

    void collect() throws IOException;

    void close();
}
