package org.graylog.inputs.beats.plugin;

import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.PluginModule;

import java.util.Arrays;
import java.util.Collection;

/**
 * Implement the Plugin interface here.
 */
public class BeatsInputPlugin implements Plugin {
    @Override
    public PluginMetaData metadata() {
        return new BeatsInputPluginMetaData();
    }

    @Override
    public Collection<PluginModule> modules () {
        return Arrays.<PluginModule>asList(new BeatsInputPluginModule());
    }
}
