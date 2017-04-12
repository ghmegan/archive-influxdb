/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.xml;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import org.csstudio.archive.config.ArchiveConfig;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.SampleMode;

/**
 * InfluxDB implementation of {@link ArchiveConfig}
 *
 * <p>
 * Provides read access via {@link ArchiveConfig} API, may in future allow write
 * access via additional InfluxDB-only methods.
 *
 * @author Megan Grodowitz - XML implementation
 */
@SuppressWarnings("nls")
public class XMLArchiveConfig implements ArchiveConfig
{
    /** Configured engines mapped by unique configuration id */
    final private Map<Integer, EngineConfig> engines_id2obj = new HashMap<Integer, EngineConfig>();

    /** Configured engines mapping of name to unique configuration id */
    final private Map<String, Integer> engines_name2id = new HashMap<String, Integer>();

    /** Next unique engine id to assign */
    private int next_engine_id;
    /** Next unique group id to assign */
    private int next_group_id;
    /** Next unique channel id to assign */
    private int next_channel_id;

    /** Initialize.
     *  This constructor will be invoked when an {@link ArchiveConfig}
     *  is created via the extension point.
     *  @throws Exception on error, for example InfluxDB connection error
     */
    public XMLArchiveConfig() throws Exception
    {
        next_group_id = 100;
        next_engine_id = 100;
        next_channel_id = 100;
    }

    /** {@inheritDoc} */
    @Override
    public EngineConfig[] getEngines() throws Exception
    {
        return engines_id2obj.values().toArray(new EngineConfig[engines_id2obj.size()]);
    }

    /** Determine sample mode
     *  @param sample_mode_id Sample mode ID from InfluxDB
     *  @param sample_value Sample value, i.e. monitor threshold
     *  @param period Scan period, estimated monitor period
     *  @return {@link SampleMode}
     *  @throws Exception
     */
    @Override
    public SampleMode getSampleMode(final boolean monitor, final double sample_value, final double period)
            throws Exception
    {
        return new SampleMode(monitor, sample_value, period);
    }

    /** Create new engine config in InfluxDB
     *  @param engine_name
     *  @param description
     *  @param engine_url
     *  @return
     *  @throws Exception
     */
    @Override
    public EngineConfig createEngine(final String engine_name, final String description,
            final String engine_url) throws Exception
    {
        if (engines_name2id.get(engine_name) != null)
        {
            throw new Exception ("Engine " + engine_name + " already exists.");
        }

        final int engine_id = next_engine_id;
        next_engine_id++;
        EngineConfig engine = new XMLEngineConfig(engine_id, engine_name, description, engine_url);
        engines_name2id.put(engine_name, engine_id);
        engines_id2obj.put(engine_id, engine);
        return engine;
    }

    /** {@inheritDoc} */
    @Override
    public EngineConfig findEngine(final String name) throws Exception
    {
        Integer id = engines_name2id.get(name);
        if (engines_name2id.get(name) == null)
        {
            return null;
        }
        return engines_id2obj.get(id);
    }

    /** Get engine for group
     *  @param group {@link XMLGroupConfig}
     *  @return {@link EngineConfig} for that group or <code>null</code>
     *  @throws Exception on error
     */
    @Override
    public EngineConfig getEngine(final GroupConfig the_group) throws Exception
    {
        // TODO: Antipattern
        XMLGroupConfig group = (XMLGroupConfig) the_group;
        final EngineConfig engine = engines_id2obj.get(group.getEngineId());
        if (engine == null)
        {
            Activator.getLogger().log(Level.WARNING, () -> "Could not get engine for group "
                    + group.getName() + ", engine id = " + group.getEngineId());
        }
        return engine;
    }

    /** Delete engine info, all the groups under it, and clear all links
     *  from channels to those groups.
     *  @param engine Engine info to remove
     *  @throws Exception on error
     */
    @Override
    public void deleteEngine(final EngineConfig engine) throws Exception
    {
        XMLEngineConfig influxdb_engine = ((XMLEngineConfig)engine);
        final int engine_id = influxdb_engine.getId();
        final String engine_name = influxdb_engine.getName();

        if (!engines_id2obj.containsKey(engine_id))
            throw new Exception("Cannot delete unknown engine " + engine_name);

        engines_name2id.remove(engine_name);
        engines_id2obj.remove(engine_id);
    }

    /** @param engine Engine to which to add group
     *  @param name Name of new group
     *  @return {@link XMLGroupConfig}
     *  @throws Exception on error
     */
    @Override
    public XMLGroupConfig addGroup(final EngineConfig engine, final String name) throws Exception
    {
        final int group_id = next_group_id;
        XMLGroupConfig group = ((XMLEngineConfig) engine).addGroup(group_id, name, null);
        if (group != null)
            next_group_id++;
        return group;
    }

    /** {@inheritDoc} */
    @Override
    public GroupConfig[] getGroups(final EngineConfig engine) throws Exception
    {
        final XMLEngineConfig influxdb_engine = (XMLEngineConfig) engine;
        return influxdb_engine.getGroupsArray();
    }

    /** @param channel_name Name of a channel
     *  @return {@link GroupConfig} for that channel or <code>null</code>
     *  @throws Exception on error
     */
    @Override
    public XMLGroupConfig getChannelGroup(final String channel_name) throws Exception
    {
        for (EngineConfig engine : engines_id2obj.values())
        {
            for (GroupConfig group : ((XMLEngineConfig)engine).getGroupObjs())
            {
                if (((XMLGroupConfig)group).containsChannel(channel_name))
                    return ((XMLGroupConfig)group);
            }
        }
        return null;
    }

    /** Set a group's enabling channel
     *  @param group Group that should enable based on a channel
     *  @param channel Channel or <code>null</code> to 'always' activate the group
     *  @throws Exception on error
     */
    @Override
    public void setEnablingChannel(final GroupConfig group, final ChannelConfig channel) throws Exception
    {
        group.setEnablingChannel(channel);
    }

    /** Add a channel.
     *
     *  <p>The channel might already exist in the InfluxDB, but maybe it is not attached
     *  to a sample engine's group, or it's attached to a different group.
     *
     *  @param group {@link XMLGroupConfig} to which to add the channel
     *  @param channel_name Name of channel
     *  @param mode Sample mode
     *  @return {@link XMLChannelConfig}
     *  @throws Exception on error
     */
    @Override
    public XMLChannelConfig addChannel(final GroupConfig the_group, final String channel_name,
            final SampleMode mode) throws Exception
    {
        // TODO: Antipattern
        XMLGroupConfig group = (XMLGroupConfig) the_group;
        final int channel_id = next_channel_id;
        XMLChannelConfig channel = group.addChannel(channel_id, channel_name, mode, null);
        if (channel != null)
        {
            next_channel_id++;
        }
        return channel;
    }

    /** {@inheritDoc} */
    @Override
    public ChannelConfig[] getChannels(final GroupConfig group, final boolean skip_last) throws Exception
    {
        final XMLGroupConfig influxdb_group = (XMLGroupConfig) group;
        return influxdb_group.getChannelArray();
    }

    /** {@inheritDoc} */
    @Override
    public void close()
    {

    }
}
