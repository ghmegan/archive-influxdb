/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.influxdb;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import org.csstudio.archive.config.ArchiveConfig;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.SampleMode;
import org.csstudio.archive.influxdb.InfluxDBArchivePreferences;
import org.csstudio.archive.influxdb.InfluxDBQueries;
import org.csstudio.archive.influxdb.InfluxDBQueries.DBNameMap;
import org.csstudio.archive.influxdb.InfluxDBQueries.DefaultDBNameMap;
import org.csstudio.archive.influxdb.InfluxDBResults;
import org.csstudio.archive.influxdb.InfluxDBUtil;
import org.influxdb.InfluxDB;

/** InfluxDB implementation of {@link ArchiveConfig}
 *
 *  <p>Provides read access via {@link ArchiveConfig} API,
 *  may in future allow write access via additional InfluxDB-only methods.
 *
 *  @author Kay Kasemir
 *  @author Megan Grodowitz - InfuxDB implementation
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveConfig implements ArchiveConfig
{
    /** InfluxDB connection */
    final private InfluxDB influxdb;

    /** InfluxDB statements */
    final private InfluxDBQueries influxQuery;

    final static private DBNameMap dbnames = new DefaultDBNameMap();

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
    public InfluxDBArchiveConfig() throws Exception
    {
        this(InfluxDBArchivePreferences.getURL(), InfluxDBArchivePreferences.getUser(),
                InfluxDBArchivePreferences.getPassword());
    }

    /** Initialize.
     *  This constructor can be invoked by test code.
     *  @param url InfluxDB URL
     *  @param user .. user name
     *  @param password .. password
     *  @param schema Schema/table prefix, ending in ".". May be empty
     *  @throws Exception on error, for example InfluxDB connection error
     */
    public InfluxDBArchiveConfig(final String url, final String user, final String password) throws Exception
    {
        next_group_id = 100;
        next_engine_id = 100;
        next_channel_id = 100;

        influxdb = InfluxDBUtil.connect(url, user, password);
        influxQuery = new InfluxDBQueries(influxdb, dbnames);
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
    public InfluxDBSampleMode getSampleMode(final boolean monitor, final double sample_value, final double period) throws Exception
    {
        //return new InfluxDBSampleMode(monitor ? monitor_mode_id : scan_mode_id, monitor, sample_value, period);
        return new InfluxDBSampleMode(monitor, sample_value, period);
    }

    /** Create new engine config in InfluxDB
     *  @param engine_name
     *  @param description
     *  @param engine_url
     *  @return
     *  @throws Exception
     */
    public EngineConfig createEngine(final String engine_name, final String description,
            final String engine_url) throws Exception
    {
        if (engines_name2id.get(engine_name) != null)
        {
            throw new Exception ("Engine " + engine_name + " already exists.");
        }

        final int engine_id = next_engine_id;
        next_engine_id++;
        EngineConfig engine = new InfluxDBEngineConfig(engine_id, engine_name, description, engine_url);
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
     *  @param group {@link InfluxDBGroupConfig}
     *  @return {@link EngineConfig} for that group or <code>null</code>
     *  @throws Exception on error
     */
    public EngineConfig getEngine(final InfluxDBGroupConfig group) throws Exception
    {
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
    public void deleteEngine(final EngineConfig engine) throws Exception
    {
        InfluxDBEngineConfig influxdb_engine = ((InfluxDBEngineConfig)engine);
        final int engine_id = influxdb_engine.getId();
        final String engine_name = influxdb_engine.getName();

        if (!engines_id2obj.containsKey(engine_id))
            throw new Exception("Cannot delete unknown engine " + engine_name);

        engines_name2id.remove(engine_name);
        engines_id2obj.remove(engine_id);
    }

    /** @param engine Engine to which to add group
     *  @param name Name of new group
     *  @return {@link InfluxDBGroupConfig}
     *  @throws Exception on error
     */
    public InfluxDBGroupConfig addGroup(final EngineConfig engine, final String name) throws Exception
    {
        final int group_id = next_group_id;
        InfluxDBGroupConfig group = ((InfluxDBEngineConfig) engine).addGroup(group_id, name, null);
        if (group != null)
            next_group_id++;
        return group;
    }

    /** {@inheritDoc} */
    @Override
    public GroupConfig[] getGroups(final EngineConfig engine) throws Exception
    {
        final InfluxDBEngineConfig influxdb_engine = (InfluxDBEngineConfig) engine;
        return influxdb_engine.getGroupsArray();
    }

    /** @param channel_name Name of a channel
     *  @return {@link GroupConfig} for that channel or <code>null</code>
     *  @throws Exception on error
     */
    public InfluxDBGroupConfig searchChannelGroup(final String channel_name) throws Exception
    {
        for (EngineConfig engine : engines_id2obj.values())
        {
            for (GroupConfig group : ((InfluxDBEngineConfig)engine).getGroupObjs())
            {
                if (((InfluxDBGroupConfig)group).containsChannel(channel_name))
                    return ((InfluxDBGroupConfig)group);
            }
        }
        return null;
    }

    /** Set a group's enabling channel
     *  @param group Group that should enable based on a channel
     *  @param channel Channel or <code>null</code> to 'always' activate the group
     *  @throws Exception on error
     */
    public void setEnablingChannel(final InfluxDBGroupConfig group, final InfluxDBChannelConfig channel) throws Exception
    {
        group.setEnablingChannel(channel);
    }

    /** Add a channel.
     *
     *  <p>The channel might already exist in the InfluxDB, but maybe it is not attached
     *  to a sample engine's group, or it's attached to a different group.
     *
     *  @param group {@link InfluxDBGroupConfig} to which to add the channel
     *  @param channel_name Name of channel
     *  @param mode Sample mode
     *  @return {@link InfluxDBChannelConfig}
     *  @throws Exception on error
     */
    public InfluxDBChannelConfig addChannel(final InfluxDBGroupConfig group, final String channel_name, final InfluxDBSampleMode mode) throws Exception
    {
        final int channel_id = next_channel_id;
        InfluxDBChannelConfig channel = group.addChannel(channel_id, channel_name, mode, null);
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
        final InfluxDBGroupConfig influxdb_group = (InfluxDBGroupConfig) group;

        if (skip_last)
        {
            return influxdb_group.getChannelArray();
        }

        final ChannelConfig[] old_channels = influxdb_group.getChannelArray();

        for (ChannelConfig channel : old_channels)
        {
            final Instant last_sample_time = InfluxDBResults.getTimestamp(influxQuery.get_newest_channel_samples(channel.getName(), null, null, 1L));
            if (last_sample_time == null)
            {
                Activator.getLogger().log(Level.WARNING, "Failed to get last sample time for channel " + channel.getName());
            }
            else if (!last_sample_time.equals(channel.getLastSampleTime()))
            {
                influxdb_group.updateChannelLastTime(channel.getName(), last_sample_time);
            }
        }
        return influxdb_group.getChannelArray();
    }

    //    /** @param channel_id Channel ID in config
    //     *  @return Name of channel
    //     *  @throws Exception on error
    //     */
    //    private String getChannelName(final int channel_id) throws Exception
    //    {
    //        try
    //        (
    //                final PreparedStatement statement =
    //                influxdb.getConnection().prepareStatement(sql.channel_sel_by_id);
    //                )
    //        {
    //            statement.setInt(1, channel_id);
    //            final ResultSet result = statement.executeQuery();
    //            if (! result.next())
    //                throw new Exception("Invalid channel ID " + channel_id);
    //            final String name = result.getString(1);
    //            result.close();
    //            return name;
    //        }
    //    }

    /** {@inheritDoc} */
    @Override
    public void close()
    {
        influxdb.close();
    }
}
