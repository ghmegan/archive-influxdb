/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.influxdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.csstudio.archive.config.ArchiveConfig;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.SampleMode;
import org.csstudio.archive.influxdb.InfluxDBArchivePreferences;
import org.csstudio.archive.vtype.TimestampHelper;
//import org.csstudio.platform.utility.influxdb.InfluxDBUtil;
//import org.csstudio.platform.utility.influxdb.InfluxDBUtil.Dialect;

/** InfluxDB implementation (Oracle, MySQL, PostgreSQL) of {@link ArchiveConfig}
 *
 *  <p>Provides read access via {@link ArchiveConfig} API,
 *  and also allows write access via additional InfluxDB-only methods.
 *
 *  @author Kay Kasemir
 *  @author Lana Abadie - Disable autocommit as needed.
 *  @author Takashi Nakamoto - Added an option to skip reading last sampled time.
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveConfig implements ArchiveConfig
{
    //    /** InfluxDB connection */
    //    private InfluxDBUtil influxdb;
    //
    //    /** SQL statements */
    //    private SQL sql;

    //    /** Numeric ID of 'monitor' mode stored in InfluxDB */
    //    private int monitor_mode_id = -1;
    //
    //    /** Numeric ID of 'scan' mode stored in InfluxDB */
    //    private int scan_mode_id = -1;

    //    /** Re-used statement for selecting time of last archived sample of a channel */
    //    private PreparedStatement last_sample_time_statement;

    /** Configured engines */
    //final private List<EngineConfig> engines = new ArrayList<EngineConfig>();
    //final private Map<String, Integer> engine_idx = new HashMap<String, Integer>();
    final private Map<Integer, EngineConfig> engines_id2obj = new HashMap<Integer, EngineConfig>();
    final private Map<String, Integer> engines_name2id = new HashMap<String, Integer>();

    private int next_engine_id;

    private int next_group_id;

    private int next_channel_id;

    /** Initialize.
     *  This constructor will be invoked when an {@link ArchiveConfig}
     *  is created via the extension point.
     *  @throws Exception on error, for example InfluxDB connection error
     */
    public InfluxDBArchiveConfig() throws Exception
    {
        next_group_id = 100;
        next_engine_id = 100;
        next_channel_id = 100;
        //        this(InfluxDBArchivePreferences.getURL(), InfluxDBArchivePreferences.getUser(),
        //                InfluxDBArchivePreferences.getPassword(), InfluxDBArchivePreferences.getSchema());
    }

    //    /** Initialize.
    //     *  This constructor can be invoked by test code.
    //     *  @param url InfluxDB URL
    //     *  @param user .. user name
    //     *  @param password .. password
    //     *  @param schema Schema/table prefix, ending in ".". May be empty
    //     *  @throws Exception on error, for example InfluxDB connection error
    //     */
    //    public InfluxDBArchiveConfig(final String url, final String user, final String password,
    //            final String schema) throws Exception
    //    {
    //        influxdb = InfluxDBUtil.connect(url, user, password, false);
    //        sql = new SQL(influxdb.getDialect(), schema);
    //        loadSampleModes();
    //    }

    /** {@inheritDoc} */
    @Override
    public EngineConfig[] getEngines() throws Exception
    {
        return engines_id2obj.values().toArray(new EngineConfig[engines_id2obj.size()]);
    }

    //    /** Load InfluxDB information about sample modes */
    //    private void loadSampleModes() throws Exception
    //    {
    //        try
    //        (
    //            final Statement statement = influxdb.getConnection().createStatement();
    //            final ResultSet result = statement.executeQuery(sql.sample_mode_sel);
    //        )
    //        {
    //            while (result.next())
    //            {
    //                final String name = result.getString(2);
    //                if (InfluxDBSampleMode.determineMonitor(name))
    //                    monitor_mode_id = result.getInt(1);
    //                else
    //                    scan_mode_id = result.getInt(1);
    //            }
    //        }
    //        if (monitor_mode_id < 0  ||  scan_mode_id < 0)
    //            throw new Exception("Undefined sample modes");
    //    }

    //    /** Determine sample mode
    //     *  @param sample_mode_id Sample mode ID from InfluxDB
    //     *  @param sample_value Sample value, i.e. monitor threshold
    //     *  @param period Scan period, estimated monitor period
    //     *  @return {@link SampleMode}
    //     *  @throws Exception
    //     */
    //    private InfluxDBSampleMode getSampleMode(final int sample_mode_id, final double sample_value, final double period) throws Exception
    //    {
    //        return new InfluxDBSampleMode(sample_mode_id, sample_mode_id == monitor_mode_id, sample_value, period);
    //    }

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

    //    /** @return Next available engine ID */
    //    private int getNextEngineId() throws Exception
    //    {
    //        try
    //        (
    //                final Statement statement = influxdb.getConnection().createStatement();
    //                final ResultSet result = statement.executeQuery(sql.smpl_eng_next_id);
    //                )
    //        {
    //            if (result.next())
    //                return result.getInt(1) + 1;
    //            return 1;
    //        }
    //    }

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
                    + group.getName() + ", engine " + group.getEngineId());
        }
        return engine;
    }

    /** Delete engine info, all the groups under it, and clear all links
     *  from channels to those groups.
     *  @param engine Engine info to remove
     *  @throws Exception on error
     */
    public void deleteEngine(final EngineConfig in_engine) throws Exception
    {
        InfluxDBEngineConfig engine = ((InfluxDBEngineConfig)in_engine);
        final int engine_id = engine.getId();
        final String engine_name = engine.getName();

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
        InfluxDBChannelConfig channel = group.addChannel(channel_id, channel_name, mode);
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
        final List<ChannelConfig> channels = new ArrayList<ChannelConfig>();
        try
        (
                final PreparedStatement statement =
                influxdb.getConnection().prepareStatement(sql.channel_sel_by_group_id);
                )
        {
            statement.setInt(1, influxdb_group.getGroupId());
            final ResultSet result = statement.executeQuery();
            while (result.next())
            {   // channel_id, name, smpl_mode_id, smpl_val, smpl_per
                final int id = result.getInt(1);
                final SampleMode sample_mode =
                        getSampleMode(result.getInt(3), result.getDouble(4), result.getDouble(5));
                Instant last_sample_time = null;
                if (!skip_last)
                    last_sample_time = getLastSampleTime(id);
                channels.add(new InfluxDBChannelConfig(id, result.getString(2),
                        sample_mode, last_sample_time));
            }
            result.close();
        }

        final ChannelConfig[] chan_arr = channels.toArray(new ChannelConfig[channels.size()]);
        // Sort by channel name in Java.
        // SQL should already give sorted result, but handling of upper/lowercase
        // names seems to differ between Oracle and MySQL, resulting in
        // files that were hard to compare
        Arrays.sort(chan_arr, new Comparator<ChannelConfig>()
        {
            @Override
            public int compare(final ChannelConfig a, final ChannelConfig b)
            {
                return a.getName().compareTo(b.getName());
            }
        });
        return chan_arr;
    }

    /** @param channel_id Channel ID in InfluxDB
     *  @return Name of channel
     *  @throws Exception on error
     */
    private String getChannelName(final int channel_id) throws Exception
    {
        try
        (
                final PreparedStatement statement =
                influxdb.getConnection().prepareStatement(sql.channel_sel_by_id);
                )
        {
            statement.setInt(1, channel_id);
            final ResultSet result = statement.executeQuery();
            if (! result.next())
                throw new Exception("Invalid channel ID " + channel_id);
            final String name = result.getString(1);
            result.close();
            return name;
        }
    }

    //TODO: Add ability to connect to Database and query last sample time
    //    /** Obtain time stamp of last sample in archive
    //     *  @param channel_id Channel's InfluxDB ID
    //     *  @return Time stamp or <code>null</code> if not in archive, yet
    //     *  @throws Exception on InfluxDB error
    //     */
    //    private Instant getLastSampleTime(final int channel_id) throws Exception
    //    {
    //        // This statement has a surprisingly complex execution plan for partitioned
    //        // Oracle setups, so re-use it
    //        if (last_sample_time_statement == null)
    //            last_sample_time_statement = influxdb.getConnection().prepareStatement(sql.sel_last_sample_time_by_id);
    //        last_sample_time_statement.setInt(1, channel_id);
    //        try
    //        (
    //                final ResultSet result = last_sample_time_statement.executeQuery();
    //                )
    //        {
    //            if (result.next())
    //            {
    //                final Timestamp stamp = result.getTimestamp(1);
    //                if (stamp == null)
    //                    return null;
    //
    //                if (influxdb.getDialect() != Dialect.Oracle)
    //                {
    //                    // For Oracle, the time stamp is indeed the last time.
    //                    // For others, it's only the seconds, not the nanoseconds.
    //                    // Since this time stamp is only used to avoid going back in time,
    //                    // add a second to assert that we are _after_ the last sample
    //                    stamp.setTime(stamp.getTime() + 1000);
    //                }
    //                return TimestampHelper.fromSQLTimestamp(stamp);
    //            }
    //        }
    //        return null;
    //    }

    //    /** {@inheritDoc} */
    //    @Override
    //    public void close()
    //    {
    //        if (last_sample_time_statement != null)
    //        {
    //            try
    //            {
    //                last_sample_time_statement.close();
    //            }
    //            catch (Exception ex)
    //            {
    //                // Ignore, closing down anyway
    //            }
    //            last_sample_time_statement = null;
    //        }
    //        influxdb.close();
    //    }
}
