/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.writer.influxdb;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.csstudio.archive.influxdb.InfluxDBArchivePreferences;
import org.csstudio.archive.influxdb.InfluxDBQueries;
import org.csstudio.archive.influxdb.InfluxDBResults;
import org.csstudio.archive.influxdb.InfluxDBUtil;
import org.csstudio.archive.influxdb.InfluxDBUtil.ConnectionInfo;
import org.csstudio.archive.vtype.MetaDataHelper;
import org.csstudio.archive.vtype.VTypeHelper;
import org.csstudio.archive.writer.ArchiveWriter;
import org.csstudio.archive.writer.WriteChannel;
//TODO: cleanup
//import org.csstudio.platform.utility.influxdb.RDBUtil;
//import org.csstudio.platform.utility.influxdb.RDBUtil.Dialect;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;
import org.diirt.util.array.ListNumber;
import org.diirt.vtype.AlarmSeverity;
import org.diirt.vtype.Display;
import org.diirt.vtype.VDouble;
import org.diirt.vtype.VEnum;
import org.diirt.vtype.VNumber;
import org.diirt.vtype.VType;

/** ArchiveWriter implementation for RDB
 *  @author Kay Kasemir
 *  @author Lana Abadie - PostgreSQL for original RDBArchive code. Disable autocommit as needed.
 *  @author Laurent Philippe (Use read-only connection when possible for MySQL load balancing)
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveWriter implements ArchiveWriter
{
    /** Status string for <code>Double.NaN</code> samples */
    final private static String NOT_A_NUMBER_STATUS = "NaN";

    //TODO: timeout?
    //final private int SQL_TIMEOUT_SECS = InfluxDBArchivePreferences.getSQLTimeoutSecs();

    //final private int MAX_TEXT_SAMPLE_LENGTH = Preferences.getMaxStringSampleLength();

    /** RDB connection */
    final private InfluxDB influxdb;

    /** SQL statements */
    final private InfluxDBQueries influxQuery;

    /** Cache of channels by name */
    final private Map<String, InfluxDBWriteChannel> channels = new HashMap<String, InfluxDBWriteChannel>();

    static class batchPointSets
    {
        /** Batched points to be written, per database */
        final private Map<String, BatchPoints> dbPoints = new HashMap<String, BatchPoints>();

        public BatchPoints getChannelPoints(final String channel_name)
        {
            final String dbName = InfluxDBUtil.getDBName(channel_name);
            BatchPoints points = dbPoints.get(dbName);
            if (points == null)
            {
                //TODO: set retention and consistency policies
                points = BatchPoints
                        .database(dbName)
                        .retentionPolicy("autogen")
                        .consistency(ConsistencyLevel.ALL)
                        .build();
                dbPoints.put(dbName, points);
            }
            return points;
        }

        public BatchPoints getDBPoints(final String dbName) throws Exception
        {
            BatchPoints points = dbPoints.get(dbName);
            if (points == null)
            {
                throw new Exception("No points stored for DB " + dbName);
            }
            return points;
        }

        public void removeDBPoints(final String dbName)
        {
            dbPoints.remove(dbName);
        }

        public Set<String> getDBNames()
        {
            return dbPoints.keySet();
        }

        public void clear()
        {
            dbPoints.clear();
        }
    };

    final batchPointSets batchSets = new batchPointSets();

    //    /** Severity (ID, name) cache */
    //    private SeverityCache severities;
    //
    //    /** Status (ID, name) cache */
    //    private StatusCache stati;

    /** Initialize from preferences.
     *  This constructor will be invoked when an {@link ArchiveWriter}
     *  is created via the extension point.
     *  @throws Exception on error, for example RDB connection error
     */
    public InfluxDBArchiveWriter() throws Exception
    {
        this(InfluxDBArchivePreferences.getURL(), InfluxDBArchivePreferences.getUser(),
                InfluxDBArchivePreferences.getPassword());
    }

    /** Initialize
     *  @param url RDB URL
     *  @param user .. user name
     *  @param password .. password
     *  @param schema Schema/table prefix, not including ".". May be empty
     *  @param use_array_blob Use BLOB for array elements?
     *  @throws Exception on error, for example RDB connection error
     */
    public InfluxDBArchiveWriter(final String url, final String user, final String password) throws Exception
    {
        influxdb = InfluxDBUtil.connect(url, user, password);
        influxQuery = new InfluxDBQueries(influxdb);
        //        severities = new SeverityCache(influxdb, sql);
        //        stati = new StatusCache(influxdb, sql);

        // JDBC and RDBUtil default to auto-commit being on.
        //
        // The batched submission of samples, however, requires
        // auto-commit to be off, so this code assumes that
        // auto-commit is off, then enables it briefly as needed,
        // and otherwise commits/rolls back.
        //influxdb.getConnection().setAutoCommit(false);
    }

    public ConnectionInfo getConnectionInfo() throws Exception
    {
        return new ConnectionInfo(influxdb);
    }

    public InfluxDBQueries getQueries()
    {
        return influxQuery;
    }

    @Override
    public WriteChannel getChannel(final String name) throws Exception
    {
        // Check cache
        InfluxDBWriteChannel channel = channels.get(name);
        if (channel == null)
        {    // Get channel information from InfluxDB
            QueryResult results = influxQuery.get_oldest_channel_point(name);
            if (InfluxDBResults.getValueCount(results) <= 0)
            {
                throw new Exception("Unknown channel " + name);
            }
            channel = new InfluxDBWriteChannel(name);
            channels.put(name, channel);
        }
        return channel;
    }

    public WriteChannel makeNewChannel(final String name) throws Exception
    {
        // Check cache
        InfluxDBWriteChannel channel = channels.get(name);
        if (channel != null)
        {
            throw new Exception("Channel already exists " + name);
        }

        QueryResult results = influxQuery.get_oldest_channel_point(name);
        if (InfluxDBResults.getValueCount(results) > 0)
        {
            throw new Exception("Channel already exists " + name);
        }
        channel = new InfluxDBWriteChannel(name);
        channels.put(name, channel);

        return channel;
    }

    @Override
    public void addSample(final WriteChannel channel, final VType sample) throws Exception
    {
        final InfluxDBWriteChannel influxdb_channel = (InfluxDBWriteChannel) channel;
        writeMetaData(influxdb_channel, sample);
        batchSample(influxdb_channel, sample);
    }

    /** Write meta data if it was never written or has changed
     *  @param channel Channel for which to write the meta data
     *  @param sample Sample that may have meta data to write
     */
    private void writeMetaData(final InfluxDBWriteChannel channel, final VType sample) throws Exception
    {
        // Note that Strings have no meta data. But we don't know at this point
        // if it's really a string channel, or of this is just a special
        // string value like "disconnected".
        // In order to not delete any existing meta data,
        // we just do nothing for strings

        if (sample instanceof Display)
        {
            final Display display = (Display)sample;
            if (MetaDataHelper.equals(display, channel.getMetadata()))
                return;

            //            // Clear enumerated meta data, replace numeric
            //            EnumMetaDataHelper.delete(influxdb, sql, channel);
            //            NumericMetaDataHelper.delete(influxdb, sql, channel);
            //            NumericMetaDataHelper.insert(influxdb, sql, channel, display);
            //            influxdb.getConnection().commit();
            //            channel.setMetaData(display);
        }
        else if (sample instanceof VEnum)
        {
            final List<String> labels = ((VEnum)sample).getLabels();
            if (MetaDataHelper.equals(labels, channel.getMetadata()))
                return;

            //            // Clear numeric meta data, set enumerated in RDB
            //            NumericMetaDataHelper.delete(influxdb, sql, channel);
            //            EnumMetaDataHelper.delete(influxdb, sql, channel);
            //            EnumMetaDataHelper.insert(influxdb, sql, channel, labels);
            //            influxdb.getConnection().commit();
            //            channel.setMetaData(labels);
        }
    }

    /** Perform 'batched' insert for sample.
     *  <p>Needs eventual flush()
     *  @param channel Channel
     *  @param sample Sample to insert
     *  @throws Exception on error
     */
    private void batchSample(final InfluxDBWriteChannel channel, final VType sample) throws Exception
    {
        final Instant stamp = VTypeHelper.getTimestamp(sample);
        final String severity = VTypeHelper.getSeverity(sample).toString();
        final String status = VTypeHelper.getMessage(sample);

        //        final Timestamp stamp = TimestampHelper.toSQLTimestamp(VTypeHelper.getTimestamp(sample));
        //        final int severity = severities.findOrCreate(VTypeHelper.getSeverity(sample));
        //        final Status status = stati.findOrCreate(VTypeHelper.getMessage(sample));
        //
        //        // Severity/status cache may enable auto-commit
        //        if (influxdb.getConnection().getAutoCommit() == true)
        //            influxdb.getConnection().setAutoCommit(false);
        //
        // Start with most likely cases and highest precision: Double, ...
        // Then going down in precision to integers, finally strings...
        if (sample instanceof VDouble)
            batchDoubleSamples(channel, stamp, severity, status, ((VDouble)sample).getValue(), null);
        else if (sample instanceof VNumber)
        {    // Write as double or integer?
            final Number number = ((VNumber)sample).getValue();
            if (number instanceof Double)
                batchDoubleSamples(channel, stamp, severity, status, number.doubleValue(), null);
            //            else
            //                batchLongSample(channel, stamp, severity, status, number.longValue());
        }
        //        else if (sample instanceof VNumberArray)
        //        {
        //            final ListNumber data = ((VNumberArray)sample).getData();
        //            batchDoubleSamples(channel, stamp, severity, status, data.getDouble(0), data);
        //        }
        //        else if (sample instanceof VEnum)
        //            batchLongSample(channel, stamp, severity, status, ((VEnum)sample).getIndex());
        //        else if (sample instanceof VString)
        //            batchTextSamples(channel, stamp, severity, status, ((VString)sample).getValue());
        //        else // Handle possible other types as strings
        //            batchTextSamples(channel, stamp, severity, status, sample.toString());
        else
        {
            throw new Exception("Cannot handle sample Vtype: " + sample.getClass().getName());
        }
    }



    /** Add 'insert' for double samples to batch, handling arrays
     *  via the original array_val table
     */
    private void batchDoubleSamples(final InfluxDBWriteChannel channel,
            final Instant stamp, final String severity,
            final String status, final double dbl, final ListNumber additional) throws Exception
    {
        Point point;

        //TODO: Catch other number states than NaN (e.g. INF)? Add tags instead of status string?
        if (Double.isNaN(dbl))
        {
            //TODO: nano precision may be lost Long time field (library limitation)
            point = Point.measurement(channel.getName())
                    .time(stamp.getEpochSecond() * 1000000000 + stamp.getNano(), TimeUnit.NANOSECONDS)
                    .tag("severity", AlarmSeverity.UNDEFINED.name())
                    .tag("status", NOT_A_NUMBER_STATUS)
                    .addField("double", 0.0d)
                    .build();
        }
        else
        {
            point = Point.measurement(channel.getName())
                    .time(stamp.getEpochSecond() * 1000000000 + stamp.getNano(), TimeUnit.NANOSECONDS)
                    .tag("severity", severity)
                    .tag("status", status)
                    .addField("double", dbl)
                    .build();
        }

        if (additional != null)
        {
            //TODO: handle arrays
            throw new Exception("batchDoubleSamples: Double array points not handled");
        }

        batchSets.getChannelPoints(channel.getName()).point(point);

        //        if (additional != null)
        //        {
        //            if (insert_array_sample == null)
        //                insert_array_sample =
        //                influxdb.getConnection().prepareStatement(
        //                        sql.sample_insert_double_array_element);
        //            final int N = additional.size();
        //            for (int i = 1; i < N; i++)
        //            {
        //                insert_array_sample.setInt(1, channel.getId());
        //                insert_array_sample.setTimestamp(2, stamp);
        //                insert_array_sample.setInt(3, i);
        //                // Patch NaN.
        //                // Conundrum: Should we set the status/severity to indicate NaN?
        //                // Would be easy if we wrote the main sample with overall
        //                // stat/sevr at the end.
        //                // But we have to write it first to avoid index (key) errors
        //                // with the array sample time stamp....
        //                // Go back and update the main sample after the fact??
        //                if (Double.isNaN(additional.getDouble(i)))
        //                    insert_array_sample.setDouble(4, 0.0);
        //                else
        //                    insert_array_sample.setDouble(4, additional.getDouble(i));
        //                // MySQL nanosecs
        //                if (influxdb.getDialect() == Dialect.MySQL || influxdb.getDialect() == Dialect.PostgreSQL)
        //                    insert_array_sample.setInt(5, stamp.getNanos());
        //                // Batch
        //                insert_array_sample.addBatch();
        //                ++batched_double_array_inserts;
        //            }
        //        }
    }

    //    /** Helper for batchSample: Add long sample to batch.  */
    //    private void batchLongSample(final InfluxDBWriteChannel channel,
    //            final Timestamp stamp, final int severity,
    //            final Status status, final long num) throws Exception
    //    {
    //        if (insert_long_sample == null)
    //        {
    //            insert_long_sample = createInsertPrepareStatement(sql.sample_insert_int);
    //        }
    //        insert_long_sample.setLong(5, num);
    //        completeAndBatchInsert(insert_long_sample, channel, stamp, severity, status);
    //        ++batched_long_inserts;
    //    }
    //
    //    /** Helper for batchSample: Add text sample to batch. */
    //    private void batchTextSamples(final InfluxDBWriteChannel channel,
    //            final Timestamp stamp, final int severity,
    //            final Status status, String txt) throws Exception
    //    {
    //        if (insert_txt_sample == null)
    //        {
    //            insert_txt_sample = createInsertPrepareStatement(sql.sample_insert_string);
    //        }
    //        if (txt.length() > MAX_TEXT_SAMPLE_LENGTH)
    //        {
    //            Activator.getLogger().log(Level.INFO,
    //                    "Value of {0} exceeds {1} chars: {2}",
    //                    new Object[] { channel.getName(), MAX_TEXT_SAMPLE_LENGTH, txt });
    //            txt = txt.substring(0, MAX_TEXT_SAMPLE_LENGTH);
    //        }
    //        insert_txt_sample.setString(5, txt);
    //        completeAndBatchInsert(insert_txt_sample, channel, stamp, severity, status);
    //        ++batched_txt_inserts;
    //    }

    //    /** Helper for batchSample:
    //     *  Set the parameters common to all insert statements, add to batch.
    //     */
    //    private void completeAndBatchInsert(
    //            final PreparedStatement insert_xx, final InfluxDBWriteChannel channel,
    //            final Timestamp stamp, final int severity,
    //            final Status status) throws Exception
    //    {
    //        // Set the stuff that's common to each type
    //        insert_xx.setInt(1, channel.getId());
    //        insert_xx.setTimestamp(2, stamp);
    //        insert_xx.setInt(3, severity);
    //        insert_xx.setInt(4, status.getId());
    //        // MySQL nanosecs
    //        if (influxdb.getDialect() == Dialect.MySQL  ||  influxdb.getDialect() == Dialect.PostgreSQL)
    //            insert_xx.setInt(6, stamp.getNanos());
    //        // Batch
    //        insert_xx.addBatch();
    //    }

    /** {@inheritDoc}
     *  RDB implementation completes pending batches
     */
    @Override
    public void flush() throws Exception
    {
        for (String dbName : batchSets.getDBNames())
        {
            BatchPoints batchPoints = batchSets.getDBPoints(dbName);
            try
            {
                influxdb.write(batchPoints);
            }
            catch (Exception e)
            {
                throw new Exception("Write of points failed " + e.getMessage(), e);
            }
            batchSets.removeDBPoints(dbName);
        }
        batchSets.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void close()
    {
        channels.clear();
        //        if (severities != null)
        //        {
        //            severities.dispose();
        //            severities = null;
        //        }
        //        if (stati != null)
        //        {
        //            stati.dispose();
        //            stati = null;
        //        }
        //
        //        if (insert_double_sample != null) {
        //            try {
        //                insert_double_sample.close();
        //            } catch (SQLException e) {
        //                Activator.getLogger().log(Level.WARNING, "close() error", e);
        //            }
        //            insert_double_sample = null;
        //        }
        //        if (insert_array_sample != null) {
        //            try {
        //                insert_array_sample.close();
        //            } catch (SQLException e) {
        //                Activator.getLogger().log(Level.WARNING, "close() error", e);
        //            }
        //            insert_array_sample = null;
        //        }
        //        if (insert_long_sample != null) {
        //            try {
        //                insert_long_sample.close();
        //            } catch (SQLException e) {
        //                Activator.getLogger().log(Level.WARNING, "close() error", e);
        //            }
        //            insert_long_sample = null;
        //        }
        //        if (insert_txt_sample != null) {
        //            try {
        //                insert_txt_sample.close();
        //            } catch (SQLException e) {
        //                Activator.getLogger().log(Level.WARNING, "close() error", e);
        //            }
        //            insert_txt_sample = null;
        //        }
        influxdb.close();
    }
}
