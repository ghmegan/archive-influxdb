/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb;

import org.influxdb.InfluxDB;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.csstudio.archive.influxdb.InfluxDBRead;
import org.csstudio.archive.influxdb.InfluxDBUtil.ConnectionInfo;
import org.csstudio.archive.reader.ArchiveInfo;
import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.UnknownChannelException;
import org.csstudio.archive.reader.ValueIterator;
//import org.csstudio.platform.utility.rdb.RDBUtil.Dialect;
import org.diirt.util.time.TimeDuration;

/** ArchiveReader for InfluxDB data
 *  @author Megan Grodowitz
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveReader implements ArchiveReader
{
    //TODO: cleanup.

    final private String url;
    final private String user;
    final private int password;
    /** Timeout [secs] used for some operations that should be 'fast' */
    //final private int timeout;
    //
    final private ConnectionCache.Entry influxdb;
    final private InfluxDBRead influxQuery;

    /** Map of status IDs to Status strings */
    // don't need this for influx, just store the status strings as tags
    //final private HashMap<Integer, String> stati;

    /** Map of severity IDs to Severities */
    // don't need this for influx, just store severity strings as tags
    //final private HashMap<Integer, AlarmSeverity> severities;

    private boolean concurrency = false;

    /** Initialize
     *  @param url Database URL
     *  @param user .. user
     *  @param password .. password
     *  @param schema .. schema (including ".") or ""
     *  @param stored_procedure Stored procedure or "" for client-side optimization
     *  @throws Exception on error
     */
    public InfluxDBArchiveReader(final String url)
            throws Exception
    {
        this(url, null, null);
    }

    /** Initialize
     *  @param url Database URL
     *  @param user .. user
     *  @param password .. password
     *  @param schema .. schema (including ".") or ""
     *  @param stored_procedure Stored procedure or "" for client-side optimization
     *  @param use_array_blob Use BLOB for array elements?
     *  @throws Exception on error
     */
    public InfluxDBArchiveReader(final String url, final String user, final String password)
            throws Exception
    {
        this.url = url;
        this.user = user;
        this.password = (password == null) ? 0 : password.length();
        //TODO: Set timeouts? Other optimization?
        //timeout = RDBArchivePreferences.getSQLTimeoutSecs();
        influxdb = ConnectionCache.get(url, user, password);
        influxQuery = new InfluxDBRead("*");

        //stati = getStatusValues();
        //severities = getSeverityValues();
    }

    //    /** @return Map of all status ID/Text mappings
    //     *  @throws Exception on error
    //     */
    //    private HashMap<Integer, String> getStatusValues() throws Exception
    //    {
    //        final HashMap<Integer, String> stati = new HashMap<Integer, String>();
    //        try
    //        (
    //                final Statement statement = rdb.getConnection().createStatement();
    //                )
    //        {
    //            if (timeout > 0)
    //                statement.setQueryTimeout(timeout);
    //            statement.setFetchSize(100);
    //            final ResultSet result = statement.executeQuery(sql.sel_stati);
    //            while (result.next())
    //                stati.put(result.getInt(1), result.getString(2));
    //            return stati;
    //        }
    //    }

    //    /** @return Map of all severity ID/AlarmSeverity mappings
    //     *  @throws Exception on error
    //     */
    //    private HashMap<Integer, AlarmSeverity> getSeverityValues() throws Exception
    //    {
    //        final HashMap<Integer, AlarmSeverity> severities = new HashMap<Integer, AlarmSeverity>();
    //        try
    //        (
    //                final Statement statement = rdb.getConnection().createStatement();
    //                )
    //        {
    //            if (timeout > 0)
    //                statement.setQueryTimeout(timeout);
    //            statement.setFetchSize(100);
    //            final ResultSet result = statement.executeQuery(sql.sel_severities);
    //            while (result.next())
    //            {
    //                final int id = result.getInt(1);
    //                final String text = result.getString(2);
    //                AlarmSeverity severity = null;
    //                for (AlarmSeverity s : AlarmSeverity.values())
    //                {
    //                    if (text.startsWith(s.name()))
    //                    {
    //                        severity = s;
    //                        break;
    //                    }
    //                    if    ("OK".equalsIgnoreCase(text) || "".equalsIgnoreCase(text))
    //                    {
    //                        severity = AlarmSeverity.NONE;
    //                        break;
    //                    }
    //                }
    //                if (severity == null)
    //                {
    //                    Activator.getLogger().log(Level.FINE,
    //                            "Undefined severity level {0}", text);
    //                    severities.put(id, AlarmSeverity.UNDEFINED);
    //                }
    //                else
    //                    severities.put(id, severity);
    //            }
    //            return severities;
    //        }
    //    }

    /** @return InfluxDB connection
     *  @throws Exception on error
     */
    InfluxDB getConnection() throws Exception
    {
        //TODO: No exception is thrown for closed connection. Connection is only checked on initial connect.
        return influxdb.getConnection();
    }

    public ConnectionInfo getConnectionInfo() throws Exception
    {
        return new ConnectionInfo(influxdb.getConnection());
    }

    /** @return Query statements */
    InfluxDBRead getQuery()
    {
        return influxQuery;
    }

    //    /** @param status_id Numeric status ID
    //     *  @return Status string for ID
    //     */
    //    String getStatus(int status_id)
    //    {
    //        final String status = stati.get(status_id);
    //        if (status == null)
    //            return "<" + status_id + ">";
    //        return status;
    //    }
    //
    //    /** @param severity_id Numeric severity ID
    //     *  @return ISeverity for ID
    //     */
    //    AlarmSeverity getSeverity(int severity_id)
    //    {
    //        final AlarmSeverity severity = severities.get(severity_id);
    //        if (severity != null)
    //            return severity;
    //        Activator.getLogger().log(Level.WARNING, "Undefined alarm severity ID {0}", severity_id);
    //        severities.put(severity_id, AlarmSeverity.UNDEFINED);
    //        return AlarmSeverity.UNDEFINED;
    //    }

    /** {@inheritDoc} */
    @Override
    public String getServerName()
    {
        return "InfluxDB";
    }

    /** {@inheritDoc} */
    @Override
    public String getURL()
    {
        return url;
    }

    /** {@inheritDoc} */
    @Override
    public String getDescription()
    {
        return "InfluxDB Archive V" + getVersion() + "\n" +
                "User: " + user + "\n" +
                "Password: " + password + " characters";
    }

    /** {@inheritDoc} */
    @Override
    public int getVersion()
    {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public ArchiveInfo[] getArchiveInfos()
    {
        return new ArchiveInfo[]
                {
                        new ArchiveInfo("influxdb", "", 1)
                };
    }

    private static String escape_specials(final String specials, String pattern)
    {
        for (char c : specials.toCharArray())
        {
            pattern = pattern.replace(String.valueOf(c), "\\" + c);
        }
        return pattern;
    }

    /** {@inheritDoc} */
    @Override
    public String[] getNamesByPattern(final int key, final String glob_pattern) throws Exception
    {
        // Escape regex special chars
        String rx_pattern = glob_pattern.replace("\\", "\\\\");
        rx_pattern = escape_specials(".^$", rx_pattern);

        rx_pattern = rx_pattern.replace("*", ".*");
        rx_pattern = rx_pattern.replace('?', '.');

        return perform_search(rx_pattern);
    }

    /** {@inheritDoc} */
    @Override
    public String[] getNamesByRegExp(final int key, final String reg_exp) throws Exception
    {
        return perform_search(reg_exp);
    }

    /** Perform channel search by name pattern
     *  @param pattern Pattern, either SQL or Reg. Ex.
     *  @param sql_query SQL query that can handle the pattern
     *  @return Channel names
     *  @throws Exception on error
     */
    private String[] perform_search(final String pattern) throws Exception
    {
        final ArrayList<String> names = new ArrayList<String>();
        //        final PreparedStatement statement = rdb.getConnection().prepareStatement(sql_query);
        //        addForCancellation(statement);
        //        try
        //        {
        //            statement.setString(1, pattern);
        //            final ResultSet result = statement.executeQuery();
        //            while (result.next())
        //                names.add(result.getString(1));
        //        }
        //        catch (Exception ex)
        //        {
        //            if (ex.getMessage().startsWith("ORA-01013") || ex.getMessage().startsWith("ERROR: canceling statement due to user request"))
        //            {
        //                // Ignore Oracle/PostgreSQL error: user requested cancel of current operation
        //            }
        //            else
        //                throw ex;
        //        }
        //        finally
        //        {
        //            removeFromCancellation(statement);
        //            statement.close();
        //        }
        return names.toArray(new String[names.size()]);
    }

    /** {@inheritDoc} */
    @Override
    public ValueIterator getRawValues(final int key, final String name,
            final Instant start, final Instant end) throws UnknownChannelException, Exception
    {
        final int channel_id = getChannelID(name);
        return getRawValues(channel_id, start, end);
    }

    /** Fetch raw samples
     *  @param channel_id Channel ID in RDB
     *  @param start Start time
     *  @param end End time
     *  @return {@link ValueIterator} for raw samples
     *  @throws Exception on error
     */
    public ValueIterator getRawValues(final int channel_id,
            final Instant start, final Instant end) throws Exception
    {
        return new RawSampleIterator(this, channel_id, start, end, concurrency);
    }

    /** {@inheritDoc} */
    @Override
    public ValueIterator getOptimizedValues(final int key, final String name,
            final Instant start, final Instant end, int count) throws UnknownChannelException, Exception
    {
        // MySQL version of the stored proc. requires count > 1
        if (count <= 1)
            throw new Exception("Count must be > 1");
        final int channel_id = getChannelID(name);

        //        // Use stored procedure in RDB server?
        //        if (stored_procedure.length() > 0)
        //            return new StoredProcedureValueIterator(this, stored_procedure, channel_id, start, end, count);
        //
        //        // Else: Determine how many samples there are
        //        final int counted;
        //        try
        //        (
        //                final PreparedStatement count_samples = rdb.getConnection().prepareStatement(
        //                        influxQuery.sample_count_by_id_start_end);
        //                )
        //        {
        //            count_samples.setInt(1, channel_id);
        //            count_samples.setTimestamp(2, TimestampHelper.toSQLTimestamp(start));
        //            count_samples.setTimestamp(3, TimestampHelper.toSQLTimestamp(end));
        //            final ResultSet result = count_samples.executeQuery();
        //            if (! result.next())
        //                throw new Exception("Cannot count samples");
        //            counted = result.getInt(1);
        //        }
        // Fetch raw data and perform averaging
        final ValueIterator raw_data = getRawValues(channel_id, start, end);

        //        // If there weren't that many, that's it
        //        if (counted < count)
        //            return raw_data;

        // Else: Perform averaging to reduce sample count
        final double seconds = TimeDuration.toSecondsDouble(Duration.between(start, end)) / count;
        return new AveragedValueIterator(raw_data, seconds);
    }

    /** @param name Channel name
     *  @return Numeric channel ID
     *  @throws UnknownChannelException when channel not known
     *  @throws Exception on error
     */
    // Allow access from 'package' for tests
    int getChannelID(final String name) throws UnknownChannelException, Exception
    {
        //        try
        //        (
        //                final PreparedStatement statement =
        //                rdb.getConnection().prepareStatement(influxQuery.channel_sel_by_name);
        //                )
        //        {
        //            if (timeout > 0)
        //                statement.setQueryTimeout(timeout);
        //            statement.setString(1, name);
        //            final ResultSet result = statement.executeQuery();
        //            if (!result.next())
        //                throw new UnknownChannelException(name);
        //            return result.getInt(1);
        //        }
        return 1;
    }

    //    /** Add a statement to the list of statements-to-cancel in cancel()
    //     *  @param statement Statement to cancel
    //     *  @see #cancel()
    //     */
    //    void addForCancellation(final Statement statement)
    //    {
    //        synchronized (cancellable_statements)
    //        {
    //            cancellable_statements.add(statement);
    //        }
    //    }
    //
    //    /** Remove a statement to the list of statements-to-cancel in cancel()
    //     *  @param statement Statement that should no longer be cancelled
    //     *  @see #cancel()
    //     */
    //    void removeFromCancellation(final Statement statement)
    //    {
    //        synchronized (cancellable_statements)
    //        {
    //            cancellable_statements.remove(statement);
    //        }
    //    }

    /** Check if an exception indicates Oracle operation was canceled,
     *  i.e. this program requested the operation to abort
     *  @param ex Exception (Throwable) to test
     *  @return <code>true</code> if it looks like the result of cancellation.
     */
    public static boolean isCancellation(final Throwable ex)
    {
        //        final String message = ex.getMessage();
        //        if (message == null)
        //            return false;
        //        if (message.startsWith(ORACLE_CANCELLATION))
        //            return true;
        //        if (message.startsWith(ORACLE_RECURSIVE_ERROR))
        //        {
        //            final Throwable cause = ex.getCause();
        //            if (cause != null)
        //                return isCancellation(cause);
        //        }
        return false;
    }

    /** Cancel an ongoing RDB query.
     *  Not supported by all queries.
     */
    @Override
    public void cancel()
    {
        //        synchronized (cancellable_statements)
        //        {
        //            for (Statement statement : cancellable_statements)
        //            {
        //                try
        //                {
        //                    // Note that
        //                    //    statement.getConnection().close()
        //                    // does NOT stop an ongoing Oracle query!
        //                    // Only this seems to do it:
        //                    statement.cancel();
        //                }
        //                catch (Exception ex)
        //                {
        //                    Logger.getLogger(Activator.ID).log(Level.WARNING,
        //                            "Attempt to cancel statement", ex); //$NON-NLS-1$
        //                }
        //            }
        //        }
    }

    /** {@inheritDoc} */
    @Override
    public void close()
    {
        cancel();
        //        try
        //        {
        //            Connection connection = rdb.getConnection();
        //            if (connection != null && connection.getAutoCommit() == false)
        //            {
        //                connection.rollback();
        //                connection.setAutoCommit(true);
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Activator.getLogger().log(Level.WARNING,
        //                    "Attempt to cleanup connection failed with Exception", ex); //$NON-NLS-1$
        //        }
        ConnectionCache.release(influxdb);
    }

    @Override
    public void enableConcurrency(boolean concurrency) {
        this.concurrency  = concurrency;
    }
}
