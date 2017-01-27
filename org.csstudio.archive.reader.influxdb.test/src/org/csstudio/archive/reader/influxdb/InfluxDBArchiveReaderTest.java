/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.csstudio.apputil.time.BenchmarkTimer;
import org.csstudio.archive.influxdb.InfluxDBResults;
import org.csstudio.archive.influxdb.InfluxDBUtil;
import org.csstudio.archive.influxdb.InfluxDBUtil.ConnectionInfo;
import org.csstudio.archive.reader.ArchiveInfo;
import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.ValueIterator;
import org.csstudio.archive.vtype.TimestampHelper;
import org.diirt.util.time.TimeDuration;
import org.diirt.vtype.Display;
import org.diirt.vtype.VType;
import org.diirt.vtype.ValueUtil;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.Thread;

/** JUnit test of the RDBArchiveServer
 *  <p>
 *  Will only work when suitable archived data is available.
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveReaderTest
{
    final private static Duration TIMERANGE = Duration.ofDays(10);
    final private static Duration WAVEFORM_TIMERANGE = Duration.ofMinutes(20);

    final private static int BUCKETS = 50;



    @SuppressWarnings("unused")
    final private static SimpleDateFormat parser = new SimpleDateFormat("yyyy/MM/dd");

    private InfluxDBArchiveReader reader;
    private String proc, channel_name, array_channel_name;

    @Before
    public void connect() throws Exception
    {
        //        final TestProperties settings = new TestProperties();
        //        final String url = settings.getString("archive_influxdb_url");
        //        final String user = settings.getString("archive_influxdb_user");
        //        final String password = settings.getString("archive_influxdb_password");
        //        name = settings.getString("archive_channel");
        //        array_name = settings.getString("archive_array_channel");

        //String archive_url = "http://localhost:8086";
        String archive_url = "http://diane.ornl.gov:8086";
        String user = null;
        String password = null;

        channel_name = "testPV";
        array_channel_name = "testPV_Array";

        if (archive_url == null  ||  channel_name == null)
        {
            System.out.println("Skipping test, missing one of: archive_url, channel_name");
            reader = null;
            return;
        }

        if (user == null  ||  password == null)
        {
            System.out.println("Trying connections with no username or password....");
            user = null;
            password = null;
        }

        try
        {
            reader = new InfluxDBArchiveReader(archive_url, user, password);
        }
        catch (Exception e)
        {
            System.err.println("Could not create archive reader");
            e.printStackTrace();
        }
    }

    @After
    public void close()
    {
        if (reader != null)
            reader.close();
    }

    /** Schedule a call to 'cancel()'
     *  @param archive ArchiveReader to cance
     *  @param seconds Seconds until cancellation
     */
    @SuppressWarnings("unused")
    private void scheduleCancellation(final ArchiveReader archive, final double seconds)
    {
        new Timer("CancellationTest").schedule(new TimerTask()
        {
            @Override
            public void run()
            {
                System.out.println("Cancelling ongoing requests!");
                archive.cancel();
            }
        }, 2000);
    }

    /** Basic connection */
    @Test
    public void testBasicInfo() throws Exception
    {
        if (reader == null)
            return;
        assertEquals("InfluxDB", reader.getServerName());
        System.out.println(reader.getDescription());
        for (ArchiveInfo arch : reader.getArchiveInfos())
            System.out.println(arch);

        ConnectionInfo ci = reader.getConnectionInfo();
        System.out.println(ci);
    }

    /** Time/Date formatting */
    @Test
    public void testTimeDateFormat() throws Exception
    {
        //        long millis = System.currentTimeMillis();
        //        long tnano = millis * 1000000 + 555;
        //        Instant stamp = Instant.ofEpochMilli(millis).plusNanos(555);
        //
        //        System.out.println("Timestamp: " + tnano + " = " + stamp + " = " + InfluxDBUtil.toInfluxDBTimeFormat(stamp));

        final Instant time1 = Instant.ofEpochMilli(1484845031986L).plusNanos(1);
        final String timestr1 = "2017-01-19T16:57:11.986000001Z";

        final Instant time2 = Instant.ofEpochMilli(1484845213540L).plusNanos(555);
        final String timestr2 = "2017-01-19T17:00:13.540000555Z";

        assertTrue(InfluxDBUtil.toInfluxDBTimeFormat(time1).equals(timestr1));
        assertTrue(InfluxDBUtil.toInfluxDBTimeFormat(time2).equals(timestr2));

        assertTrue(InfluxDBUtil.fromInfluxDBTimeFormat(timestr1).equals(time1));
        assertTrue(InfluxDBUtil.fromInfluxDBTimeFormat(timestr2).equals(time2));
    }

    /** Locate channels by pattern */
    @Test
    public void testChannelByPattern() throws Exception
    {
        if (reader == null)
            return;
        final String pattern = channel_name.substring(0, channel_name.length()-1) + "?";
        System.out.println("Channels matching a pattern: " + pattern);
        final String[] names = reader.getNamesByPattern(1, pattern);
        for (String name : names)
            System.out.println(name);
        assertTrue(names.length > 0);
    }

    /** Locate channels by pattern */
    @Test
    public void testChannelByRegExp() throws Exception
    {
        if (reader == null)
            return;
        final String pattern = "." + channel_name.replace("(", "\\(").substring(1, channel_name.length()-3) + ".*";
        System.out.println("Channels matching a regular expression: " + pattern);
        final String[] names = reader.getNamesByRegExp(1, pattern);
        for (String name : names)
            System.out.println(name);
        assertTrue(names.length > 0);
    }


    @Test
    public void testChunkQuery() throws Exception
    {
        if (reader == null)
            return;

        Thread.sleep(2000L);

        final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();

        reader.getQueries().chunk_get_channel_samples(2, channel_name, null, null, 10L,
                new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result) {
                queue.add(result);
            }});

        Thread.sleep(2000L);

        QueryResult result = queue.poll(5, TimeUnit.SECONDS);
        while (result != null)
        {
            System.out.println(InfluxDBResults.toString(result));
            result = queue.poll(5, TimeUnit.SECONDS);
        }
    }

    final private static boolean dump = false;
    final private static int max_samples = 10000000;



    /** Get raw data for scalar
     *
     * Results from testing:
     * Server:
     *    - RHEL 7
     *    - 8 core Intel i7-4790 3.6GHz
     *    - 16GB 1600MHz DDR3 RAM
     *    - 500GB 7200 RPM SATA Disk
     *
     *  Client:
     *    - OSX 10.11
     *    - 2.5 GHz Intel Core i7
     *    - 16 GB 1600 MHz DDR3
     *    - 512GB SSD storage
     *
     * Speed was
     *
     * Several different chunking sizes were tried in the raw sample iterator.
     * There was no real difference in speed.
     * Using JProfiler
     * */
    @Test
    public void demoRawDataSpeedTest() throws Exception
    {
        if (reader == null)
            return;
        System.out.println("Raw samples for " + channel_name + ":");
        final Instant end = Instant.now();
        final Instant start = end.minus(TIMERANGE);

        final BenchmarkTimer timer = new BenchmarkTimer();
        final ValueIterator values = reader.getRawValues(0, channel_name, start, end);

        if (dump)
        {
            int count = 0;
            Display display = null;
            while (values.hasNext())
            {
                VType value = values.next();
                System.out.println(value);
                if (display == null)
                    display = ValueUtil.displayOf(value);
                ++count;
                if (count > 10)
                {
                    System.out.println("Skipping rest...");
                    break;
                }
            }
            values.close();
            System.out.println("Meta data: " + display);
        }
        else
        {
            int count = 0;
            while ((values.hasNext()) && (count < max_samples))
            {
                final VType value = values.next();
                // System.out.println(value);
                assertNotNull(value);
                ++count;
            }
            timer.stop();
            /* PostgreSQL 9 Test Results without the System.out in the loop:
             *
             * HP Compact 8000 Elite Small Form Factor,
             * Intel Core Duo, 3GHz, Windows 7, 32 bit,
             * Hitachi Hds721025cla382 250gb Sata 7200rpm
             *
             * No constraints on sample table.
             * 723000 samples in 7.547 seconds
             * 95796.79727732475 samples/sec
             *
             * JProfiler shows that time is spent in ResultSet.next(),
             * which fetches new data according to the 'fetch size'
             * of 100000.
             * But overall about 3 times that much CPU is spent
             * in ResultSet.getTimestamp() because it needs to
             * deal with Calendar
             */
            System.out.println(count + " samples in " + timer);
            System.out.println(count/timer.getSeconds() + " samples/sec");
        }
    }

    /** Get raw data for waveform */
    @Test
    public void testRawWaveformData() throws Exception
    {
        if (reader == null  ||  array_channel_name == null)
            return;
        System.out.println("Raw samples for waveform " + array_channel_name + ":");

        //        if (reader.useArrayBlob())
        //            System.out.println(".. using BLOB");
        //        else
        //            System.out.println(".. using non-BLOB array table");

        final Instant end = Instant.now();
        final Instant start = end.minus(WAVEFORM_TIMERANGE);

        // Cancel after 10 secs
        // scheduleCancellation(reader, 10.0);
        final ValueIterator values = reader.getRawValues(0, array_channel_name, start, end);
        while (values.hasNext())
        {
            final VType value = values.next();
            System.out.println(value);
        }
        values.close();
    }

    /** Get optimized data for scalar, using the client-side {@link AveragedValueIterator} */
    @Test
    public void testJavaOptimizedScalarData() throws Exception
    {
        if (reader == null)
            return;
        System.out.println("Optimized samples for " + channel_name + ":");
        System.out.println("-- Java implementation --");

        final Instant end = Instant.now();
        final Instant start = end.minus(TIMERANGE);

        final ValueIterator raw = reader.getRawValues(0, channel_name, start, end);
        final double seconds = TimeDuration.toSecondsDouble(Duration.between(start, end)) / BUCKETS;
        System.out.println("Time range: "
                + TimestampHelper.format(start) + " ... " + TimestampHelper.format(end)
                + ", " + BUCKETS + " bins, "
                + "i.e. every " + seconds + " seconds");
        final ValueIterator values = new AveragedValueIterator(raw, seconds);
        while (values.hasNext())
        {
            final VType value = values.next();
            System.out.println(value);
        }
        values.close();
    }

    /** Get optimized data for scalar, using the server-side {@link StoredProcedureValueIterator} */
    @Test
    public void testStoredProcedure() throws Exception
    {
        if (reader == null)
            return;
        if (proc.isEmpty())
            System.out.println("No stored procedure available");
        final int channel_id = reader.getChannelID(channel_name);
        System.out.println("Optimized samples for " + channel_name + " (" + channel_id + "):");
        System.out.println("-- Stored procedure --");

        final Instant end = Instant.now();
        final Instant start = end.minus(TIMERANGE);
        final ValueIterator values = new StoredProcedureValueIterator(reader, proc, channel_name, start, end, BUCKETS);
        while (values.hasNext())
        {
            final VType value = values.next();
            System.out.println(value);
        }
        values.close();
    }

    //    /** Directly call the stored procedure */
    //    @Test
    //    @Ignore
    //    public void testSQL() throws Exception
    //    {
    //        final RDBUtil rdb = RDBUtil.connect(URL, USER, PASSWORD, false);
    //        final PreparedStatement statement = rdb.getConnection().prepareStatement(
    //            "SELECT smpl_time, severity_id, status_id, num_val, float_val, str_val" +
    //            " FROM chan_arch.sample" +
    //            " WHERE channel_id=?" +
    //            "   AND smpl_time BETWEEN ? AND ?"
    //            // + "   ORDER BY smpl_time"
    //            );
    //        statement.setInt(1, 58418);
    //        statement.setTimestamp(2, new Timestamp(parser.parse("2009/07/01").getTime()));
    //        statement.setTimestamp(3, new Timestamp(parser.parse("2009/11/01").getTime()));
    //        statement.setFetchSize(100000);
    //        final long bench_start = System.currentTimeMillis();
    //        final ResultSet result = statement.executeQuery();
    //        // - Network sniffer:
    //        // Client -> Oracle: SELECT ....
    //        // Oracle -> Client: Result contains columns time SMPL_TIME, int SEVERITY_ID, ...
    //        // Client -> Oracle: OK, send me the data
    //        // - then a long pause, like 180 seconds.
    //        // - No network traffic, client waits in executeQuery(), until suddenly
    //        // Oracle -> Client: data
    //        // Oracle -> Client: data
    //        // Oracle -> Client: data
    //        // Client -> Oracle: Acknowledge
    //        // Oracle -> Client: data
    //        // Oracle -> Client: data
    //        // Oracle -> Client: data
    //        // Client -> Oracle: Acknowledge
    //        // - about 4000 network packages with data in maybe 4 seconds
    //        // - executeQuery() returns
    //        final long bench_lap1 = System.currentTimeMillis();
    //        // While fetching the rows of data, most calls to next()
    //        // do not cause any network traffic.
    //        // With setFetchSize(10), the default, there is another request for data
    //        // about every 10 rows. With setFetchSize(100000) there is no network
    //        // traffic after about 100000 rows, so maybe that fetch size is too big
    //        // and the JDBC library uses its own idea of how many rows to transfer
    //        // in a network packet.
    //        // These bursts data read over the network triggered by next()
    //        // take almost no time compared to the initial executeQuery(),
    //        // but a bigger setFetchSize() reduces the number of network requests
    //        // and is of course faster.
    //        int count = 0;
    //        Timestamp last = null;
    //        while (result.next())
    //        {
    //            final Timestamp time = result.getTimestamp(1);
    //            if (last != null &&  time.before(last))
    //                System.out.println("Time error!");
    //            last = time;
    //            ++count;
    //        }
    //        final long bench_lap2 = System.currentTimeMillis();
    //        final double secs_query = (bench_lap1 - bench_start) / 1000.0;
    //        final double secs_total = (bench_lap2 - bench_start) / 1000.0;
    //        System.out.println(count + " samples");
    //        System.out.println("Query: " + secs_query);
    //        System.out.println("Total: " + secs_total);
    //        statement.close();
    //        rdb.close();
    //    }
}
