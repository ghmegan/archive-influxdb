/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.writer.influxdb;

import java.time.Instant;
import java.util.Arrays;

import org.csstudio.archive.vtype.ArchiveVEnum;
import org.csstudio.archive.vtype.ArchiveVNumber;
import org.csstudio.archive.vtype.ArchiveVNumberArray;
import org.csstudio.archive.vtype.ArchiveVString;
import org.csstudio.archive.writer.WriteChannel;
import org.diirt.util.text.NumberFormats;
import org.diirt.vtype.AlarmSeverity;
import org.diirt.vtype.Display;
import org.diirt.vtype.ValueFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.csstudio.archive.influxdb.InfluxDBResults;
//import org.junit.Ignore;
import org.csstudio.archive.influxdb.InfluxDBUtil.ConnectionInfo;

/** JUnit test of the archive writer
 *
 *  <p>Main purpose of these tests is to run in debugger, step-by-step,
 *  so verify if correct DB entries are made.
 *  The sources don't include anything to check the raw DB data.
 *
 *  @author Megan Grodowitz
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveWriterTest
{
    final Display display = ValueFactory.newDisplay(0.0, 1.0, 2.0, "a.u.", NumberFormats.format(2), 8.0, 9.0, 10.0, 0.0, 10.0);
    private InfluxDBArchiveWriter writer = null;
    private String channel_name, array_channel_name;


    @Before
    public void connect() throws Exception
    {
        //        final TestProperties settings = new TestProperties();
        //        final String url = settings.getString("archive_influxdb_url");
        //        final String user = settings.getString("archive_influxdb_user");
        //        final String password = settings.getString("archive_influxdb_password");
        //        name = settings.getString("archive_channel");
        //        array_name = settings.getString("archive_array_channel");

        String archive_url = "http://localhost:8086";
        String user = null;
        String password = null;

        channel_name = "testPV";
        array_channel_name = "testPV_Array";

        if (archive_url == null  ||  channel_name == null)
        {
            System.out.println("Skipping test, missing one of: archive_url, channel_name");
            writer = null;
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
            writer = new InfluxDBArchiveWriter(archive_url, user, password);
        }
        catch (Exception e)
        {
            System.err.println("Could not create archive writer");
            e.printStackTrace();
        }
    }

    @After
    public void close()
    {
        if (writer != null)
            writer.close();
    }

    /** Basic connection */
    @Test
    public void testBasicInfo() throws Exception
    {
        if (writer == null)
            return;

        ConnectionInfo ci = writer.getConnectionInfo();
        System.out.println(ci);
    }

    private WriteChannel getMakeChannel(final String channel_name) throws Exception
    {
        WriteChannel channel;
        try
        {
            channel = writer.getChannel(channel_name);
        }
        catch (Exception e)
        {
            System.out.println("Failed to get channel, trying to make new...");
            try
            {
                channel = writer.makeNewChannel(channel_name);
            }
            catch (Exception e1)
            {
                e.printStackTrace();
                throw new Exception("Failed to find or make new channel " + channel_name, e1);
            }
        }
        return channel;
    }

    private void printSomePoints(final String name)
    {
        System.out.println(InfluxDBResults.toString(writer.getQueries().get_newest_channel_points(name, 4)));
    }

    @Test
    public void testWriteDouble() throws Exception
    {
        if (writer == null)
            return;
        System.out.println("Writing double sample for channel " + channel_name);

        WriteChannel channel = getMakeChannel(channel_name);
        // Write double
        writer.addSample(channel, new ArchiveVNumber(Instant.now(), AlarmSeverity.NONE, "OK", display, 3.14));
        // .. double that could be int
        writer.addSample(channel, new ArchiveVNumber(Instant.now(), AlarmSeverity.NONE, "OK", display, 3.00));
        writer.flush();

        printSomePoints(channel.getName());

    }

    @Test
    public void testWriteDoubleArray() throws Exception
    {
        if (writer == null  ||  array_channel_name == null)
            return;
        System.out.println("Writing double array sample for channel " + array_channel_name);
        final WriteChannel channel = getMakeChannel(array_channel_name);
        writer.addSample(channel, new ArchiveVNumberArray(Instant.now(), AlarmSeverity.NONE, "OK", display,
                3.14, 6.28, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0));
        writer.flush();

        printSomePoints(channel.getName());
    }

    @Test
    public void testWriteLongEnumText() throws Exception
    {
        if (writer == null)
            return;
        final WriteChannel channel = getMakeChannel(channel_name);

        // Enum, sets enumerated meta data
        writer.addSample(channel, new ArchiveVEnum(Instant.now(), AlarmSeverity.MINOR, "OK", Arrays.asList("Zero", "One"), 1));
        writer.flush();

        // Writing string leaves the enumerated meta data untouched
        writer.addSample(channel, new ArchiveVString(Instant.now(), AlarmSeverity.MAJOR, "OK", "Hello"));
        writer.flush();

        // Integer, sets numeric meta data
        writer.addSample(channel, new ArchiveVNumber(Instant.now(), AlarmSeverity.MINOR, "OK", display, 42));
        writer.flush();

        printSomePoints(channel.getName());
    }

    final private static int TEST_DURATION_SECS = 60;
    final private static long FLUSH_COUNT = 500;

    // @Ignore
    @Test
    public void testWriteSpeedDouble() throws Exception
    {
        if (writer == null)
            return;

        System.out.println("Write test: Adding samples to " + channel_name + " for " + TEST_DURATION_SECS + " secs");
        final WriteChannel channel = writer.getChannel(channel_name);

        long count = 0;
        final long start = System.currentTimeMillis();
        final long end = start + TEST_DURATION_SECS*1000L;
        do
        {
            ++count;
            writer.addSample(channel, new ArchiveVNumber(Instant.now(), AlarmSeverity.NONE, "OK", display, 3.11111111));
            if (count % FLUSH_COUNT == 0)
                writer.flush();
        }
        while (System.currentTimeMillis() < end);
        writer.flush();

        System.out.println("Wrote " + count + " samples, i.e. "
                + ((double)count / TEST_DURATION_SECS) + " samples/sec.");
    }
}
