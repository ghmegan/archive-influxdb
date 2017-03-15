/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb;

import static org.junit.Assert.assertEquals;

import java.util.Timer;
import java.util.TimerTask;

import org.csstudio.archive.influxdb.InfluxDBUtil.ConnectionInfo;
import org.csstudio.archive.reader.ArchiveInfo;
import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.influxdb.raw.InfluxDBRawReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** JUnit test of the InfluxDBArchiveServer
 *  <p>
 *  Will only work when suitable archived data is available.
 *  @author Megan Grodowitz
 */
@SuppressWarnings("nls")
public class InfluxDBRawReaderTest
{
    private InfluxDBRawReader reader;
    private String dbname;

    @Before
    public void connect() throws Exception
    {
        String archive_url = "http://localhost:8086";
        // String archive_url = "http://diane.ornl.gov:8086";
        String user = null;
        String password = null;

        dbname = "InfluxDBRawReaderTest-DB";

        if (user == null  ||  password == null)
        {
            System.out.println("Trying connections with no username or password....");
            user = null;
            password = null;
        }

        try
        {
            reader = new InfluxDBRawReader(archive_url, user, password, dbname);
            reader.getQueries().initDatabases(reader.getConnectionInfo().influxdb);

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
        {
            try {
                reader.getConnectionInfo().influxdb.deleteDatabase(dbname);
            } catch (Exception e) {
                e.printStackTrace();
            }
            reader.close();
        }
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
        assertEquals("InfluxDB-Raw", reader.getServerName());
        System.out.println(reader.getDescription());
        for (ArchiveInfo arch : reader.getArchiveInfos())
            System.out.println(arch);

        ConnectionInfo ci = reader.getConnectionInfo();
        System.out.println(ci);
        System.out.println("Databases: ");
        for (String db : ci.dbs) {
            System.out.println("\t" + db);
        }
    }

    // /** Locate channels by pattern */
    // @Test
    // public void testChannelByPattern() throws Exception
    // {
    // if (reader == null)
    // return;
    // final String pattern = channel_name.substring(0, channel_name.length()-1)
    // + "?";
    // System.out.println("Channels matching a pattern: " + pattern);
    // final String[] names = reader.getNamesByPattern(1, pattern);
    // for (String name : names)
    // System.out.println(name);
    // assertTrue(names.length > 0);
    // }
    //
    // /** Locate channels by pattern */
    // @Test
    // public void testChannelByRegExp() throws Exception
    // {
    // if (reader == null)
    // return;
    // final String pattern = "." + channel_name.replace("(",
    // "\\(").substring(1, channel_name.length()-3) + ".*";
    // System.out.println("Channels matching a regular expression: " + pattern);
    // final String[] names = reader.getNamesByRegExp(1, pattern);
    // for (String name : names)
    // System.out.println(name);
    // assertTrue(names.length > 0);
    // }
}
