/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.influxdb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.XMLExport;
import org.csstudio.archive.config.XMLImport;
import org.csstudio.archive.config.influxdb.InfluxDBArchiveConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** JUnit demo of {@link XMLExport} and {@link XMLImport}
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class InfluxDBArchiveExportImportDemo
{
    private InfluxDBArchiveConfig config = null;
    private File tmp_file = null;
    private File input_file = null;
    private String engine_name = null;

    /**
     * Archive configurations for accelerator (snapshot only)
     * ics-srv02.ornl.gov:/usr/local/css/archive_configs/
     *
     * Beamlines (e.g. Mandi)
     * bl11b-dassrv1.sns.gov:/home/controls/bl11b/Makefile
     * archive:
        css/build_arch.sh > css/bl11b_arch.xml
     *
     * @throws Exception
     */
    @Before
    public void connect() throws Exception
    {
        //        final TestProperties settings = new TestProperties();
        //        final String url = settings.getString("archive_influxdb_url");
        //        final String user = settings.getString("archive_influxdb_user");
        //        final String password = settings.getString("archive_influxdb_password");
        //        final String filename = settings.getString("tmp_file");
        //        final String engine_name = settings.getString("archive_config");

        //String archive_url = "http://localhost:8086";
        String archive_url = "http://diane.ornl.gov:8086";
        String user = null;
        String password = null;
        tmp_file = File.createTempFile("InfluxDBConfigTest-out", ".xml");
        input_file = new File("../org.csstudio.archive.config.influxdb/xml/demo.xml");
        engine_name = "demo";

        if (archive_url == null || tmp_file == null || input_file == null || engine_name == null)
        {
            System.out.println("Skipping test, missing one of: archive_url, tmp_file, input_file, engine_name");
            config = null;
            return;
        }

        System.out.println("Using temporary file: " + tmp_file.getName());

        if (user == null  ||  password == null)
        {
            System.out.println("Trying connections with no username or password....");
            user = null;
            password = null;
        }

        config = new InfluxDBArchiveConfig(archive_url, user, password);

        assertTrue(input_file.exists());
        final XMLImport importer = new XMLImport(config, true, false);
        final InputStream stream = new FileInputStream(input_file);
        System.out.println("Reading file " + input_file + ", " + input_file.length() + " bytes");

        importer.parse(stream, engine_name, "Demo", "http://localhost:4813");
    }

    @After
    public void close()
    {
        if (config != null)
            config.close();
    }

    /** Export the config to temporary xml
     * @throws Exception
     */
    @Test
    public void testExport() throws Exception
    {
        final String filename = tmp_file.getAbsolutePath();
        if (tmp_file.exists())
            tmp_file.delete();
        assertFalse(tmp_file.exists());
        final PrintStream out = new PrintStream(filename);
        try
        {
            new XMLExport().export(out, config, engine_name);
        }
        finally
        {
            out.close();
        }
        assertTrue(tmp_file.exists());
        System.out.println("Created file " + tmp_file + ", " + tmp_file.length() + " bytes");
    }

    @Test
    public void testEngine() throws Exception
    {
        if (config == null)
            return;
        final EngineConfig engine = config.findEngine(engine_name);
        assertNotNull("Cannot locate engine " + engine_name, engine);
        System.out.println(engine.getName() + ": " + engine.getDescription() +
                " @ " + engine.getURL());
        assertEquals(engine_name, engine.getName());

        final GroupConfig[] groups = config.getGroups(engine);
        for (GroupConfig group : groups)
            System.out.println(group.getName());
        assertTrue(groups.length > 0);

        for (GroupConfig group : groups)
        {
            final ChannelConfig[] channels = config.getChannels(group, false);
            for (ChannelConfig channel : channels)
                System.out.println(group.getName() + " - " + channel.getName() + " " + channel.getSampleMode() +
                        ", last sample time: " + channel.getLastSampleTime());
        }
    }

    //    @Test
    //    public void testDelete() throws Exception
    //    {
    //        try
    //        {
    //            EngineConfig engine = config.findEngine(engine_name);
    //            assertNotNull(engine);
    //            config.deleteEngine(engine);
    //            engine = config.findEngine(engine_name);
    //            assertNull(engine);
    //        }
    //        finally
    //        {
    //            config.close();
    //        }
    //    }


}
