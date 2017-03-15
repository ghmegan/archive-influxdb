/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb.raw;

import java.util.regex.Pattern;

import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.ArchiveReaderFactory;

/** The plugin.xml registers this factory for ArchiveReaders when the
 *  URL prefix indicates an InfluxDB URL
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class InfluxDBRawReaderFactory implements ArchiveReaderFactory
{
    /** {@inheritDoc} */
    @Override
    public ArchiveReader getArchiveReader(final String url) throws Exception
    {
        // Format: influxdb-raw://host:port[|arg1=ARG1|arg2=ARG2...]
        // Valid args:
        // dbname=DBNAME (required) Using influxdb database DBNAME
        // user=USER (optional) login as USER
        // password=PASSWORD (optional) login as USER with PASSWORD

        // There used to be problems with empty user and password preference
        // settings.
        // On CSS startup, a restored Data Browser would launch multiple
        // archive retrieval jobs.
        // The first job's ArchiveReaderFactory call, trying to get the user name,
        // would cause the preference service to read the default preferences.
        // Meanwhile(!) a second archive retrieval job calling the ArchiveReaderFactory
        // would receive an empty user or password.
        // By locking on the plug-in instance, the first ArchiveReaderFactory
        // call will be able to complete the preference initialization
        // before a second instance tries to read preferences.
        // Using the plug-in instance as the lock also asserts that we're
        // running in a plug-in environment that supports preferences in the
        // first place.

        // TODO: other parsing?
        // Activator.getLogger().log(Level.INFO,
        // "Input url for influxdb is currently ignored. Set url with
        // preferences. " + url);

        final Activator instance = Activator.getInstance();
        if (instance == null)
            throw new Exception("InfluxDBArchiveReaderFactory requires Plugin infrastructure");

        String[] strv = url.split(Pattern.quote("|"));

        if (strv.length < 1)
            throw new Exception("Error parsing influxdb url: " + url);

        String[] urlv = strv[0].split(Pattern.quote("://"));

        if (urlv.length < 2)
            throw new Exception("Error parsing influxdb host url: " + strv[0]);

        final String actual_url = "http://"+ urlv[1];

        String user = null;
        String password = null;
        String dbname = null;
        for (int idx = 1; idx < strv.length; idx++)
        {
            String argv[] = strv[idx].split("=");
            if (argv.length < 2)
                throw new Exception("Error parsing InfluxDB raw arg: " + strv[idx]);

            if (argv[0].equals("dbname"))
                dbname = argv[1];
            else if (argv[0].equals("user"))
                user = argv[1];
            else if (argv[0].equals("password"))
                password = argv[1];
            else
                throw new Exception("Bad argument in InfluxDB raw url: " + argv[0]);
        }

        if (dbname == null)
            throw new Exception("Must specify dbname=DBNAME in InfluxDB raw argument list");

        synchronized (instance)
        {
            // final String user = InfluxDBArchivePreferences.getUser();
            // final String password = InfluxDBArchivePreferences.getPassword();
            //final String stored_proc = Preferences.getStoredProcedure();
            //final String actual_url = InfluxDBArchivePreferences.getURL();
            return new InfluxDBRawReader(actual_url, dbname, user, password);
        }
    }
}
