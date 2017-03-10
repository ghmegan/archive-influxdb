/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb.raw;

import java.util.logging.Level;

import org.csstudio.archive.influxdb.InfluxDBArchivePreferences;
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
        Activator.getLogger().log(Level.INFO,
                "Input url for influxdb is currently ignored. Set url with preferences. " + url);

        final Activator instance = Activator.getInstance();
        if (instance == null)
            throw new Exception("InfluxDBArchiveReaderFactory requires Plugin infrastructure");
        synchronized (instance)
        {
            final String user = InfluxDBArchivePreferences.getUser();
            final String password = InfluxDBArchivePreferences.getPassword();
            //final String stored_proc = Preferences.getStoredProcedure();
            final String actual_url = InfluxDBArchivePreferences.getURL();
            return new InfluxDBRawReader(actual_url, user, password);
        }
    }
}
