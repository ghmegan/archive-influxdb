/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb.raw;

import org.csstudio.archive.reader.ValueIterator;

/** Base for ValueIterators that read from the InfluxDB
 *  @author Kay Kasemir
 *  @author Megan Grodowitz (InfluxDB)
 */
@SuppressWarnings("nls")
abstract public class AbstractInfluxDBValueIterator  implements ValueIterator
{
    final protected InfluxDBArchiveReader reader;
    final protected String channel_name;

    /** @param reader InfluxDBArchiveReader
     *  @param channel_name ID of channel
     *  @throws Exception on error
     */
    AbstractInfluxDBValueIterator(final InfluxDBArchiveReader reader,
            final String channel_name) throws Exception
    {
        this.reader = reader;
        this.channel_name = channel_name;
    }

    /** Release all database resources.
     *  OK to call more than once.
     */
    @Override
    public void close()
    {
        //TODO: cleanup? No DB connection to close
    }
}
