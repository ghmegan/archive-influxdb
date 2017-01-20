/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.reader.influxdb;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.csstudio.archive.influxdb.InfluxDBResults;
import org.csstudio.archive.influxdb.InfluxDBUtil;
//import org.csstudio.platform.utility.rdb.RDBUtil.Dialect;
import org.diirt.vtype.VType;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;

/** Value Iterator that reads from the SAMPLE table.
 *  @author Kay Kasemir
 *  @author Megan Grodowitz (InfluxDB)
 */
public class RawSampleIterator extends AbstractInfluxDBValueIterator
{
    /** Queue of result chunks of the sample query */
    final BlockingQueue<QueryResult> sample_queue = new LinkedBlockingQueue<>();

    /** Queue of result chunks of the metadata query */
    final BlockingQueue<QueryResult> metadata_queue = new LinkedBlockingQueue<>();

    /** 'Current' value that <code>next()</code> will return,
     *  or <code>null</code>
     */
    private VType value = null;

    private final ChunkReader samples;

    static final private int sample_chunk_size = 10;
    static final private int metadata_chunk_size = 2;

    /** Initialize
     *  @param reader RDBArchiveReader
     *  @param channel_name ID of channel
     *  @param start Start time
     *  @param end End time
     *  @throws Exception on error
     */
    public RawSampleIterator(final InfluxDBArchiveReader reader,
            final String channel_name, final Instant start,
            final Instant end) throws Exception
    {
        super(reader, channel_name);
        Instant sample_endtime, metadata_endtime;
        QueryResult results = null;

        try
        {
            //Find the last timestamp of the sample in this timerange
            results = reader.getQueries().get_newest_meta_data(channel_name, null, end, 1L);
        }
        catch (Exception e)
        {
            throw new Exception ("Error getting last metadata in timerange ", e);
        }

        Logger logger = Activator.getLogger();
        logger.log(Level.WARNING, "Results from metadata query: {0}", InfluxDBResults.toString(results));

        try
        {
            final Series series0 = results.getResults().get(0).getSeries().get(0);
            final String ts = (String) InfluxDBResults.getValue(series0, "time", 0);
            metadata_endtime = InfluxDBUtil.fromInfluxDBTimeFormat(ts);
        }
        catch (Exception e)
        {
            throw new Exception("Could not get timestamp of any metadata for " + channel_name + " before time " + end, e);
        }

        try
        {
            results = reader.getQueries().get_newest_channel_samples(channel_name, start, end, 1L);
        }
        catch (Exception e)
        {
            throw new Exception ("Error getting last metadata in timerange ", e);
        }

        Activator.getLogger().log(Level.WARNING, "Results from sample query: {0}", InfluxDBResults.toString(results));
        if (InfluxDBResults.getValueCount(results) < 1)
        {
            samples = null;
            close();
            return;
        }

        try
        {
            final Series series0 = results.getResults().get(0).getSeries().get(0);
            final String ts = (String) InfluxDBResults.getValue(series0, "time", 0);
            sample_endtime = InfluxDBUtil.fromInfluxDBTimeFormat(ts);
        }
        catch (Exception e)
        {
            throw new Exception ("Error getting last sample in timerange ", e);
        }


        reader.getQueries().chunk_get_channel_samples(sample_chunk_size, channel_name, start, end, null,
                new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result) {
                sample_queue.add(result);
            }});

        reader.getQueries().chunk_get_channel_metadata(metadata_chunk_size, channel_name, start, end, null,
                new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result) {
                metadata_queue.add(result);
            }});

        samples = new ChunkReader(sample_queue, sample_endtime, metadata_queue, metadata_endtime, reader.getTimeout());

        if (samples.step())
            value = samples.decodeSampleValue();
        else
            close();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext()
    {
        return value != null;
    }


    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("nls")
    public VType next() throws Exception
    {
        // This should not happen...
        if (value == null)
            throw new Exception("RawSampleIterator.next(" + channel_name + ") called after end");

        // Remember value to return...
        final VType result = value;

        // ... and prepare next value
        if (samples.step())
            value = samples.decodeSampleValue();
        else
            close();

        return result;
    }

    /** Release all database resources.
     *  OK to call more than once.
     */
    @Override
    public void close()
    {
        super.close();
        value = null;
    }
}
