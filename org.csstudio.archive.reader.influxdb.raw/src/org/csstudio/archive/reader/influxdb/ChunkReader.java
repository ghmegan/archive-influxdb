/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/

package org.csstudio.archive.reader.influxdb.raw;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.csstudio.archive.influxdb.InfluxDBResults;
import org.csstudio.archive.influxdb.InfluxDBUtil;
import org.csstudio.archive.influxdb.MetaTypes;
import org.csstudio.archive.influxdb.MetaTypes.MetaObject;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;

/** Manager of chunked data coming from both sample and metadata queries simultaneously
 *  @author Megan Grodowitz
 */

public class ChunkReader extends InfluxDBSampleDecoder
{
    /** Queue of result chunks of the sample query */
    final BlockingQueue<QueryResult> sample_queue;

    /** Queue of result chunks of the metadata query */
    final BlockingQueue<QueryResult> metadata_queue;

    /** Expected last timestamp for samples */
    final Instant last_sample_time;

    /** Current sample timestamp or null for none */
    Instant cur_sample_time;

    /** Expected last timestamp for metadata */
    final Instant last_metadata_time;

    /** Max time to wait for a chunk of data to arrive */
    final int timeout_secs;

    /** Column labels of current series of samples to process */
    public String[] cur_sample_columns;

    /** Map of label strings to column index */
    final protected Map<String, Integer> cur_column_map = new HashMap<String, Integer>();

    /** Values of current sample series */
    public List<Object> cur_sample_values;

    /** Current meta data to process and next metadata if such exists */
    public MetaObject cur_meta, next_meta;

    /** Remaining values of current series of samples to process */
    final protected Queue<List<Object>> next_sample_values = new LinkedList<List<Object>>();

    /** Remaining sample_series in the current sample chunk */
    final protected Queue<Series> next_sample_series = new LinkedList<Series>();

    /** Remaining metadata in the current metadata chunk */
    final protected Queue<MetaObject> next_metadata = new LinkedList<MetaObject>();

    private int step_count;

    private int recv_vals;

    ChunkReader(final BlockingQueue<QueryResult> sample_queue, final Instant last_sample_time,
            final BlockingQueue<QueryResult> metadata_queue, final Instant last_metadata_time,
            final int timeout_secs)
    {
        this.sample_queue = sample_queue;
        this.metadata_queue = metadata_queue;
        this.timeout_secs = timeout_secs;

        this.last_metadata_time = last_metadata_time;
        this.last_sample_time = last_sample_time;
        this.cur_sample_time = Instant.MIN;

        this.cur_sample_columns = new String[1];
        this.cur_sample_values = null;
        this.cur_meta = null;
        this.next_meta = null;

        this.step_count = 0;
        this.recv_vals = 0;
    }

    private boolean poll_next_sample_series() throws Exception
    {
        Series next_series = next_sample_series.poll();

        while (next_series == null)
        {
            try
            {
                Activator.getLogger().log(Level.FINER, "Polling for next chunk of samples");
                final QueryResult results = sample_queue.poll(timeout_secs, TimeUnit.SECONDS);
                //Activator.getLogger().log(Level.FINEST, () -> "Got sample chunk : " + InfluxDBResults.toString(results) );
                next_sample_series.addAll(InfluxDBResults.getSeries(results));
            }
            catch (Exception e)
            {
                return false;
            }
            next_series = next_sample_series.poll();
        }

        //        if (!next_series.getName().equals(parent.channel_name))
        //        {
        //            throw new Exception("Got series result with name " + next_series.getName() + ", expected channel name " + parent.channel_name);
        //        }

        int col_count = InfluxDBResults.getColumnCount(next_series);
        int val_count = InfluxDBResults.getValueCount(next_series);

        recv_vals += val_count;
        Activator.getLogger().log(Level.FINE, "Polled for next series of samples (cols = {0}, vals = {1}, total vals = {2})", new Object[] {col_count, val_count, recv_vals});


        if ((col_count < 1) || (val_count < 1))
            return poll_next_sample_series();

        if (col_count != cur_sample_columns.length)
            cur_sample_columns = new String[col_count];

        cur_sample_columns = next_series.getColumns().toArray(cur_sample_columns);
        cur_column_map.clear();
        int i = 0;
        for (String col : cur_sample_columns)
        {
            cur_column_map.put(col, i);
            i++;
        }

        next_sample_values.addAll(next_series.getValues());

        return true;
    }

    private void step_next_metadata() throws Exception
    {
        next_meta = next_metadata.poll();
        while (next_meta == null)
        {
            try
            {
                final QueryResult results = metadata_queue.poll(timeout_secs, TimeUnit.SECONDS);
                //Activator.getLogger().log(Level.FINEST, () -> "Got metadata chunk " + InfluxDBResults.toString(results) );
                next_metadata.addAll(MetaTypes.toMetaObjects(results));
            }
            catch (Exception e)
            {
                throw new Exception ("failed to poll metadata queue for next metadata results ", e);
            }
            next_meta = next_metadata.poll();
        }
        Activator.getLogger().log(Level.FINER, () -> "Stepped next metadata " + next_meta.toString());
    }

    private void update_meta() throws Exception
    {
        if (cur_meta == null)
        {
            try
            {
                step_next_metadata();
            }
            catch (Exception e)
            {
                throw new Exception ("Could not set initial metadata object", e);
            }

            cur_meta = next_meta;
            Activator.getLogger().log(Level.FINE, "Set current metadata {0}, last timestamp is {1}", new Object[] {cur_meta, last_metadata_time});

            if (cur_meta.timestamp.isBefore(last_metadata_time))
            {
                // There should be more metadata, because we haven't hit the end timestamp
                step_next_metadata();
            }
            else {
                next_meta = null;
                return;
            }
        }

        if (!cur_meta.timestamp.isBefore(last_metadata_time))
            return;

        // This is the last metadata value for this sample range
        if (next_meta == null)
            return;

        // Update to next metadata until we run out or the sample is the same or later timestamp
        // while (current sample is the same time as or after the next metadata)
        while (!cur_sample_time.isBefore(next_meta.timestamp))
        {
            cur_meta = next_meta;
            Activator.getLogger().log(Level.FINE, "Set current metadata {0}, last timestamp is {1}", new Object[] {cur_meta, last_metadata_time});

            if (cur_meta.timestamp.isBefore(last_metadata_time))
            {
                //We expect more metadata
                step_next_metadata();
            }
            else
            {
                next_meta = null;
                return;
            }
        }
    }

    public boolean step() throws Exception
    {
        // if the current sample is the same time as or after the end time stamp, we are done
        if (!cur_sample_time.isBefore(last_sample_time))
        {
            return false;
        }

        if (next_sample_values.isEmpty())
        {
            if (!poll_next_sample_series())
            {
                Activator.getLogger().log(Level.WARNING, () -> "Unable to poll next set of sample results. Possible timeout? Vals recieved = " + recv_vals + ", Step count = "
                        + step_count + ", last sample time = " + last_sample_time + " (" + InfluxDBUtil.toNanoLong(last_sample_time)
                        + ") cur sample time " + cur_sample_time + " (" + InfluxDBUtil.toNanoLong(cur_sample_time) + ")");
                return false;
            }
        }

        final List<Object> vals = next_sample_values.poll();
        if (vals.size() != cur_sample_columns.length)
        {
            throw new Exception ("Sample result encountered with wrong number of values != " + cur_sample_columns.length + ": " + vals);
        }

        cur_sample_values = vals;
        cur_sample_time = InfluxDBUtil.fromInfluxDBTimeFormat(this.getValue("time"));
        update_meta();

        Activator.getLogger().log(Level.FINER, () -> "sample step success: " + this.toString());
        step_count++;
        return true;
    }

    public boolean containsColumn(String key)
    {
        return cur_column_map.containsKey(key);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(cur_meta.toString()).append("\n");
        sb.append(InfluxDBResults.makeSeriesTable(Arrays.asList(cur_sample_columns), Arrays.asList(cur_sample_values)));
        return sb.toString();
    }

    @Override
    public Object getValue(final String colname) throws Exception
    {
        Integer idx = cur_column_map.get(colname);
        if (idx == null)
        {
            throw new Exception ("Tried to access sample value in nonexistant column " + colname);
        }
        return cur_sample_values.get(idx);
    }

    @Override
    public boolean hasValue(final String colname)
    {
        return cur_column_map.containsKey(colname);
    }

    @Override
    public MetaObject getMeta()
    {
        return cur_meta;
    }

}
