package org.csstudio.archive.influxdb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxDBQueries
{
    private final InfluxDB influxdb;

    abstract public static class DBNameMap {
        public abstract String getDataDBName(final String channel_name) throws Exception;

        public abstract String getMetaDBName(final String channel_name) throws Exception;

        public abstract List<String> getAllDBNames();
    };

    private final DBNameMap dbnames;

    public static class DefaultDBNameMap extends DBNameMap {
        @Override
        public String getDataDBName(String channel_name) {
            return InfluxDBArchivePreferences.DFLT_DBNAME;
        }

        @Override
        public String getMetaDBName(String channel_name) {
            return InfluxDBArchivePreferences.DFLT_METADBNAME;
        }

        @Override
        public List<String> getAllDBNames() {
            List<String> ret = new ArrayList<String>();
            ret.add(InfluxDBArchivePreferences.DFLT_DBNAME);
            ret.add(InfluxDBArchivePreferences.DFLT_METADBNAME);
            return ret;
        }
    };

    public void initDatabases(final InfluxDB influxdb) {
        for (String db : dbnames.getAllDBNames()) {
            influxdb.createDatabase(db);
        }
    }

    public InfluxDBQueries(InfluxDB influxdb, final DBNameMap dbnames)
    {
        this.influxdb = influxdb;
        if (dbnames == null)
            this.dbnames = new DefaultDBNameMap();
        else
            this.dbnames = dbnames;
    }

    //TODO: timestamps come back with wrong values stored in Double... would be faster if it worked.
    //private final static boolean query_nanos = true;

    public static QueryResult makeQuery(final InfluxDB influxdb, final String stmt, final String dbName)
    {
        Activator.getLogger().log(Level.FINE, "InfluxDB query ({0}): {1}", new Object[] {dbName, stmt});
        //if (query_nanos)
        //    return influxdb.query(new Query(stmt, dbName), TimeUnit.NANOSECONDS);
        return influxdb.query(new Query(stmt, dbName));

    }

    public static void makeChunkQuery(int chunkSize, Consumer<QueryResult> consumer,
            InfluxDB influxdb, String stmt, String dbName) throws Exception
    {
        Activator.getLogger().log(Level.FINE, "InfluxDB chunked ({2}) query ({0}): {1}", new Object[] {dbName, stmt, chunkSize});
        //if (query_nanos)
        //   influxdb.query(new Query(stmt, dbName), TimeUnit.NANOSECONDS, chunkSize, consumer);
        //else
        influxdb.query(new Query(stmt, dbName), chunkSize, consumer);
    }


    private static String get_points(final StringBuilder sb, final Instant starttime, final Instant endtime,
            final Long limit)
    {
        if (starttime != null)
        {
            sb.append(" WHERE time >= ").append(InfluxDBUtil.toNano(starttime).toString());
        }
        if (endtime != null)
        {
            if (starttime == null)
                sb.append(" WHERE");
            else
                sb.append(" AND");
            sb.append(" time <= ").append(InfluxDBUtil.toNano(endtime).toString());
        }
        sb.append(" ORDER BY time ");
        if (limit != null)
        {
            if (limit > 0)
                sb.append(" LIMIT ").append(limit);
            else if (limit < 0)
                sb.append(" DESC LIMIT ").append(-limit);
        }
        return sb.toString();
    }

    /**
     * Create a query string to extract points, ordered by time, for a given
     * channel
     *
     * @param channel_name
     *            String name of channel
     * @param starttime
     *            initial timestamp
     * @param endtime
     *            final timestamp
     * @param limit
     *            max number of samples to return
     * @return Query string
     *
     *         starttime may be null to indicate beginning of time endtime may
     *         be null to indicate no end time cutoff limit may be null for no
     *         limit, positive for list of oldest points in range, negative for
     *         list of newest (most recent) points in range
     */
    public static String get_channel_points(final String select_what, final String channel_name,
            final Instant starttime, final Instant endtime, final Long limit) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(select_what).append(" FROM \"").append(channel_name).append('\"');
        return get_points(sb, starttime, endtime, limit);
    }

    public static String get_pattern_points(final String select_what, final String pattern, final Instant starttime,
            final Instant endtime, final Long limit) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(select_what).append(" FROM /").append(pattern).append('/');
        return get_points(sb, starttime, endtime, limit);
    }

    public QueryResult get_oldest_channel_sample(final String channel_name) throws Exception
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, null, 1L),
                dbnames.getDataDBName(channel_name));
    }

    //    public QueryResult get_newest_channel_samples(final String channel_name, Long num)
    //    {
    //        return makeQuery(
    //                influxdb,
    //                get_channel_points("*", channel_name, null, null, -num),
    // dbnames.getDataDBName(channel_name));
    //    }

    public QueryResult get_newest_channel_samples(final String channel_name, final Instant starttime,
            final Instant endtime, Long num) throws Exception
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, starttime, endtime, -num),
                dbnames.getDataDBName(channel_name));
    }

    public QueryResult get_channel_samples(final String channel_name, final Instant starttime, final Instant endtime,
            Long num) throws Exception
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, starttime, endtime, num),
                dbnames.getDataDBName(channel_name));
    }


    public void chunk_get_channel_samples(final int chunkSize,
            final String channel_name, final Instant starttime, final Instant endtime, Long limit, Consumer<QueryResult> consumer) throws Exception
    {
        makeChunkQuery(
                chunkSize, consumer, influxdb,
                get_channel_points("*", channel_name, starttime, endtime, limit),
                dbnames.getDataDBName(channel_name));
    }

    public QueryResult get_newest_channel_datum_regex(final String pattern) throws Exception {
        return makeQuery(influxdb, get_pattern_points("*", pattern, null, null, -1L), dbnames.getDataDBName(pattern));
    }

    public QueryResult get_newest_meta_data(final String channel_name, final Instant starttime, final Instant endtime,
            Long num) throws Exception
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, starttime, endtime, -num),
                dbnames.getMetaDBName(channel_name));
    }

    public QueryResult get_newest_meta_datum(final String channel_name) throws Exception
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, null, -1L),
                dbnames.getMetaDBName(channel_name));
    }

    public QueryResult get_newest_meta_datum_regex(final String pattern) throws Exception {
        return makeQuery(influxdb, get_pattern_points("*", pattern, null, null, -1L),
                dbnames.getMetaDBName(pattern));
    }

    public QueryResult get_all_meta_data(final String channel_name) throws Exception
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, null, null),
                dbnames.getMetaDBName(channel_name));
    }

    public void chunk_get_channel_metadata(final int chunkSize,
            final String channel_name, final Instant starttime, final Instant endtime, Long limit, Consumer<QueryResult> consumer) throws Exception
    {
        makeChunkQuery(
                chunkSize, consumer, influxdb,
                get_channel_points("*", channel_name, starttime, endtime, limit),
                dbnames.getMetaDBName(channel_name));
    }

}
