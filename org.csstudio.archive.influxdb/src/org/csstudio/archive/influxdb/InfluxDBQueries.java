package org.csstudio.archive.influxdb;

import java.time.Instant;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxDBQueries
{
    private final InfluxDB influxdb;

    public InfluxDBQueries(InfluxDB influxdb)
    {
        this.influxdb = influxdb;
    }

    public static QueryResult makeQuery(final InfluxDB influxdb, final String stmt, final String dbName)
    {
        //System.out.println("Query: " + stmt);
        return influxdb.query(new Query(stmt, dbName));
    }


    /**
     * Create a query string to extract points, ordered by time, for a given channel
     *
     * @param channel_name String name of channel
     * @param starttime initial timestamp
     * @param endtime final timestamp
     * @param limit max number of samples to return
     * @return Query string
     *
     * starttime may be null to indicate beginning of time
     * endtime may be null to indicate no end time cutoff
     * limit may be null for no limit, positive for list of oldest points in range, negative for list of newest (most recent) points in range
     */
    public static String get_channel_points(final String select_what, final String channel_name, final Instant starttime, final Instant endtime, final Integer limit)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(select_what).append(" from ").append(channel_name).append(" ORDER BY time");
        if (starttime != null)
        {
            sb.append(" WHERE time >= ").append(InfluxDBUtil.toNano(starttime).toString());
        }
        if (endtime != null)
        {
            sb.append(" WHERE time <= ").append(InfluxDBUtil.toNano(endtime).toString());
        }
        if (limit != null)
        {
            if (limit > 0)
                sb.append(" LIMIT ").append(limit);
            else if (limit < 0)
                sb.append(" DESC LIMIT ").append(-limit);
        }
        return sb.toString();
    }

    public QueryResult get_oldest_channel_sample(final String channel_name)
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, null, 1),
                InfluxDBUtil.getDataDBName(channel_name));
    }

    public QueryResult get_newest_channel_samples(final String channel_name, int num)
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, null, -num),
                InfluxDBUtil.getDataDBName(channel_name));
    }

    public QueryResult get_newest_channel_samples(final String channel_name, final Instant starttime, final Instant endtime, int num)
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, starttime, endtime, -num),
                InfluxDBUtil.getDataDBName(channel_name));
    }

    public static QueryResult get_newest_meta_data(final InfluxDB influxdb, final String channel_name, Instant endtime)
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, endtime, -1),
                InfluxDBUtil.getMetaDBName(channel_name));
    }

    public static QueryResult get_all_meta_data(final InfluxDB influxdb, final String channel_name)
    {
        return makeQuery(
                influxdb,
                get_channel_points("*", channel_name, null, null, -1),
                InfluxDBUtil.getMetaDBName(channel_name));
    }

}
