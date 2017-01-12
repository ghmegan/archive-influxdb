package org.csstudio.archive.influxdb;

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
        return influxdb.query(new Query(stmt, dbName));
    }

    public static QueryResult get_oldest_channel_point(final InfluxDB influxdb, final String channel_name)
    {
        return makeQuery(influxdb, "SELECT * from " + channel_name + " LIMIT 1", InfluxDBUtil.getDataDBName(channel_name));
    }

    public QueryResult get_oldest_channel_point(final String channel_name)
    {
        return get_oldest_channel_point(influxdb, channel_name);
    }

    public static QueryResult get_newest_channel_points(final InfluxDB influxdb, final String channel_name, int num)
    {
        return makeQuery(influxdb, "SELECT * from " + channel_name + " ORDER BY time DESC LIMIT " + num, InfluxDBUtil.getDataDBName(channel_name));
    }

    public QueryResult get_newest_channel_points(final String channel_name, int num)
    {
        return get_newest_channel_points(influxdb, channel_name, num);
    }
}
