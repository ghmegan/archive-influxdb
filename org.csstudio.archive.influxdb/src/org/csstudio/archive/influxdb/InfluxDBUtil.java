package org.csstudio.archive.influxdb;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxDBUtil
{

    private static final BigInteger nanomult = new BigInteger("1000000000");

    public static BigInteger toNano(Instant time)
    {
        BigInteger ret = BigInteger.valueOf(time.getEpochSecond());
        ret = ret.multiply(nanomult);
        ret = ret.add(BigInteger.valueOf(time.getNano()));
        return ret;
    }

    public static long toNanoLong(Instant time)
    {
        final BigInteger ret = toNano(time);
        try
        {
            return ret.longValueExact();
        }
        catch (Exception e)
        {
            Activator.getLogger().log(Level.WARNING, "Could not convert instant to long time stamp!", e);
        }
        return ret.longValue();
    }

    /**
     * convert a unix epoch time to timestamp used by influxdb.
     * this can then be used in query expressions against influxdb's time column like so:
     * influxDB.query(new Query("SELECT * FROM some_measurement WHERE time >= '"
     *                          + toInfluxDBTimeFormat(timeStart) + "'", some_database))
     * influxdb time format example: 2016-10-31T06:52:20.020Z
     *
     * @param time timestamp to use, in unix epoch time
     * @return influxdb compatible date-tome string
     */
    public static String toInfluxDBTimeFormat(final Instant time) {
        return DateTimeFormatter.ISO_INSTANT.format(ZonedDateTime.ofInstant(time, ZoneId.of("UTC").normalized()));
    }

    public static Instant fromInfluxDBTimeFormat(final String timestamp)
    {
        return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(timestamp));
    }

    public static Instant fromInfluxDBTimeFormat(final Object timestamp) throws Exception
    {
        if (timestamp == null)
            throw new Exception ("Cannot convert null to instant timestamp");

        if (timestamp instanceof String)
            return fromInfluxDBTimeFormat((String)timestamp);

        throw new Exception ("Cannot convert nonstring object to instant : " + timestamp.getClass().getName());
    }

    public static String getDataDBName(final String channel_name)
    {
        return InfluxDBArchivePreferences.DBNAME;
    }

    public static String getMetaDBName(final String channel_name)
    {
        return InfluxDBArchivePreferences.METADBNAME;
    }

    public static InfluxDB connect(final String url, final String user, final String password) throws Exception
    {
        Activator.getLogger().log(Level.FINE, "Connecting to {0}", url);
        InfluxDB influxdb;
        if (user == null || password == null)
        {
            influxdb = InfluxDBFactory.connect(url);
        }
        else {
            influxdb = InfluxDBFactory.connect(url, user, password);
        }

        try
        {
            // Have to do something like this because connect fails silently.
            influxdb.version();
        }
        catch (Exception e)
        {
            throw new Exception("Failed to connect to InfluxDB as user " + user + " at " + url, e);
        }
        return influxdb;
    }

    public static class ConnectionInfo
    {
        final public String version;
        final public List<String> dbs;
        final public InfluxDB influxdb;

        public ConnectionInfo(InfluxDB influxdb) throws Exception
        {
            this.influxdb = influxdb;
            try
            {
                version = influxdb.version();
                dbs = influxdb.describeDatabases();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to get info for connection. Maybe disconnected?", e);
            }
        }

        @Override
        public String toString()
        {
            return "InfluxDB connection version " + version + " [" + dbs.size() + " databases]";
        }

    };

    public static byte[] toByteArray(double value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static double toDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    public static String bytesToHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

}
