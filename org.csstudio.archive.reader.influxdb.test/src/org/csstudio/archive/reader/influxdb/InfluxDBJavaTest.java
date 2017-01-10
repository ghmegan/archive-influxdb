package org.csstudio.archive.reader.influxdb;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.csstudio.archive.influxdb.InfluxDBUtil;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;

public class InfluxDBJavaTest
{

    public void printInfo(InfluxDB influxdb)
    {
        List<String> dbs = influxdb.describeDatabases();
        String ver = influxdb.version();

        System.out.println("Connected to database version: " + ver);
        System.out.println("Contains " + dbs.size() + " databases: ");

        for (String db : dbs)
        {
            System.out.println("\t" + db);
        }
    }

    /** Basic connection */
    @Test
    public void demoBasicConnect() throws Exception
    {
        InfluxDB influxDB;
        try
        {
            influxDB = InfluxDBFactory.connect("http://localhost:8086");
            String ver = influxDB.version();
        }
        catch (Exception e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            return;
        }
        printInfo(influxDB);

        String dbName = "aTimeSeries";
        influxDB.createDatabase(dbName);

        long tnano = System.currentTimeMillis() * 1000000 + 1;

        double tricky = -Double.MAX_VALUE;
        //double tricky = Double.NaN;
        byte[] trickybytes = InfluxDBUtil.toByteArray(tricky);
        System.out.println("Tricky: " + tricky + " = "+ InfluxDBUtil.bytesToHex(trickybytes));

        BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .tag("async", "true")
                .retentionPolicy("autogen")
                .consistency(ConsistencyLevel.ALL)
                .build();
        Point point1 = Point.measurement("cpu1")
                .time(tnano, TimeUnit.NANOSECONDS)
                .addField("idle", 90L)
                .addField("user", tricky)
                .addField("system", 1L)
                .build();
        Point point2 = Point.measurement("disk")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("used", 80L)
                .addField("free", 1L)
                .build();
        batchPoints.point(point1);
        batchPoints.point(point2);
        System.out.println("Line Protocol for points: " + batchPoints.lineProtocol());

        try
        {
            influxDB.write(batchPoints);
        }
        catch (Exception e)
        {
            System.err.println("Write Failed: " + e.getMessage());
            e.printStackTrace();
        }
        //Query query = new Query("SELECT idle FROM cpu", dbName);
        //Select 3 most recent points
        Query query = new Query("SELECT * FROM cpu1 ORDER BY time DESC LIMIT 2", dbName);
        System.out.println("Sending query: " + query.getCommandWithUrlEncoded());

        QueryResult result = influxDB.query(query);
        //System.out.println(result.toString());
        InfluxDBUtil.printResult(result);
    }



}
