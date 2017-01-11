package org.csstudio.archive.influxdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import org.apache.commons.lang3.StringUtils;

public class InfluxDBUtil
{
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

    public static class TableBuilder
    {
        List<String[]> rows = new LinkedList<String[]>();
        String indent;

        TableBuilder()
        {
            this.indent = "";
        }

        void setIndent(String indent)
        {
            this.indent = indent;
        }

        public void addRow(String... cols)
        {
            rows.add(cols);
        }

        public void addRow(Object... cols)
        {
            rows.add(Arrays.copyOf(cols, cols.length, String[].class));
        }

        private int[] colWidths()
        {
            int cols = -1;

            for(String[] row : rows)
                cols = Math.max(cols, row.length);

            int[] widths = new int[cols];

            for(String[] row : rows) {
                for(int colNum = 0; colNum < row.length; colNum++) {
                    widths[colNum] =
                            Math.max(
                                    widths[colNum],
                                    StringUtils.length(row[colNum]));
                }
            }

            return widths;
        }

        @Override
        public String toString()
        {
            StringBuilder buf = new StringBuilder();

            int[] colWidths = colWidths();

            for(String[] row : rows) {
                buf.append(indent);
                buf.append("| ");
                for(int colNum = 0; colNum < row.length; colNum++) {
                    buf.append(
                            StringUtils.rightPad(
                                    StringUtils.defaultString(
                                            row[colNum]), colWidths[colNum]));
                    buf.append(" | ");
                }

                buf.append('\n');
            }

            return buf.toString();
        }

    }

    public static TableBuilder makeSeriesTable(Series series, int col_count, int val_count)
    {
        TableBuilder tb = new TableBuilder();
        ArrayList<String> r0 = new ArrayList<String>();
        ArrayList<String> r1 = new ArrayList<String>();
        //ArrayList<String> r2 = new ArrayList<String>();

        if (col_count > 0)
        {
            r0.clear();
            r1.clear();
            for (String col : series.getColumns())
            {
                r0.add(col);
                r1.add(StringUtils.repeat('-', col.length()));
            }
            tb.addRow(r0.toArray());
            tb.addRow(r1.toArray());
        }

        if (val_count > 0)
        {
            for (List<Object> vals : series.getValues())
            {
                r0.clear();
                //r1.clear();
                //r2.clear();
                for (Object val : vals)
                {
                    r0.add(val.toString());
                    r1.add(val.getClass().getName());
                    //r2.add(bytesToHex(toByteArray((Double)val)))
                }
                tb.addRow(r0.toArray());
                //tb.addRow(r1.toArray());
                //tb.addRow(r2.toArray());
            }
        }

        return tb;
    }

    public static void printResult(QueryResult results)
    {
        if ( results.hasError() ) {
            System.out.println( "Results have error: " + results.getError() );
            return;
        }

        System.out.println("Total Results = " + results.getResults().size());
        int result_count = 0;

        for ( Result result : results.getResults() )
        {
            result_count++;
            System.out.println("  Result " + result_count + ": Total Series = " + result.getSeries().size());

            int series_count = 0;
            for ( Series series : result.getSeries() )
            {
                series_count++;
                int tag_count = 0;
                int col_count = 0;
                int val_count = 0;

                // API will return null for tags if there are none. Lame.
                if (series.getTags() != null)
                {
                    tag_count = series.getTags().size();
                }

                // Not really sure if columns or values would return null. I am assuming so since tags would
                if (series.getColumns() != null)
                {
                    col_count = series.getColumns().size();
                }

                if (series.getValues() != null)
                {
                    val_count = series.getValues().size();
                }

                System.out.println("    Series " + series_count + ": name[" + series.getName() + "] (" + tag_count + " tags) (" + col_count + " cols) (" + val_count + " vals)");

                if (tag_count > 0)
                {
                    for ( String key : series.getTags().keySet() )
                    {
                        System.out.println("    tag[" + key + ":" + series.getTags().get(key) + "]");
                    }
                }

                System.out.println("    +-");
                final TableBuilder tb  = makeSeriesTable(series, col_count, val_count);
                tb.setIndent("    ");
                System.out.println(tb.toString());
            }
        }
    }
}
