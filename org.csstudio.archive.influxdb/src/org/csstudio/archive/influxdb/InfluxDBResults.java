package org.csstudio.archive.influxdb;

import java.util.ArrayList;
import java.util.List;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import org.apache.commons.lang3.StringUtils;

public class InfluxDBResults
{
    public static int getResultCount(QueryResult results)
    {
        if ( results.hasError() ) {
            return -1;
        }
        if ( results.getResults() == null )
        {
            return 0;
        }
        return results.getResults().size();
    }

    public static int getSeriesCount(Result result)
    {
        if ( result.hasError() )
        {
            return -1;
        }
        if ( result.getSeries() == null )
        {
            return 0;
        }
        return result.getSeries().size();
    }

    public static int getTagCount(Series series)
    {
        if (series.getTags() == null)
        {
            return 0;
        }
        return series.getTags().size();
    }

    public static int getColumnCount(Series series)
    {
        if (series.getColumns() == null)
        {
            return 0;
        }
        return series.getColumns().size();
    }

    public static int getValueCount(Series series)
    {
        if (series.getValues() == null)
        {
            return 0;
        }
        return series.getValues().size();
    }

    public static int getValueCount(QueryResult results)
    {
        int ret = 0;
        if (getResultCount(results) > 0)
        {
            for (Result result : results.getResults() )
            {
                if (getSeriesCount(result) > 0)
                {
                    for (Series series : result.getSeries())
                    {
                        ret += getValueCount(series);
                    }
                }
            }
        }
        return ret;
    }

    public static TableBuilder makeSeriesTable(Series series, int col_count, int val_count)
    {
        TableBuilder tb = new TableBuilder();
        ArrayList<String> r0 = new ArrayList<String>();
        ArrayList<String> r1 = new ArrayList<String>();

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
                for (Object val : vals)
                {
                    r0.add(val.toString());
                    //r1.add(val.getClass().getName());
                }
                tb.addRow(r0.toArray());
                //tb.addRow(r1.toArray());
            }
        }

        return tb;
    }

    public static String toString(QueryResult results)
    {
        StringBuilder buf = new StringBuilder();

        if ( results.hasError() ) {
            buf.append( "Result error: ").append(results.getError());
            return buf.toString();
        }

        int result_count = getResultCount(results);
        buf.append("Total Results = ").append(result_count);

        if (result_count > 0)
        {
            result_count = 0;

            for ( Result result : results.getResults() )
            {
                result_count++;
                int series_count = getSeriesCount(result);

                buf.append("  Result ").append(result_count).append(": Total Series = ").append(series_count);
                if (series_count > 0)
                {
                    series_count = 0;

                    for ( Series series : result.getSeries() )
                    {
                        series_count++;
                        int tag_count = getTagCount(series);
                        int col_count = getColumnCount(series);
                        int val_count = getValueCount(series);

                        buf.append("    Series ").append(series_count)
                        .append(": name[").append(series.getName())
                        .append("] (").append(tag_count).append(" tags) (")
                        .append(col_count).append(" cols) (")
                        .append(val_count).append(" vals)");

                        if (tag_count > 0)
                        {
                            for ( String key : series.getTags().keySet() )
                            {
                                buf.append("    tag[").append(key).append(":").append(series.getTags().get(key)).append("]");
                            }
                        }

                        buf.append("    +-");
                        final TableBuilder tb  = makeSeriesTable(series, col_count, val_count);
                        tb.setIndent("    ");
                        buf.append(tb.toString());
                    }
                }
            }
        }
        return buf.toString();
    }
}