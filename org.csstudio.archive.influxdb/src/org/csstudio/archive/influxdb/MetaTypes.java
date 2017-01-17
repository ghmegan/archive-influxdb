package org.csstudio.archive.influxdb;

import java.text.NumberFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.csstudio.archive.vtype.ArchiveVEnum;
import org.csstudio.archive.vtype.ArchiveVNumber;
import org.csstudio.archive.vtype.ArchiveVNumberArray;
import org.csstudio.archive.vtype.ArchiveVString;
import org.diirt.util.text.NumberFormats;
import org.diirt.vtype.Display;
import org.diirt.vtype.VDouble;
import org.diirt.vtype.VEnum;
import org.diirt.vtype.VNumber;
import org.diirt.vtype.VNumberArray;
import org.diirt.vtype.VString;
import org.diirt.vtype.VType;
import org.diirt.vtype.ValueFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;

public class MetaTypes
{
    // WARNING: DO NOT CHANGE THESE NAMES
    // Names are used as DB meta data values
    public enum StoreAs {
        ARCHIVE_UNKNOWN (ArchiveVString.class),
        ARCHIVE_STRING (ArchiveVString.class),
        ARCHIVE_ENUM (ArchiveVEnum.class),
        ARCHIVE_DOUBLE (ArchiveVNumber.class),
        ARCHIVE_LONG (ArchiveVNumber.class),
        ARCHIVE_DOUBLE_ARRAY (ArchiveVNumberArray.class),
        ARCHIVE_LONG_ARRAY (ArchiveVNumberArray.class);

        final public Class<?> readclass;

        StoreAs(Class<?> readclass)
        {
            this.readclass = readclass;
        }
    }

    public static class MetaObject {
        public final StoreAs storeas;
        public final Object object;

        MetaObject(Object object, StoreAs storeas)
        {
            this.object = object;
            this.storeas = storeas;
        }
    }

    public static StoreAs writeVtypeAs(VType sample)
    {
        // Start with most likely cases and highest precision: Double, ...
        // Then going down in precision to integers, finally strings...
        if (sample instanceof VDouble)
            return StoreAs.ARCHIVE_DOUBLE;
        else if (sample instanceof VNumber)
        {
            final Number number = ((VNumber)sample).getValue();
            if (number instanceof Double)
                return StoreAs.ARCHIVE_DOUBLE;
            else
                return StoreAs.ARCHIVE_LONG;
        }
        else if (sample instanceof VNumberArray)
        {
            //TODO: Detect arrays of long?
            return StoreAs.ARCHIVE_DOUBLE_ARRAY;
        }
        else if (sample instanceof VEnum)
            return StoreAs.ARCHIVE_ENUM;
        else if (sample instanceof VString)
            return StoreAs.ARCHIVE_STRING;

        return StoreAs.ARCHIVE_UNKNOWN;
    }

    public static StoreAs storedTypeIs(final String str)
    {
        try
        {
            return StoreAs.valueOf(str);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }

    private static Object checkAssign (final Map<String, Object> map, final String key, Class<?> dest, final Object dflt)
    {
        Object src = map.get(key);

        if (src == null) {
            Activator.getLogger().log(Level.WARNING, "No object stored in metadata for key " + key);
            return dest.cast(dflt);
        }

        try
        {
            return dest.cast(src);
        }
        catch (Exception e)
        {
            Activator.getLogger().log(Level.WARNING, "Got object type " + src.getClass().getName() + " could not be cast to " + dest.getName());
            return dest.cast(dflt);
        }
    }

    public static Point toDisplayMetaPoint(final Display meta, final String channel_name,
            final Instant stamp, final StoreAs storeas)
    {
        final NumberFormat format = meta.getFormat();
        int precision = 0;
        if (format != null)
            precision = format.getMinimumFractionDigits();

        String units = meta.getUnits();
        if (units == null  ||  units.length() < 1)
            units = " "; //$NON-NLS-1$

        return Point.measurement(channel_name)
                .time(InfluxDBUtil.toNanoLong(stamp), TimeUnit.NANOSECONDS)
                .tag("datatype", storeas.name())
                .addField("low_disp_rng", meta.getLowerDisplayLimit())
                .addField("high_disp_rng", meta.getUpperDisplayLimit())
                .addField("low_warn_lmt", meta.getLowerWarningLimit())
                .addField("high_warn_lmt", meta.getUpperWarningLimit())
                .addField("low_alarm_lmt", meta.getLowerAlarmLimit())
                .addField("high_alarm_lmt", meta.getUpperAlarmLimit())
                .addField("low_ctrl_lmt", meta.getLowerCtrlLimit())
                .addField("high_ctrl_lmt", meta.getUpperCtrlLimit())
                .addField("precision", precision)
                .addField("units", units)
                .build();
    }

    private static Display mapToDisplay(final Map<String, Object> map) throws Exception
    {
        Double lowerDisplayLimit, lowerAlarmLimit, lowerWarningLimit;
        String units;
        Integer precision;
        Double upperWarningLimit, upperAlarmLimit, upperDisplayLimit;
        Double lowerCtrlLimit, upperCtrlLimit;

        lowerDisplayLimit = (Double) checkAssign(map, "low_disp_rng", Double.class, 0.0);
        upperDisplayLimit = (Double) checkAssign(map, "high_disp_rng", Double.class, 0.0);

        lowerAlarmLimit = (Double) checkAssign(map, "low_alarm_lmt", Double.class, 0.0);
        upperAlarmLimit = (Double) checkAssign(map, "high_alarm_lmt", Double.class, 0.0);

        lowerWarningLimit = (Double) checkAssign(map, "low_warn_lmt", Double.class, 0.0);
        upperWarningLimit = (Double) checkAssign(map, "high_warn_lmt", Double.class, 0.0);

        lowerCtrlLimit = (Double) checkAssign(map, "low_ctrl_lmt", Double.class, 0.0);
        upperCtrlLimit = (Double) checkAssign(map, "high_ctrl_lmt", Double.class, 0.0);

        units = (String) checkAssign(map, "units", String.class, "");
        precision = ((Double) checkAssign(map, "precision", Double.class, 1)).intValue();

        final Display display = ValueFactory.newDisplay(
                lowerDisplayLimit, lowerAlarmLimit, lowerWarningLimit,
                units, NumberFormats.format(precision),
                upperWarningLimit, upperAlarmLimit, upperDisplayLimit,
                lowerCtrlLimit, upperCtrlLimit);

        return display;
    }

    public static Point toEnumMetaPoint(final List<String> enum_states, final String channel_name,
            final Instant stamp, final StoreAs storeas)
    {
        org.influxdb.dto.Point.Builder point;

        point = Point.measurement(channel_name)
                .time(InfluxDBUtil.toNanoLong(stamp), TimeUnit.NANOSECONDS)
                .tag("datatype", storeas.name());

        //handle arrays (Recommended way is lots of fields)
        final int N = enum_states.size();
        for (int i = 0; i < N; i++)
        {
            String fname = "state." + Integer.toString(i);
            point.addField(fname, enum_states.get(i));
        }

        return point.build();
    }

    private static List<String> mapToEnumList(Map<String, Object> map )
    {
        int i = 0;
        String fname = "state." + Integer.toString(i);
        Object obj = map.get(fname);
        List<String> enum_states = new ArrayList<String>();
        while (obj != null)
        {
            if (obj instanceof String)
                enum_states.add((String)obj);
            else
            {
                Activator.getLogger().log(Level.WARNING, "Got non string enum state? " + obj.getClass().getName());
                enum_states.add(obj.toString());
            }
            i++;
            fname = "state." + Integer.toString(i);
            obj = map.get(fname);
        }
        return enum_states;
    }

    public static Point toNullMetaPoint(final String channel_name,
            final Instant stamp, final StoreAs storeas)
    {
        return Point.measurement(channel_name)
                .time(InfluxDBUtil.toNanoLong(stamp), TimeUnit.NANOSECONDS)
                .tag("datatype", storeas.name())
                .addField("null_metadata", true)
                .build();
    }

    private static Object mapToNull(Map<String, Object> map)
    {
        if (map.get("null_metadata") == null)
        {
            Activator.getLogger().log(Level.WARNING, "Expected null_metadata field in results. Not found.");
        }
        return null;
    }

    public static MetaObject toMetaObject(QueryResult results) throws Exception
    {
        if (InfluxDBResults.getValueCount(results) < 1)
        {
            throw new Exception ("Could not extract meta object from Query Results. No values: " + results.toString());
        }

        final Series series0 = results.getResults().get(0).getSeries().get(0);
        List<String> cols = series0.getColumns();
        List<Object> val0 = series0.getValues().get(0);
        Map<String, Object> map = new HashMap<String, Object>();

        final int N = cols.size();
        for (int i = 0; i < N; i++)
        {
            map.put(cols.get(i), val0.get(i));
        }

        final StoreAs storeas = storedTypeIs((String) map.get("datatype"));
        if (storeas == null)
        {
            throw new Exception ("Could not extract meta object from Query Results. Bad/No datatype tag: " + map.get("datatype"));
        }

        switch(storeas)
        {
        case ARCHIVE_DOUBLE :
        case ARCHIVE_LONG :
        case ARCHIVE_DOUBLE_ARRAY :
        case ARCHIVE_LONG_ARRAY :
            return new MetaObject(mapToDisplay(map), storeas);
        case ARCHIVE_ENUM :
            return new MetaObject(mapToEnumList(map), storeas);
        case ARCHIVE_STRING :
        case ARCHIVE_UNKNOWN :
            return new MetaObject(mapToNull(map), storeas);
        default:
            throw new Exception ("Could not extract meta object from Query Results. Unhandled stored type: " + storeas.name());
        }
    }


}
