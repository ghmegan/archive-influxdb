package org.csstudio.archive.influxdb;

import org.csstudio.archive.vtype.ArchiveVEnum;
import org.csstudio.archive.vtype.ArchiveVNumber;
import org.csstudio.archive.vtype.ArchiveVNumberArray;
import org.csstudio.archive.vtype.ArchiveVString;
import org.diirt.vtype.VDouble;
import org.diirt.vtype.VEnum;
import org.diirt.vtype.VNumber;
import org.diirt.vtype.VNumberArray;
import org.diirt.vtype.VString;
import org.diirt.vtype.VType;

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
}
