/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.influxdb;

//import org.csstudio.platform.utility.rdb.RDBUtil;
//import org.csstudio.platform.utility.rdb.RDBUtil.Dialect;

/** statements for InfluxDB read archive access
 *  @author Megan Grodowitz
 */
@SuppressWarnings("nls")
public class InfluxDBRead
{
    //TODO: cleanup

    //    // 'status' table
    // Maps status integer to string, not needed in influx, use string tag
    //    final public String sel_stati;
    //
    //    // 'severity' table
    // Maps severity integer to string, not needing in influx, using string tag
    //    final public String sel_severities;
    //
    //    // Meta data tables
    //    final public String numeric_meta_sel_by_channel;
    //    final public String enum_sel_num_val_by_channel;
    //
    //    // 'channel' table
    //    final public String channel_sel_by_like;
    //    final public String channel_sel_by_reg_exp;
    //    final public String channel_sel_by_name;
    //
    //    // 'sample' table
    //    final public String sample_sel_initial_time;
    //    final public String sample_sel_by_id_start_end;
    //    final public String sample_sel_by_id_start_end_with_blob;
    //    final public String sample_sel_array_vals;
    //    final public String sample_count_by_id_start_end;

    // Use this to select all the channels (measurement names) that match a string pattern
    final public String channel_sel_by_like;
    // Use this to select all the channels (measurements names) that match a regular expresssion
    final public String channel_sel_by_reg_exp;

    // Do a query on a channel (measurement) which will give a result that lets you do this:
    //    return ValueFactory.newDisplay(
    //            result.getDouble(1),  // lowerDisplayLimit
    //            result.getDouble(5),  // lowerAlarmLimit
    //            result.getDouble(3),  // lowerWarningLimit
    //            result.getString(8),   // units
    //            format,               // numberFormat
    //            result.getDouble(4),  // upperWarningLimit
    //            result.getDouble(6),  // upperAlarmLimit
    //            result.getDouble(2),  // upperDisplayLimit
    //            result.getDouble(1),  // lowerCtrlLimit
    //            result.getDouble(2)); // upperCtrlLimit
    final public String numeric_meta_sel_by_channel;

    // Get the enumerated values for an enum channel.
    // In oracle this is an enum_metadata table with channel id as key and int/string values for all the possible labels for an enum channel
    // For influx, we can use SHOW TAG VALUES to list all the values of a given tag fr some measurement
    // Just store a junk measurement with a tag for the enum value and store each enum value once in that tag
    final public String enum_sel_num_val_by_channel;

    public InfluxDBRead(String prefix)
    {
        numeric_meta_sel_by_channel = "SELECT low_disp_rng, high_disp_rng," +
                " low_warn_lmt, high_warn_lmt," +
                " low_alarm_lmt, high_alarm_lmt," +
                " prec, unit FROM " + prefix + "num_metadata WHERE channel_id=?";

        enum_sel_num_val_by_channel = "SELECT enum_nbr, enum_val FROM "
                + prefix + "enum_metadata WHERE channel_id=? ORDER BY enum_nbr";

        //        if (dialect == RDBUtil.Dialect.Oracle)
        //        {   // '\\' because Java swallows one '\', be case-insensitive by using all lowercase
        channel_sel_by_like = "SELECT name FROM " + prefix + "channel WHERE LOWER(name) LIKE LOWER(?) ESCAPE '\\' ORDER BY name";
        //            // Use case-insensitive REGEXP_LIKE
        channel_sel_by_reg_exp = "SELECT name FROM " + prefix + "channel WHERE REGEXP_LIKE(name, ?, 'i') ORDER BY name";
        //        }
        //        else
        //        {   // MySQL uses '\' by default, and everything is  by default case-insensitive
        //            channel_sel_by_like = "SELECT name FROM " + prefix + "channel WHERE name LIKE ? ORDER BY name";
        //            if (dialect == RDBUtil.Dialect.PostgreSQL)
        //                channel_sel_by_reg_exp = "SELECT name FROM " + prefix + "channel WHERE name ~* ? ORDER BY name";
        //            else
        //                channel_sel_by_reg_exp = "SELECT name FROM " + prefix + "channel WHERE name REGEXP ? ORDER BY name";
        //        }
        //
        //        channel_sel_by_name = "SELECT channel_id FROM " + prefix + "channel WHERE name=?";
    }

    //    /** Initialize SQL statements
    //     *  @param dialect RDB dialect
    //     *  @param prefix Schema (table) prefix, including "." etc. as needed
    //     */
    //    public InfluxDBRead(final Dialect dialect, String prefix)
    //    {
    //        if (prefix == null  ||  dialect == Dialect.MySQL)
    //            prefix = "";
    //        else
    //            if (prefix.length() > 0   &&   !prefix.endsWith("."))
    //                prefix = prefix + ".";
    //
    //        // 'status' table
    //        sel_stati = "SELECT status_id, name FROM " + prefix + "status";
    //
    //        // 'severity' table
    //        sel_severities = "SELECT severity_id, name FROM " + prefix + "severity";
    //
    //        // Meta data tables
    //        numeric_meta_sel_by_channel = "SELECT low_disp_rng, high_disp_rng," +
    //                " low_warn_lmt, high_warn_lmt," +
    //                " low_alarm_lmt, high_alarm_lmt," +
    //                " prec, unit FROM " + prefix + "num_metadata WHERE channel_id=?";
    //
    //        enum_sel_num_val_by_channel = "SELECT enum_nbr, enum_val FROM "
    //                + prefix + "enum_metadata WHERE channel_id=? ORDER BY enum_nbr";
    //
    //        // 'channel' table
    //        if (dialect == RDBUtil.Dialect.Oracle)
    //        {   // '\\' because Java swallows one '\', be case-insensitive by using all lowercase
    //            channel_sel_by_like = "SELECT name FROM " + prefix + "channel WHERE LOWER(name) LIKE LOWER(?) ESCAPE '\\' ORDER BY name";
    //            // Use case-insensitive REGEXP_LIKE
    //            channel_sel_by_reg_exp = "SELECT name FROM " + prefix + "channel WHERE REGEXP_LIKE(name, ?, 'i') ORDER BY name";
    //        }
    //        else
    //        {   // MySQL uses '\' by default, and everything is  by default case-insensitive
    //            channel_sel_by_like = "SELECT name FROM " + prefix + "channel WHERE name LIKE ? ORDER BY name";
    //            if (dialect == RDBUtil.Dialect.PostgreSQL)
    //                channel_sel_by_reg_exp = "SELECT name FROM " + prefix + "channel WHERE name ~* ? ORDER BY name";
    //            else
    //                channel_sel_by_reg_exp = "SELECT name FROM " + prefix + "channel WHERE name REGEXP ? ORDER BY name";
    //        }
    //
    //        channel_sel_by_name = "SELECT channel_id FROM " + prefix + "channel WHERE name=?";
    //
    //        // 'sample' table
    //        if (dialect == RDBUtil.Dialect.Oracle)
    //        {   // For Oracle, the stored procedure package
    //            // also includes a function for determining
    //            // the initial sample time
    //            sample_sel_initial_time = Preferences.getStarttimeFunction().isEmpty()
    //                    ? "SELECT smpl_time FROM (SELECT smpl_time FROM " +
    //                    prefix + "sample WHERE channel_id=? AND smpl_time<=?" +
    //                    " ORDER BY smpl_time DESC) WHERE ROWNUM=1"
    //                    : Preferences.getStarttimeFunction();
    //            sample_sel_by_id_start_end =
    //                    "SELECT smpl_time, severity_id, status_id, num_val, float_val, str_val FROM " + prefix + "sample"+
    //                            "   WHERE channel_id=?" +
    //                            "     AND smpl_time BETWEEN ? AND ?" +
    //                            "   ORDER BY smpl_time";
    //            sample_sel_by_id_start_end_with_blob =
    //                    "SELECT smpl_time, severity_id, status_id, num_val, float_val, str_val, datatype, array_val" +
    //                            "   FROM " + prefix + "sample" +
    //                            "   WHERE channel_id=?" +
    //                            "     AND smpl_time>=? AND smpl_time<=?" +
    //                            "   ORDER BY smpl_time";
    //            sample_sel_array_vals = "SELECT float_val FROM " + prefix + "array_val" +
    //                    " WHERE channel_id=? AND smpl_time=? ORDER BY seq_nbr";
    //        }
    //        else
    //        {    // MySQL, Postgres
    //            sample_sel_initial_time =
    //                    "SELECT smpl_time, nanosecs" +
    //                            "   FROM " + prefix + "sample WHERE channel_id=? AND smpl_time<=?" +
    //                            "   ORDER BY smpl_time DESC, nanosecs DESC LIMIT 1";
    //            sample_sel_by_id_start_end =
    //                    "SELECT smpl_time, severity_id, status_id, num_val, float_val, str_val, nanosecs FROM " + prefix + "sample" +
    //                            "   WHERE channel_id=?" +
    //                            "     AND smpl_time>=? AND smpl_time<=?" +
    //                            "   ORDER BY smpl_time, nanosecs";
    //            sample_sel_by_id_start_end_with_blob =
    //                    "SELECT smpl_time, severity_id, status_id, num_val, float_val, str_val, nanosecs, datatype, array_val" +
    //                            "   FROM " + prefix + "sample" +
    //                            "   WHERE channel_id=?" +
    //                            "     AND smpl_time>=? AND smpl_time<=?" +
    //                            "   ORDER BY smpl_time, nanosecs";
    //            sample_sel_array_vals = "SELECT float_val FROM " + prefix + "array_val" +
    //                    " WHERE channel_id=? AND smpl_time=? AND nanosecs=? ORDER BY seq_nbr";
    //        }
    //        // Rough count, ignoring nanosecs for the non-Oracle dialects
    //        sample_count_by_id_start_end = "SELECT COUNT(*) FROM " + prefix + "sample" +
    //                "   WHERE channel_id=? AND smpl_time BETWEEN ? AND ?";
    //    }
}
