/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.writer.influxdb;

import java.util.List;
import org.csstudio.archive.influxdb.InfluxDBWrite;
import org.influxdb.InfluxDB;

//import org.csstudio.platform.utility.influxdb.RDBUtil;

/** Enumeration Strings for a channel.
 *  <p>
 *  Presented as an array of strings for the enumerated values 0, 1, 2, ...
 *  The case where no enum strings are defined is represented by
 *  <code>null</code> EnumStrings.
 *
 *  @author Kay Kasemir
 */
public class EnumMetaDataHelper
{
    private EnumMetaDataHelper()
    {
        // prevent instantiation
    }

    /** Delete meta data for channel
     *  @param influxdb RDBUtil
     *  @param sql SQL statements
     *  @param channel Channel
     *  @throws Exception on error
     */
    public static void delete(final InfluxDB influxdb, final InfluxDBWrite sql,
            final InfluxDBWriteChannel channel) throws Exception
    {
        //        // Delete any existing entries
        //        final Connection connection = influxdb.getConnection();
        //        final PreparedStatement del = connection.prepareStatement(sql.enum_delete_by_channel);
        //        try
        //        {
        //            del.setInt(1, channel.getId());
        //            del.executeUpdate();
        //        }
        //        finally
        //        {
        //            del.close();
        //        }
    }

    /** Insert meta data for channel into archive
     *  @param influxdb RDBUtil
     *  @param sql SQL statements
     *  @param channel Channel
     *  @param states Enumeration labels
     *  @throws Exception on error
     */
    @SuppressWarnings("nls")
    public static void insert(final InfluxDB influxdb, final InfluxDBWrite sql, final InfluxDBWriteChannel channel,
            final List<String> states) throws Exception
    {
        //        final Connection connection = influxdb.getConnection();
        //        // Define the new ones
        //        final PreparedStatement insert = connection.prepareStatement(sql.enum_insert_channel_num_val);
        //        try
        //        {
        //            for (int i=0; i<states.size(); ++i)
        //            {
        //                insert.setInt(1, channel.getId());
        //                insert.setInt(2, i);
        //                // Oracle doesn't allow empty==null state strings.
        //                String state = states.get(i);
        //                if (state == null  ||  state.length() < 1)
        //                {   // Patch as "<#>"
        //                    state = "<" + i + ">";
        //                    Activator.getLogger().log(Level.WARNING,
        //                            "Channel {0} has undefined state {1}",
        //                            new Object[] { channel.getName(), state });
        //                }
        //                insert.setString(3, state);
        //                insert.addBatch();
        //            }
        //            insert.executeBatch();
        //        }
        //        finally
        //        {
        //            insert.close();
        //        }
    }
}
