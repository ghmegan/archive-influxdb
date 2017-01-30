/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.influxdb;

import org.csstudio.archive.config.GroupConfig;

/** InfluxDB implementation of {@link GroupConfig}
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class InfluxDBGroupConfig extends GroupConfig
{
    final private int id;

    /** Initialize
     *  @param id
     *  @param name
     *  @param enabling_channel
     */
    public InfluxDBGroupConfig(final int id, final String name, final String enabling_channel)
    {
        super(name, enabling_channel);
        this.id = id;
    }

    /** @return InfluxDB ID of channel group */
    public int getId()
    {
        return id;
    }

    /** @param channel Channel that enables this group */
    void setEnablingChannel(final InfluxDBChannelConfig channel)
    {
        enabling_channel = channel.getName();
    }

    /** @return Debug representation */
    @Override
    public String toString()
    {
        return super.toString() + " (" + id + ")";
    }
}
