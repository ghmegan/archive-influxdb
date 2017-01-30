/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.influxdb;

import org.csstudio.archive.config.SampleMode;

/** Sample mode as stored in InfluxDB
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class InfluxDBSampleMode extends SampleMode
{
    final private int id;

    /** Initialize
     *  @param id InfluxDB ID
     *  @param monitor Monitor/subscription or active scan?
     *  @param delta Value change threshold
     *  @param period (Expected) update period in seconds
     */
    public InfluxDBSampleMode(final int id, final boolean monitor, final double delta, final double period)
    {
        super(monitor, delta, period);
        this.id = id;
    }

    /** @param name Scan mode name
     *  @return <code>true</code> if it's the 'Monitor' mode
     */
    public static boolean determineMonitor(final String name)
    {
        return "Monitor".equalsIgnoreCase(name);
    }

    /** @return InfluxDB ID */
    public int getId()
    {
        return id;
    }
}
