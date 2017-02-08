/*******************************************************************************
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.archive.config.influxdb;

import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;

/** [Headless] Populate a database using config files as the template
 *  @author Megan Grodowitz - InfluxDB version
 */
@SuppressWarnings("nls")
public class PopulateDatabaseApplication implements IApplication
{
    @Override
    public Object start(final IApplicationContext context) throws Exception
    {
        System.out.println("Hello World");
        return IApplication.EXIT_OK;
    }

    @Override
    public void stop()
    {
        // Ignored
    }
}
