package org.csstudio.archive.config.influxdb.generate;

import org.csstudio.archive.influxdb.MetaTypes.StoreAs;

public class ChannelGenerator {

    public static class ChanInfo {
        public final String name;
        public final double period;
        protected final StoreAs storeas;

        public ChanInfo(final String name, final double period, final StoreAs storeas) {
            this.name = name;
            this.period = period;
            this.storeas = storeas;
        }

        public static ChanInfo makeDouble(final String name, final double period) {
            return new ChanInfo(name, period, StoreAs.ARCHIVE_DOUBLE);
        }
    }

}
