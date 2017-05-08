package org.csstudio.archive.config.influxdb.generate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.xml.XMLArchiveConfig;
import org.csstudio.archive.config.xml.XMLChannelConfig;
import org.csstudio.archive.config.xml.XMLEngineConfig;
import org.csstudio.archive.config.xml.XMLGroupConfig;
import org.csstudio.archive.influxdb.Activator;
import org.csstudio.archive.influxdb.MetaTypes.StoreAs;
import org.diirt.util.text.NumberFormats;
import org.diirt.vtype.Display;
import org.diirt.vtype.ValueFactory;

public class ChannelGenerator {

    final Display display = ValueFactory.newDisplay(0.0, 1.0, 2.0, "a.u.", NumberFormats.format(2), 8.0, 9.0, 10.0, 0.0,
            10.0);

    public static class ChanInfo {
        public final String name;
        protected final StoreAs storeas;

        public ChanInfo(final String name, final StoreAs storeas) {
            this.name = name;
            this.storeas = storeas;
        }

        @Override
        public String toString() {
            return name + ":" + storeas.toString();
        }
    }

    public static class TickSet implements Comparable<TickSet> {
        public final double period;
        public double next_tick;
        public final List<ChanInfo> chans;

        public TickSet(final double period) {
            this.period = period;
            chans = new ArrayList<ChanInfo>();
        }

        @Override
        public int compareTo(TickSet o) {
            return (next_tick < o.next_tick) ? -1 : ((next_tick == o.next_tick) ? 0 : 1);
        }

        public void addChanDouble(final String name) {
            chans.add(new ChanInfo(name, StoreAs.ARCHIVE_DOUBLE));
        }

        @Override
        public String toString() {
            return "Tickset[" + period + "], " + chans.size() + " Channels, Next=" + next_tick;
        }
    }

    protected TickSet[] ticks;

    public ChannelGenerator(XMLArchiveConfig config, boolean skipPVSample) throws Exception {

        Map<Double, TickSet> ticksets = new HashMap<Double, TickSet>();

        for (EngineConfig engine : config.getEngines()) {
            XMLEngineConfig the_engine = (XMLEngineConfig) engine;
            for (GroupConfig group : the_engine.getGroupsArray()) {
                XMLGroupConfig the_group = (XMLGroupConfig) group;
                for (ChannelConfig chan : the_group.getChannelArray()) {
                    XMLChannelConfig the_chan = (XMLChannelConfig) chan;

                    Double period = the_chan.getSampleMode().getPeriod();
                    if (period == 0) {
                        Activator.getLogger().log(Level.WARNING,
                                "Got bad period (0) for PV " + the_chan.getName() + " set to default of 1");
                        period = 1.0;
                    }

                    if (!ticksets.containsKey(period)) {
                        ticksets.put(period, new TickSet(period));
                    }
                    final TickSet tset = ticksets.get(period);

                    if (skipPVSample) {
                        tset.addChanDouble(the_chan.getName());
                    } else {
                        // TODO: determine channel type from PV access
                        tset.addChanDouble(the_chan.getName());
                    }
                }
            }
        }

        ticks = new TickSet[ticksets.size()];
        ticks = ticksets.values().toArray(ticks);
        Arrays.sort(ticks);
    }

    private void printState() {
        for (TickSet ts : ticks) {
            System.out.println(ts);
            for (ChanInfo ci : ts.chans) {
                System.out.println("\t" + ci);
            }
        }
    }

    public void step() {
        // printState();

        ticks[0].next_tick += ticks[0].period;
        Arrays.sort(ticks);
    }

}
