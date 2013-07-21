package org.effrafax.storm;

import static org.effrafax.storm.Measure.SECONDS;

import org.effrafax.storm.bolt.RecordWord;
import org.effrafax.storm.sprout.WordSprout;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormInAGlassOfWaterTest {
	private static final String TOPOLOGY_NAME = "storm in a glass of water";

	private Config configuration;
	private TopologyBuilder builder;

	@Before
	public void createConfiguration() {
		configuration = new Config();
		configuration.setDebug(true);
		configuration.setNumWorkers(2);
	}
	
	@Before
	public void createTopologyBuilder() {
		builder = new TopologyBuilder();
		builder.setSpout("words", new WordSprout(), 1);
		builder.setBolt("record word", new RecordWord(), 1)
			.shuffleGrouping("words");
	}
	
	@Test
	public void runStormInLocalMode() throws InterruptedException {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, configuration, builder.createTopology());
		wait(10, SECONDS);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

	private void wait(int amount, Measure measure) throws InterruptedException {
		Thread.sleep(measure.times(amount));
	}

}
