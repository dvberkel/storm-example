package org.effrafax.storm.sprout;

import static org.effrafax.storm.Measure.MILLISECONDS;

import java.util.Map;

import org.effrafax.storm.Measure;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordSprout extends BaseRichSpout {
	private static final long serialVersionUID = 37L;
	private SpoutOutputCollector collector;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		collector.emit(new Values("Alice"));
		wait(100, MILLISECONDS);
	}

	private void wait(int amount, Measure measure) {
		try {
			Thread.sleep(measure.times(amount));
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
