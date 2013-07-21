package org.effrafax.storm.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RecordWord extends BaseRichBolt {
	private static final long serialVersionUID = 37L;
	private OutputCollector collector;
	private PrintWriter writer;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.writer = createWriterTo(new File("src/test/resources/recorded_words.txt"));
	}

	private PrintWriter createWriterTo(File file)  {
		OutputStream outStream;
		try {
			outStream = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
		OutputStreamWriter out = new OutputStreamWriter(outStream);
		BufferedWriter writer = new BufferedWriter(out);
		return new PrintWriter(writer);
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		writer.format("word: %s\n", word);
		writer.flush();
		collector.emit(input, new Values(word));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void cleanup() {
		super.cleanup();
		writer.close();
	}

	
	
}
