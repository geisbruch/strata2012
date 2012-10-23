package strata.twitter;

import java.util.Map;

import strata.twitter.utils.TwitterReader;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ApiStreamingSpout extends BaseRichBolt{
	private static final long serialVersionUID = 1L;

    SpoutOutputCollector collector;
    TwitterReader reader;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
	}

	@Override
	public void execute(Tuple input) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
