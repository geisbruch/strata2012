package twitter.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.TimeCacheMap.ExpiredCallback;

public class TwitterHashtagsSaver extends BaseBasicBolt implements ExpiredCallback<String, Integer>{

	Map<String, Integer> hashtags = new HashMap<String, Integer>();
	private Jedis jedis;
	TimeCacheMap<String, Integer> cacheMap;
	@Override
	public void cleanup() {
		
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = input.getStringByField("hashtag");
		Integer hashtagCount = cacheMap.get(hashtag);
		if(hashtagCount == null)
			hashtagCount = 0;
		cacheMap.put(hashtag, hashtagCount + 1);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		String redisHost = (String) stormConf.get("redisHost");
		Integer redisPort = ((Long) stormConf.get("redisPort")).intValue();
		this.jedis = new Jedis(redisHost, redisPort);
		this.jedis.connect();
		cacheMap = new TimeCacheMap<String, Integer>(10, this);
	}


	@Override
	public void expire(String key, Integer val) {
		jedis.hincrBy("hashs", key, val);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
