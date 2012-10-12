package twitter.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterDataExtractor extends BaseBasicBolt{
	static JSONParser jsonParser = new JSONParser();
	Map<String, Integer> hashtags = new HashMap<String, Integer>();
	private Jedis jedis;
	
	@Override
	public void cleanup() {
		
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String json = (String)input.getValueByField("tweet");
		try {
			JSONObject jsonObject = (JSONObject) jsonParser.parse(json);
			if(jsonObject.containsKey("entities")){
				JSONObject entities = (JSONObject) jsonObject.get("entities");
				if(entities.containsKey("hashtags")){
					for(Object obj : (JSONArray)entities.get("hashtags")){
						JSONObject hashObj = (JSONObject) obj;
						collector.emit(new Values(input.getString(0),hashObj.get("text").toString()));
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		String redisHost = (String) stormConf.get("redisHost");
		Integer redisPort = ((Long) stormConf.get("redisPort")).intValue();
		this.jedis = new Jedis(redisHost, redisPort);
		this.jedis.connect();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("trackList","hashtag"));
	}

}
