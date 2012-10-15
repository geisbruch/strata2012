package twitter.streaming.trident;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class HashTagSplitter extends BaseFunction {
	private static final long serialVersionUID = 4177035756923453986L;
	static JSONParser jsonParser = new JSONParser();
	Map<String, Integer> hashtags = new HashMap<String, Integer>();

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String json = (String) tuple.getValueByField("tweet");
		try {
			JSONObject jsonObject = (JSONObject) jsonParser.parse(json);
			if (jsonObject.containsKey("entities")) {
				JSONObject entities = (JSONObject) jsonObject.get("entities");
				if (entities.containsKey("hashtags")) {
					for (Object obj : (JSONArray) entities.get("hashtags")) {
						JSONObject hashObj = (JSONObject) obj;
						collector.emit(new Values(hashObj.get("text")
								.toString().toLowerCase()));
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
