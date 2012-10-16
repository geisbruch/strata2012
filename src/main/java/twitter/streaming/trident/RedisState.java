package twitter.streaming.trident;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.SnapshottableMap;
import backtype.storm.tuple.Values;

public class RedisState implements IBackingMap<Long> {
	Jedis jedis = new Jedis("localhost");

	public static class Factory implements StateFactory {
		@Override
		public State makeState(Map conf, int partitionIndex, int numPartitions) {
			CachedMap c = new CachedMap(new RedisState(), 1000);
			MapState ms = NonTransactionalMap.build(c);
			return new SnapshottableMap(ms, new Values(""));
		}
	}

	@Override
	public List<Long> multiGet(List<List<Object>> keys) {
		if(keys.size() == 0)
			return new ArrayList<Long>();

		String[] skeys = new String[keys.size()];
		int i = 0;
		for (List<Object> key : keys) {
			skeys[i++] = (String) key.get(0);
		}

		List<Long> longs = new ArrayList<Long>();

		List<String> strs = jedis.hmget("hashs", skeys);

		for (String string : strs) {
			if(string == null)
				longs.add(0L);
			else
				longs.add(Long.parseLong(string));
		}
		return longs;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<Long> vals) {
		Map<String, String> map = new HashMap<String, String>();
		int i = 0;
		
		for (List<Object> key : keys) {
			String sKey = key.get(0).toString();
			String sVal = vals.get(i).toString();
			map.put(sKey, sVal);
			i++;
		}
		jedis.hmset("hashs", map);
	}
}
