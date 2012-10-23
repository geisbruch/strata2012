package strata.twitter.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import twitter.streaming.ApiStreamingSpout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;

public class TwitterReader implements Runnable {
	static String STREAMING_API_URL="https://stream.twitter.com/1/statuses/filter.json?track=";
	private String track;
	private String user;
	private String password;
	private DefaultHttpClient client;
	private UsernamePasswordCredentials credentials;
	private BasicCredentialsProvider credentialProvider;
	static Logger LOG = Logger.getLogger(ApiStreamingSpout.class);
	LinkedBlockingQueue<Tweet> tweets = new LinkedBlockingQueue<Tweet>();
	Thread thr = null;
	

	
	public TwitterReader(String track, String user, String password) {
		this.track = track;
		this.user = user;
		this.password = password;
	}
	
	public void start() {
		if(thr == null) {
			thr = new Thread(this, "LoadTweets");
			thr.start();
		} else {
			throw new IllegalStateException("Thread already started");
		}
	}
	
	protected Tweet parseTweet(String json) {
		JSONParser jsonParser = new JSONParser();

		JSONObject jsonObject = (JSONObject) jsonParser.parse(json);
		
		String orginalUser = null;
		if(jsonObject.containsKey("user")) {
			JSONObject userData = (JSONObject) jsonObject.get("user");
			user = userData.get("name").toString();
		}
		
		Tweet ret = new Tweet(user);
		
		if(jsonObject.containsKey("entities")){
			JSONObject entities = (JSONObject) jsonObject.get("entities");
			if(entities.containsKey("hashtags")){
				for(Object obj : (JSONArray)entities.get("hashtags")){
					JSONObject hashObj = (JSONObject) obj;
					
				}
			}
		}
		return null; // TODO implement
	}
	
	@Override
	public void run() {
		/*
		 * Create the client call
		 */
		while(true){
			try{
				client = new DefaultHttpClient();
				client.setCredentialsProvider(credentialProvider);
				HttpGet get = new HttpGet(STREAMING_API_URL+track);		
				HttpResponse response;
				try {
					//Execute
					response = client.execute(get);
					StatusLine status = response.getStatusLine();
					if(status.getStatusCode() == 200){
						InputStream inputStream = response.getEntity().getContent();
						BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
						String in;
						//Read line by line
						while((in = reader.readLine())!=null){
							//Parse and emit
							Tweet tweet = parseTweet(in);
							tweets.add(tweet);
						}
					}
				} catch (IOException e) {
					LOG.error("Error in communication with twitter api ["+get.getURI().toString()+"]");
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
					}
				}
			}catch(Throwable e){
				LOG.error(e);
			}
			LOG.error("An error ocurs, all runnable thread will be restarted");
			try{
				Thread.sleep(1000);
			}catch(Exception e){
			}
		}
	}
	
	
	public Tweet getNextTweet() {
		try {
			return tweets.poll(200, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			return null;
		}
	}
	
}
