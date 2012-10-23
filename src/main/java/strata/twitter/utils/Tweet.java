package strata.twitter.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Tweet {
	String user;
	List<String> hashtags;

	public Tweet(String user) {
		this.user = user;
		hashtags = new ArrayList<String>();
	}

	public String getUser() {
		return user;
	}

	public List<String> getHashtags() {
		return Collections.unmodifiableList(hashtags);
	}

	public void addHashtag(String hashtag) {
		hashtags.add(hashtag);
	}
}
