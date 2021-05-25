package com.ngt.demo.networkflow.beans;

/**
 * @author ngt on 2021-05-25 8:54
 * @version 1.0
 */
public class ApacheLogEvent {
	private String id;
	private String userId;
	private Long timestamp;
	private String moethd;
	private String url;

	public ApacheLogEvent() {
	}

	public ApacheLogEvent(String id, String userId, Long timestamp, String moethd, String url) {
		this.id = id;
		this.userId = userId;
		this.timestamp = timestamp;
		this.moethd = moethd;
		this.url = url;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getMoethd() {
		return moethd;
	}

	public void setMoethd(String moethd) {
		this.moethd = moethd;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		return "ApacheLogEvent{" +
				"id='" + id + '\'' +
				", userId='" + userId + '\'' +
				", timestamp=" + timestamp +
				", moethd='" + moethd + '\'' +
				", url='" + url + '\'' +
				'}';
	}
}
