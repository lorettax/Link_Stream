package com.lorettax.StreamExtractionEngine.console.commands;

public class KafkaSink {
	private String broker;
	private String topic;
	
	public String getBroker() {
		return broker;
	}
	
	public void setBroker(String broker) {
		this.broker = broker;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}
}