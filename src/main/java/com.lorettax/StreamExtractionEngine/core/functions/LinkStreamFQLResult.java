package com.lorettax.StreamExtractionEngine.core.functions;


import com.lorettax.StreamExtractionEngine.core.LinkStreamFQL;

public class LinkStreamFQLResult {
	
	private LinkStreamFQL fql;
	private Object result;
	
	public LinkStreamFQLResult() {
	}
	
	public LinkStreamFQLResult(LinkStreamFQL fql, Object result) {
		this.fql = fql;
		this.result = result;
	}
	
	public LinkStreamFQL getFql() {
		return fql;
	}
	
	public void setFql(LinkStreamFQL fql) {
		this.fql = fql;
	}
	
	public Object getResult() {
		return result;
	}
	
	public void setResult(Object result) {
		this.result = result;
	}
	
}

