package com.lorettax.StreamExtractionEngine.core.functions;


public class Void {
	private static final Void INSTANCE = new Void();
	
	public final String value = "Void";
	
	private Void() {
		
	}
	
	public static Void create() {
		return INSTANCE;
	}
	
	public String getValue() {
		return value;
	}
}