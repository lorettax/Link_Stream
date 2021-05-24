package com.lorettax.StreamExtractionEngine.tools;


import org.apache.commons.codec.digest.DigestUtils;

public class MD5Tool {
	public static String md5ID(String... args) {
		StringBuilder sb = new StringBuilder();
		for (String arg : args) {
			sb.append(arg);
		}
		return DigestUtils.md5Hex(sb.toString());
		
	}	
	
	
}