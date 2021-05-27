package com.lorettax.StreamExtractionEngine.tools;


import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;

public class Base64Utils {
	public static String encodeBase64(String input) {
		if(input == null) {
			return null;
		}
		return BaseEncoding.base64Url().omitPadding().encode(input.getBytes(Charsets.UTF_8));
	}
	
	public static String decodeBase64(String input) {
		if (input == null) {
			return null;
		}
		return new String(BaseEncoding.base64Url().decode(input), Charsets.UTF_8);
		
	}
	
	
}