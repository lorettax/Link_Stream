package com.lorettax.StreamExtractionEngine.core;

public class FieldCondition {

    public static final String VOID = "___VOID___";

    private String method;

    public FieldCondition(String method) {
        this.method = method;
    }

    public String getMethod() {
        return method;
    }

}
