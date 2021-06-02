package com.lorettax.StreamExtractionEngine.core;

public class Field {

    private String field;

    private FieldCondition condition;

    public Field(String field, FieldCondition condition) {
        this.field = field;
        this.condition = condition;
    }

    public Field() {

    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public FieldCondition getCondition() {
        return condition;
    }

    public void setCondition(FieldCondition condition) {
        this.condition = condition;
    }

}
