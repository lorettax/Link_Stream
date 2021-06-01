package com.lorettax.StreamExtractionEngine.core;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LinkStreamFQL {

    private String name;
    private String text_name;
    private String op;
    private String window;
    private String event_type;
    private Field target;
    private List<Field> on;
    private Set<String> alias;

    public LinkStreamFQL() {
        this.alias = new HashSet<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getWindow() {
        return window;
    }

    public void setWindow(String window) {
        this.window = window;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public Field getTarget() {
        return target;
    }

    public void setTarget(Field target) {
        this.target = target;
    }

    public List<Field> getOn() {
        return on;
    }

    public void setOn(List<Field> on) {
        this.on = on;
    }

    public String getText_name() {
        return text_name;
    }

    public void setText_name(String text_name) {
        this.text_name = text_name;
    }

    public Set<String> getAlias() {
        return alias;
    }

    public void setAlias(Set<String> alias) {
        this.alias = alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LinkStreamFQL that = (LinkStreamFQL) o;

        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

}

