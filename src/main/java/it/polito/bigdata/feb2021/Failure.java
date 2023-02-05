package it.polito.bigdata.feb2021;

import java.io.Serializable;

public class Failure implements Serializable {

    private String rid, code, date, time;

    public Failure(String text) {
        String[] fields = text.split(",");
        this.rid = fields[0];
        this.code = fields[1];
        this.date = fields[2];
        this.time = fields[3];
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
