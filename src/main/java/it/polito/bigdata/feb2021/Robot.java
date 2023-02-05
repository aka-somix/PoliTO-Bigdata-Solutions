package it.polito.bigdata.feb2021;

import java.io.Serializable;

public class Robot implements Serializable {
    private String id, pid, ip;

    public Robot(String text) {
        String[] fields = text.split(",");
        this.id = fields[0];
        this.pid = fields[1];
        this.ip = fields[2];
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
