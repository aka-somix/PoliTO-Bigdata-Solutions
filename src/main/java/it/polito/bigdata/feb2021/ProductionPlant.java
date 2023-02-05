package it.polito.bigdata.feb2021;

import java.io.Serializable;

public class ProductionPlant implements Serializable {

    private String id, city, country;

    public ProductionPlant(String text) {
        String[] fields = text.split(",");

        this.id = fields[0];
        this.city = fields[1];
        this.country = fields[2];
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}
