package it.polito.bigdata.feb2019;

import java.io.Serializable;

public class Poi implements Serializable {

    private String id, city, country, category, subcategory;
    private String toString;

    Poi(String textPoi) {
        String[] fields = textPoi.split(",");
        this.id = fields[0];
        this.city = fields[3];
        this.country = fields[4];
        this.category = fields[5];
        this.subcategory = fields[6];
        this.toString = textPoi;
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getSubcategory() {
        return subcategory;
    }

    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
    }

    @Override
    public String toString() {
        return this.toString;
    }

}
