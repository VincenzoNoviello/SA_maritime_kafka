package com.example;

public class AISMessage {

    private String id;
    private String timestamp;
    private String longitude;
    private String latitude;
    private String annotation;
    private String speed;
    private String heading;
    private String type;
    
    public AISMessage(String id, String timestamp, String longitude, String latitude, String annotation, String speed,
            String heading,String type) {
        this.id = id;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        this.annotation = annotation;
        this.speed = speed;
        this.heading = heading;
        this.type = type;
    }

    public AISMessage(){

    }
    
    @Override
    public String toString() {
        return annotation + "," + heading + "," + id + "," + latitude
                + "," + longitude + "," + speed + "," + timestamp + "," + type;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    public String getLongitude() {
        return longitude;
    }
    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
    public String getLatitude() {
        return latitude;
    }
    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }
    public String getAnnotation() {
        return annotation;
    }
    public void setAnnotation(String annotation) {
        this.annotation = annotation;
    }
    public String getSpeed() {
        return speed;
    }
    public void setSpeed(String speed) {
        this.speed = speed;
    }
    public String getHeading() {
        return heading;
    }
    public void setHeading(String heading) {
        this.heading = heading;
    }

    public String getType(){
        return type;
    }

}
