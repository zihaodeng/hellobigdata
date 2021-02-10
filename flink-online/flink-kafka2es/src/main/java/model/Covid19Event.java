package model;

public class Covid19Event {
    private String city_code;
    private Integer count;
    private Long timestamp;

    public Covid19Event(String city_code, Integer count, Long timestamp){
        this.city_code=city_code;
        this.count=count;
        this.timestamp=timestamp;
    }

    public String getCityCode() {
        return this.city_code;
    }

    public Integer getCount() {
        return this.count;
    }

    public long getTimestamp() {
        return this.timestamp;
    }
}
