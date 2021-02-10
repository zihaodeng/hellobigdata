package com.zmboo.lotemall.api.entity;

import java.math.BigDecimal;

public class Order {
    private Long orderId;
    private Integer goodsId;
    private Integer userId;
    private String address;
    private String PayTime;
    private BigDecimal amount;

    public String getPayTime() {
        return PayTime;
    }

    public void setPayTime(String payTime) {
        PayTime = payTime;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Integer getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Integer goodsId) {
        this.goodsId = goodsId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

}
