package com.wang930126.cat.catpublisher.service;

import java.util.Map;

public interface PublisherService {

    public int getDauTotal(String date);

    public Map getDauHour(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

}
