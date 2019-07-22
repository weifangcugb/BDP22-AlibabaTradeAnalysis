package com.aura.service;

import com.aura.model.result.CityConsumption;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("prestoService")
public class PrestoService {

    /**
     *  以城市为单位，统计每个城市总体消费金额 （饼状图）
     */
    public List<CityConsumption> getCityConsumption() {

        return null;
    }
}
