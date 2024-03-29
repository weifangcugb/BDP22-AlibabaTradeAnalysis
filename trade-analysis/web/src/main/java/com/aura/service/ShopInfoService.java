package com.aura.service;

import com.aura.basic.BasicServiceSupportImpl;
import com.aura.dao.ShopInfoDao;
import com.aura.model.ShopInfo;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("shopInfoService")
public class ShopInfoService extends BasicServiceSupportImpl{

    @Resource
    private ShopInfoDao shopInfoDao;

    //SparkStreaming中用到的shop与城市对应表
    public Map<String,String> getShopInfoList() {
        List<ShopInfo> list = (List<ShopInfo>)shopInfoDao.selectList("common.shopInfo.getStreamShopCityList", null);
        Map<String,String> shopCityMap = new HashMap<>();
        list.stream().forEach(info -> shopCityMap.put(info.getShopId().toString(),info.getCityName()));
        return shopCityMap;
    }

    //HBase中用到的根据shop_id查询shop_info信息
    @Cacheable
    public ShopInfo getShopInfoById(Integer shopId) {
        ShopInfo info = (ShopInfo) shopInfoDao.selectObject("common.shopInfo.getShopInfoById",shopId);
        return info;
    }
}
