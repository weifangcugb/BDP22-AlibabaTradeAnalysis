package com.aura;

import com.aura.action.HBaseController;
import com.aura.dao.ShopInfoDao;
import com.aura.model.Dimension;
import com.aura.model.ShopInfo;
import com.aura.service.DimensionService;
import com.aura.service.ShopInfoService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * Created by weifang on 19-7-18.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:spring.xml"})
public class TestService {
    @Resource
    private ShopInfoDao shopInfoDao;

    //SparkStreaming中用到的shop与城市对应表
    @Test
    public void getShopInfoList() {
        List<ShopInfo> list = (List<ShopInfo>)shopInfoDao.selectList("common.shopInfo.getStreamShopCityList", null);
        System.out.println(list.size());
    }

    @Resource
    private ShopInfoService service;

    @Test
    public void getDismensionLis() {
        ShopInfo info = service.getShopInfoById(Integer.valueOf("1862"));
        System.out.println(info.getCityName());
    }

    @Resource
    HBaseController hBaseController;

    @Test
    public void queryFromHBase() throws IOException {
        String result = hBaseController.searchByUserIdAndDate("13822095","2016.02.01","2016.08.30");
        System.out.println(result);
    }



}
