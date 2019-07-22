package com.aura.database;

import com.aura.model.ShopInfo;
import org.apache.ibatis.session.SqlSessionException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SaveShopInfo {
    public List<ShopInfo> getShopInfoList(){
        List<ShopInfo> list = new ArrayList<ShopInfo>();
        try{
            String filePath = "trade-analysis/web/data/shop_info.txt";
            File filename = new File(filePath);
            FileReader fileReader = new FileReader(filename);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            String[] tmpValues = null;
            int length = 0;
            while((line = bufferedReader.readLine()) != null){
                tmpValues = line.split(",");
                length = tmpValues.length;
                ShopInfo shopInfo = new ShopInfo();
                for(int i = 0; i < length; i++){
                    if(i == 0){
                        shopInfo.setShopId(Long.valueOf("".equals(tmpValues[0]) ? "0" : tmpValues[0]));
                    }else if(i == 1){
                        shopInfo.setCityName(tmpValues[1]);
                    }else if(i == 2){
                        shopInfo.setLocationId(Integer.parseInt("".equals(tmpValues[2]) ? "0" :  tmpValues[2]));
                    }else if(i == 3){
                        shopInfo.setPerPay(Integer.parseInt("".equals(tmpValues[3]) ? "0" :  tmpValues[3]));
                    }else if(i == 4){
                        shopInfo.setScore(Integer.parseInt("".equals(tmpValues[4]) ? "0" :  tmpValues[4]));
                    }else if(i == 5){
                        shopInfo.setCommentCnt(Integer.parseInt("".equals(tmpValues[5]) ? "0" :  tmpValues[5]));
                    }else if(i == 6){
                        shopInfo.setShopLevel(Integer.parseInt("".equals(tmpValues[6]) ? "0" :  tmpValues[6]));
                    }else if(i == 7){
                        shopInfo.setCate1Name(tmpValues[7]);
                    }else if(i == 8){
                        shopInfo.setCate2Name(tmpValues[8]);
                    }else{
                        shopInfo.setCate3Name(tmpValues[9]);
                    }
                }
                list.add(shopInfo);
                //清空shopInfo属性值
                shopInfo = null;
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return list;
    }

    public int save(List<ShopInfo> list){
        String sql = "insert into shop_info (shop_id, city_name, location_id, per_pay, score, comment_cnt," +
                " shop_level," +
                " cate_1_name, cate_2_name, cate_3_name) values "+
                " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = JDBCUtils.getConnection();
            //设置事务属性
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql,
                    ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            for(ShopInfo s : list){
                pstmt.setLong(1, s.getShopId());
                pstmt.setString(2, s.getCityName());
                pstmt.setInt(3, s.getLocationId());
                pstmt.setInt(4, s.getPerPay());
                pstmt.setInt(5, s.getScore());
                pstmt.setInt(6, s.getCommentCnt());
                pstmt.setInt(7, s.getShopLevel());
                pstmt.setString(8, s.getCate1Name());
                pstmt.setString(9, s.getCate2Name());
                pstmt.setString(10, s.getCate3Name());
                pstmt.addBatch();
            }
            int[] tt = pstmt.executeBatch();
            System.out.println("insert : " + tt.length);
            //提交，设置事务初始值
            conn.commit();
            conn.setAutoCommit(true);
            //插入成功，返回
            return tt.length;
        }catch (SQLException ex){
            ex.printStackTrace();
            try{
                //提交失败，执行回滚操作
                conn.rollback();

            }catch (SQLException e) {
                e.printStackTrace();
                System.err.println("回滚执行失败!!!");
            }
            System.err.println("执行失败");
            //插入失败返回标志0
            return 0;
        }finally {
            JDBCUtils.release(conn, pstmt, null);
        }
    }

    public static void main(String[] args) {
        SaveShopInfo saveShopInfo = new SaveShopInfo();
        List<ShopInfo> list = saveShopInfo.getShopInfoList();
        int txt = list.size();
        System.out.println("--共有"+txt+"条商家信息");
        int saveint = saveShopInfo.save(list);
        System.out.println("--插入成功"+saveint+"条商家信息");
        System.out.println("--插入失败"+(txt-saveint)+"条商家信息");
    }

}
