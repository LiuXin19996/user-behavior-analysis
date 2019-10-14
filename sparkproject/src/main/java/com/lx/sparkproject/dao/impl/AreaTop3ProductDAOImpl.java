package com.lx.sparkproject.dao.impl;

import com.lx.sparkproject.dao.IAreaTop3ProductDAO;
import com.lx.sparkproject.domian.AreaTop3Product;
import com.lx.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;



/**
 * 各区域top3热门商品DAO实现类
 * @author Administrator
 *
 *             AreaTop3Product areaTop3Product = new AreaTop3Product();
 *             areaTop3Product.setTaskid(taskid);
 *             areaTop3Product.setArea(row.getString(0));
 *             areaTop3Product.setProductid(row.getLong(1));
 *             areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(2))));
 *             areaTop3Product.setCityInfos(row.getString(3));
 *             areaTop3Product.setProductName(row.getString(4));
 *             areaTop3Product.setProductStatus(row.getString(5));
 *             areaTop3Products.add(areaTop3Product);
 *
 */
public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {

	@Override
	public void insertBatch(List<AreaTop3Product> areaTopsProducts) {
		String sql = "INSERT INTO area_top3_product VALUES(?,?,?,?,?,?,?)";
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		
		for(AreaTop3Product areaTop3Product : areaTopsProducts) {
			Object[] params = new Object[7];
			
			params[0] = areaTop3Product.getTaskid();
			params[1] = areaTop3Product.getArea();
			params[2] = areaTop3Product.getProductid();
			params[3] = areaTop3Product.getCityInfos();
			params[4] = areaTop3Product.getClickCount();
			params[5] = areaTop3Product.getProductName();
			params[6] = areaTop3Product.getProductStatus();
			
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
	}

}
