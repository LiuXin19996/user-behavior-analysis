package com.lx.sparkproject.dao.impl;

import com.lx.sparkproject.dao.ISessionRandomExtractDAO;

import com.lx.sparkproject.domian.SessionRandomExtract;
import  com.lx.sparkproject.jdbc.JDBCHelper;




/**
 * 随机抽取session的DAO实现
 * @author Administrator
 *
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

	/**
	 * 插入session随机抽取
	 * @param
	 */
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		
		Object[] params = new Object[]{
				sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getClickCategoryIds(),
				sessionRandomExtract.getSearchKeywords()
				 };
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
}
