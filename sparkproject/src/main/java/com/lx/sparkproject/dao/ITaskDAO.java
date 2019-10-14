package com.lx.sparkproject.dao;
import com.lx.sparkproject.domian.Task;

/**
 * 任务管理接口
 */
public interface ITaskDAO {
    /**
     * 根据主键查询任务
     * @param taskId  主键
     * @return
     */
   Task findById(long taskId);
}
