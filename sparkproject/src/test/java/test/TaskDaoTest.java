package test;

import com.lx.sparkproject.dao.ITaskDAO;
import com.lx.sparkproject.dao.impl.DAOFactory;
import com.lx.sparkproject.domian.Task;

public class TaskDaoTest {
    public static void main(String[] args) {
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task=taskDao.findById(2);
        System.out.println(task.getTaskName());
    }
}
