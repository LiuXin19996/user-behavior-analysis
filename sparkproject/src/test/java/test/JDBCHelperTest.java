package test;

import com.lx.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class JDBCHelperTest {
    public static void main(String[] args) throws Exception {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
//        final Map<String, Object> userInfo = new HashMap<String, Object>();
//        jdbcHelper.executeUpdate("insert into user_info(id,name,age) values(?,?,?)", new Object[]{55,"张飞",44});

//        jdbcHelper.executeQuery("select * from user_info where id=?", new Object[]{3}, new JDBCHelper.QueryCallback() {
//            @Override
//            public void process(ResultSet rs) throws Exception {
//                while (rs.next()) {
//                    String name = rs.getString(2);
//                    int age = rs.getInt(3);
//                    userInfo.put("name", name);
//                    userInfo.put("age", age);
//                    System.out.println("name: " + name + " age: " + age);
//                }
//            }
//        });

        String sql="insert into user_info(id,name,age) values(?,?,?)";
        List<Object[]> paramsList=new ArrayList<Object[]>();
        for(int i=500;i<506;i++){
             paramsList.add(new Object[]{i,"wangwu",99});
        }
        jdbcHelper.executeBatch(sql, paramsList);

    }
}
