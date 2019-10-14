package test;

import java.sql.*;

public class JdbcCRUD {
    public static void main(String[] args) {
//        insert();
//        select();
        preparedStatementTest();
    }


    public static void insert() {
        //定义数据库连接对象
        Connection conn = null;
        //定义sql语句执行句柄，Statement对象
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "lx@6688");
            stmt = conn.createStatement();
            int rnt = stmt.executeUpdate("insert into user_info(id,name,age) values(1,'laozhao',25)");
            System.out.println(rnt);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void select() {
        //定义数据库连接对象
        Connection conn = null;
        //定义sql语句执行句柄，Statement对象
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project", "root", "lx@6688");
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from user_info");
            while (rs.next()) {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                int age = rs.getInt(3);
                System.out.println("id: " + id + " name: " + name + " age: " + age);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void preparedStatementTest() {
        //定义数据库连接对象
        Connection conn = null;
        //定义sql语句执行句柄，Statement对象
        PreparedStatement pstmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_test?characterEncoding=utf-8", "root", "lx@6688");
            pstmt = conn.prepareStatement("insert into user_info(id,name,age) values(?,?,?)");
            pstmt.setInt(1, 3);
            pstmt.setString(2, "李四");
            pstmt.setInt(3, 88);
            int rnt = pstmt.executeUpdate();
            System.out.println(rnt);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
