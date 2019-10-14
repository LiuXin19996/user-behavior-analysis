package test;
import com.lx.sparkproject.conf.ConfigurationManager;

public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String testV1=ConfigurationManager.getProperty("jdbc.driver");
        System.out.println(testV1);
    }
}
