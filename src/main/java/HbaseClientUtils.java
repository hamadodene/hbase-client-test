import java.io.*;
import java.util.Optional;
import java.util.Properties;

public class HbaseClientUtils {

    public static Properties getHbaseConfig(String config ) throws FileNotFoundException {
        Properties hbaseConfig = new Properties();
        try (InputStream input = new FileInputStream(config)){
            System.out.println("Start reading hbase client configuration file");
            if(input == null) {
                throw new Exception("Unable to find hbase.properties");
            }
            hbaseConfig.load(input);
            hbaseConfig.forEach((key,value)-> {
                if (key.toString().startsWith("test")) {
                    System.setProperty(key.toString().replace("test.", ""), value.toString());
                    hbaseConfig.remove(key.toString());
                }
            });
            return hbaseConfig;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
