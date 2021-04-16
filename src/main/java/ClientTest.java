import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import java.io.IOException;
import java.util.Properties;

public class ClientTest {
    public static void main(String[] args) throws Exception {

        Properties properties = HbaseClientUtils.getHbaseConfig("src/main/resources/hbase.properties");
        Configuration config = HBaseConfiguration.create();
        HbaseClientAuthentication authentication = new HbaseClientAuthentication(true,config,false, properties);
        authentication.login();
        try {
            HBaseAdmin.available(config);
            System.out.println("Master running");
            Connection conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();
            System.out.println("list of regionserver: " + admin.getRegionServers());

            System.out.println("Leader is " + admin.getMaster());
            System.out.println("Backup master is " + admin.getBackupMasters());

            TableName[] tables  = admin.listTableNames();
            System.out.println("Tables " + tables.length);
            for(TableName table: tables){
                System.out.println( "Table : " + table.toString());
            }


        }catch (IOException ex) {
            System.out.println("Hbase is not running " + ex.getMessage());
        }

    }
}
