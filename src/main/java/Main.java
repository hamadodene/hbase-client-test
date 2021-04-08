import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
        new Main().connect();
    }

    public void connect() throws IOException {
        Configuration config = HBaseConfiguration.create();
        String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
        config.set("hbase.regionserver.kerberos.principal","hbase/_HOST@KRB.SVILUPPO.DNA");
        config.set("hbase.master.kerberos.principal","hbase/_HOST@KRB.SVILUPPO.DNA");
        config.addResource(new Path(path));
        config.set("hadoop.security.authentication", "Kerberos");
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
        System.setProperty("sun.security.jgss.debug", "true");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.jgss.debug", "true");
        config.set("hadoop.rpc.protection", "privacy");
        config.set("hbase.rpc.protection", "privacy");
        System.setProperty("java.security.debug", "logincontext,policy,scl,gssloginconfig");
        System.out.println("Krb " + System.getProperty("java.security.krb5.conf"));
        File file = new File("src/main/resources/krb5.conf");
        System.out.println("Check if krb5 exist " + file.exists());

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab("hbase@KRB.SVILUPPO.DNA","src/main/resources/hbase.headless.keytab");
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
