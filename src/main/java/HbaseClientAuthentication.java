import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.Properties;

public class HbaseClientAuthentication {
    public static final String HBASE_CONFIGURATION_FILE = System.getProperty("hbase.configurationFile");
    public static final String HBASE_CLIENT_PRINCIPAL = System.getProperty("hbase.client.keytab.principal");
    public static final String HBASE_CLIENT_KEYTAB = System.getProperty("hbase.client.keytab.file");
    private boolean useKerberos;
    private Configuration hbaseConfig;
    private boolean useHbaseSiteConfig;
    private Properties properties;

    public HbaseClientAuthentication(boolean useKerberos, Configuration hbaseConfig, boolean useHbaseSiteConfig, Properties properties) {
        this.useKerberos = useKerberos;
        this.hbaseConfig = hbaseConfig;
        this.useHbaseSiteConfig = useHbaseSiteConfig;
        this.properties = properties;
    }

    public void login() throws Exception {
        if (useKerberos) {
            //Login with kerberos
            if (useHbaseSiteConfig) {
                //Assume all properties is specified in hbase-site.xml
                String configFile = HBASE_CONFIGURATION_FILE;
                if(configFile == null) {
                    throw new Exception("hbase-site.xml not found");
                }
                hbaseConfig.addResource(new Path(configFile));
            } else {
                //Get info from hbase.properties files
                properties.forEach((key,value) ->{
                    hbaseConfig.set(key.toString(),value.toString());
                });
            }
            String clientPrincipal = HBASE_CLIENT_PRINCIPAL;
            String clientKeytab = HBASE_CLIENT_KEYTAB;
            System.out.println("Login with principal "+ clientPrincipal + " keytab " + clientKeytab);
            UserGroupInformation.setConfiguration(hbaseConfig);
            UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal, clientKeytab);
            UserGroupInformation.setLoginUser(userGroupInformation);
        } else {
            //Login without kerberos
            if(useHbaseSiteConfig) {
                String configFile = HBASE_CONFIGURATION_FILE;
                hbaseConfig.addResource(new Path(configFile));
            } else {
                //Get info from hbase.properties files
                properties.forEach((key,value) ->{
                    hbaseConfig.set(key.toString(),value.toString());
                });
            }
            UserGroupInformation.setConfiguration(hbaseConfig);
            UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();
            UserGroupInformation.setLoginUser(userGroupInformation);
        }
    }
}
