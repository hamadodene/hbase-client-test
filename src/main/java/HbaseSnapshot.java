import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

public class HbaseSnapshot {
    private Admin admin;

    public HbaseSnapshot(Admin admin) {
        this.admin = admin;
    }

    public void createTableSnapshot(TableName table, String snapshotName) throws IOException {
        admin.snapshot(snapshotName, TableName.valueOf(table.getNameAsString()));
    }

    public void createAllTablesSnapshot() throws IOException {
        TableName[] tables  = admin.listTableNames();
        LocalDateTime ldt = LocalDateTime.now();
        String date = DateTimeFormatter.ofPattern("yyyy-dd-MM", Locale.ITALIAN).format(ldt);
        for (TableName table : tables) {
            String snapShotName = table.getNameAsString()+"-"+date;
            admin.snapshot(snapShotName, TableName.valueOf(table.getNameAsString()));
        }
    }

    public List<SnapshotDescription> listSnapShot() throws IOException {
        return admin.listSnapshots();
    }

    public void restoreSnapshot(String snapShotName) throws IOException {
        //Restore snapshot with the same table name
        admin.restoreSnapshot(snapShotName);
    }
}
