import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.util.ToolRunner;

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
        LocalDateTime ldtStart = LocalDateTime.now();
        String _start = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStart);
        System.out.println("Start making " + table.getNameAsString() + " Snapshot in time " + _start);
        admin.snapshot(snapshotName, TableName.valueOf(table.getNameAsString()));
        LocalDateTime ldtStop = LocalDateTime.now();
        String _stop = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStop);
        System.out.println("End making " + table.getNameAsString() + " Snapshot in time " + _stop);
    }

    public void createAllTablesSnapshot() throws IOException {
        TableName[] tables = admin.listTableNames();
        LocalDateTime ldt = LocalDateTime.now();
        String date = DateTimeFormatter.ofPattern("yyyy-dd-MM", Locale.ITALIAN).format(ldt);
        for (TableName table : tables) {
            LocalDateTime ldtStart = LocalDateTime.now();
            String _start = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStart);
            System.out.println("Start making " + table.getNameAsString() + " Snapshot in time " + _start);
            String snapShotName = table.getNameAsString() + "-" + date;
            admin.snapshot(snapShotName, TableName.valueOf(table.getNameAsString()));
            LocalDateTime ldtStop = LocalDateTime.now();
            String _stop = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStop);
            System.out.println("End making " + table.getNameAsString() + " Snapshot in time " + _stop);
        }
    }

    public List<SnapshotDescription> listSnapShot() throws IOException {
        return admin.listSnapshots();
    }

    public void cloneSnapshot(String snapShotName, String newTableName) throws IOException {
        LocalDateTime ldtStart = LocalDateTime.now();
        String _start = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStart);
        System.out.println("Start cloning snapshot in time" + _start);
        admin.cloneSnapshot(snapShotName, TableName.valueOf(newTableName));
        LocalDateTime ldtStop = LocalDateTime.now();
        String _stop = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStop);
        System.out.println("End cloning snapshot in time " + _stop);
    }

    public void restoreSnapshot(TableName tableName, String snapShotName) throws IOException {
        LocalDateTime ldtStart = LocalDateTime.now();
        String _start = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStart);
        System.out.println("Start restoring snapshot for table " + tableName.getNameAsString() + " in time " + _start);
        admin.disableTable(tableName);
        admin.restoreSnapshot(snapShotName);
        admin.enableTable(tableName);
        LocalDateTime ldtStop = LocalDateTime.now();
        String _stop = DateTimeFormatter.ofPattern("yyyy-dd-MM hh:mm:ss", Locale.ITALIAN).format(ldtStop);
        System.out.println("End restoring snapshot for table " + tableName.getNameAsString() + " in time " + _stop);
    }

    public boolean sendSnapShot(Configuration config, Configuration hadoopMaster, Configuration hadoopReplica, String snapshotName) throws Exception {
        int result = -1;
        String src = hadoopMaster.get("fs.defaultFS") + "/hbase";
        String dest = hadoopReplica.get("fs.defaultFS") + "/hbase";
        System.out.println("Send snapshot " + snapshotName);

        String[] args = {
                "-snapshot",
                snapshotName,
                "-copy-from",
                src,
                "-copy-to",
                dest,
                "-mappers",
                "100"
        };

        System.out.println("Start send snapshot from " + hadoopMaster.get("fs.defaultFS") + " to " + hadoopReplica.get("fs.defaultFS"));
        result = ToolRunner.run(config, new ExportSnapshot(), args);
        return result == 0;
    }
}
