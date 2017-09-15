package cn.jpush.hivejdbc;

/**
 * Created by wmky_kk on 2017-09-14.
 */
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver"; // 如果写成org.apache.hadoop.hive.jdbc.HiveDriver 会报java.lang.ClassNotFoundException

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab("youck@JIGUANG.CN", "src/main/Resources/youck.keytab");
        }catch (IOException e){
            System.out.println(e);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.254.96:10000/tmp;principal=hive/nfjd-hadoop02-node56.jpushoa.com@JIGUANG.CN", "", "");
        Statement stmt = con.createStatement();
        String tableName = "zzws_imeiphone_dw";

        // select * query
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
        }
    }
}