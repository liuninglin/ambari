package com.johnny.demo;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;

/**
 * Examples of jdbc connection with Kerberos
 */

public class Test {
    /**
     * HiveServer2 with Kerberos authentication, the URL format is:
     * jdbc:hive2://<host>:<port>/<db>;principal=
     * <Server_Principal_of_HiveServer2>
     */
    private static final String LOCAL_SPARK_PRINCIPAL = "spark/ambari.com@CONNEXT.COM";
    private static final String REMOTE_SPARK_PRINCIPAL = "hive/slave1.bigdata.com@BIGDATA.COM";

    private static final String LOCAL_THRIFT_SERVER_IP_PORT = "ambari.com:10016";
    private static final String REMOTE_THRIFT_SERVER_IP_PORT = "192.168.110.151:10001";

    private static final String LOCAL_KRB5_FILE = "local/krb5.conf";
    private static final String REMOTE_KRB5_FILE = "remote/krb5.conf";

    private static final String LOCAL_SPARK_KEYTAB_FILE = "local/spark.service.keytab";
    private static final String REMOTE_SPARK_KEYTAB_FILE = "remote/hive.service.keytab";


    private static String sparkPrincipal = LOCAL_SPARK_PRINCIPAL;
    private static String thriftServerIPPort = LOCAL_THRIFT_SERVER_IP_PORT;
    private static String krb5File = LOCAL_KRB5_FILE;
    private static String sparkKeytab = LOCAL_SPARK_KEYTAB_FILE;


    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String thrift_url = "jdbc:hive2://" + thriftServerIPPort + "/sephora;httpPath=/;principal=" + sparkPrincipal;
    private static String sql = "";
    private static ResultSet res;

    public static Connection get_conn(String url) throws SQLException, ClassNotFoundException, FileNotFoundException {

        // If you want to see detail info, you ought to turn on.
//        System.setProperty("sun.security.krb5.debug", "true");

        // Use kerberos to login safely
        System.setProperty("java.security.krb5.conf", Test.class.getClassLoader().getResource(krb5File).getPath());
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(sparkPrincipal, Test.class.getClassLoader().getResource(sparkKeytab).getPath());
        } catch (IOException e1) {
            e1.printStackTrace();
        }


        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url);

        return conn;
    }

    /**
     * Check all tables of specific database
     *
     * @param statement
     * @return
     */
    public static boolean show_tables(Statement statement) {
        sql = "SHOW TABLES";
        System.out.println("Running SQL: " + sql);
        try {
            ResultSet res = statement.executeQuery(sql);
            System.out.println("results: ");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Obtain description of specific table
     *
     * @param statement
     * @param tableName
     * @return
     */
    public static boolean describ_table(Statement statement, String tableName) {
        sql = "DESCRIBE " + tableName;
        System.out.println("Running SQL: " + sql);
        try {
            res = statement.executeQuery(sql);
            System.out.print(tableName + "desc: ");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Drop specific table
     *
     * @param statement
     * @param tableName
     * @return
     */
    public static boolean drop_table(Statement statement, String tableName) {
        sql = "DROP TABLE IF EXISTS " + tableName;
        System.out.println("Running SQL: " + sql);
        try {
            statement.execute(sql);
            System.out.println(tableName + "Delete successfully");
            return true;
        } catch (SQLException e) {
            System.out.println(tableName + "Delete unsuccessfully");
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Obtain record from specific table
     *
     * @param statement
     * @return
     */
    public static boolean queryData(Statement statement, String tableName) {
        sql = "SELECT * FROM " + tableName + " LIMIT 1000";
        System.out.println("Running SQL: " + sql);
        try {
            res = statement.executeQuery(sql);
            System.out.println("results: ");
            while (res.next()) {
                System.out.println(res.getString(1) + "," + res.getString(2) + "," + res.getString(3));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Obtain num of records from specific table
     *
     * @param statement
     * @return
     */
    public static boolean queryDataCount(Statement statement, String tableName) {
        sql = "SELECT COUNT(*) FROM " + tableName;
        System.out.println("Running SQL: " + sql);
        try {
            res = statement.executeQuery(sql);
            System.out.println("results: ");
            while (res.next()) {
                System.out.println(res.getInt(1));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * Create table
     *
     * @param statement
     * @return
     */
    public static boolean createTable(Statement statement, String tableName) {
        sql = "CREATE TABLE test_1m_test2 AS SELECT * FROM test_1m_test"; //  为了方便直接复制另一张表数据来创建表
        System.out.println("Running SQL: " + sql);
        try {
            boolean execute = statement.execute(sql);
            System.out.println("results: " + execute);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {

        try {
            Connection conn = get_conn(thrift_url);
            Statement stmt = conn.createStatement();

            String tableName = "sephora_order";

            show_tables(stmt);
//            describ_table(stmt, tableName);
//            drop_table(stmt, tableName);
//            show_tables(stmt);
//            queryData(stmt, tableName);
//            queryDataCount(stmt, tableName);
//            createTable(stmt, tableName);

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("!!!!!!END!!!!!!!!");
        }
    }
}
