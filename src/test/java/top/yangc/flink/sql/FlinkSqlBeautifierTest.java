package top.yangc.flink.sql;

import org.junit.Test;

public class FlinkSqlBeautifierTest {


    /**
     * 格式化单条SQL语句
     *
     * 输出结果：
     * CREATE TABLE t1 (
     *   id STRING,
     *   name STRING
     * ) WITH (
     *   'connector' = 'print'
     * )
     */
    @Test
    public void formatSingleSql() {
        FlinkSqlBeautifier flinkSqlBeautifier = new FlinkSqlBeautifier();
        String sql = "create table t1 (id string,name string) with('connector'='print')";
        System.out.println(flinkSqlBeautifier.format(sql));
    }


    /**
     * 格式化多条语句
     */
    @Test
    public void formatMultiSql() {
        FlinkSqlBeautifier flinkSqlBeautifier = new FlinkSqlBeautifier();
        String sql = "" +
                "CREATE TABLE oracle_subscribe_papam_dim (" +
                "    APP_NAME STRING," +
                "    TABLE_NAME STRING," +
                "    SUBSCRIBE_TYPE STRING," +
                "    SUBSCRIBER STRING," +
                "    DB_URL STRING," +
                "    KAFKA_SERVERS STRING," +
                "    KAFKA_SINK_TOPIC STRING," +
                "    ISCURRENT DECIMAL(11, 0)," +
                "    LASTUPDATEDDT TIMESTAMP," +
                "    PRIMARY KEY(APP_NAME,TABLE_NAME) NOT ENFORCED" +
                ") WITH (" +
                "    'connector' = 'jdbc'," +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c'," +
                "    'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM'," +
                "    'driver' = 'oracle.jdbc.OracleDriver'" +
                ");" +
                "" +
                "CREATE TABLE redis_dim (" +
                "  `key` String," +
                "  hashkey String," +
                "  res String" +
                ") WITH (" +
                "  'connector.type' = 'redis'," +
                "  'redis.ip' = '192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379,192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379',\n" +
                "  'database.num' = '0'," +
                "  'operate.type' = 'hash'," +
                "  'redis.version' = '2.6'" +
                ");";
        System.out.println(flinkSqlBeautifier.format(sql));
    }


    /**
     * insert 语句 在junit环境下会莫名报错，原因暂时未知,使用main方法执行就正常
     */
    @Test
    public void formatSqlFromFile() {
        FlinkSqlBeautifier flinkSqlBeautifier = new FlinkSqlBeautifier();
        String filePath = "src/test/resources/simpleJoin.sql";
        System.out.println(flinkSqlBeautifier.formatSqlFromFile(filePath));
    }



    public static void main(String[] args) {
        FlinkSqlBeautifier flinkSqlBeautifier = new FlinkSqlBeautifier();
        String sql = "CREATE TABLE source (\n" +
                "  `id` INT,\n" +
                "  `saleCount` INT,\n" +
                "  `banned` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'fields.id.min' = '0',\n" +
                "  'fields.id.max' = '10',\n" +
                "  'rows-per-second' = '1'\n" +
                ");\n" +
                "\n" +
                "CREATE TEMPORARY TABLE dim (\n" +
                "  `id` INT,\n" +
                "  `name` STRING,\n" +
                "  `power` STRING,\n" +
                "  `age` INT,\n" +
                "  `address` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'fields.id.min' = '0',\n" +
                "  'fields.id.max' = '10',\n" +
                "  'rows-per-second' = '2'\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW joinView (\n" +
                "  `id`,`name`,`power`,`age`,`address`,`saleCount`\n" +
                ") AS SELECT a.`id`,b.`name`,b.`power`,b.`age`,b.`address`,a.`saleCount` FROM source a LEFT JOIN dim b ON a.`id`=b.`id`;\n" +
                "\n" +
                "\n" +
                "CREATE TEMPORARY TABLE target (\n" +
                "  `id` INT,\n" +
                "  `name` STRING,\n" +
                "  `power` STRING,\n" +
                "  `age` INT,\n" +
                "  `address` STRING,\n" +
                "  `saleCount` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "\n" +
                "CREATE TEMPORARY TABLE source1 (\n" +
                "  `id` INT,\n" +
                "  `stock` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'fields.id.min' = '0',\n" +
                "  'fields.id.max' = '10',\n" +
                "  'rows-per-second' = '1'\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW joinView1 (\n" +
                "  `id`,`name`,`power`,`age`,`address`,`saleCount`,`stock`\n" +
                ") AS\n" +
                "SELECT a.`id`,a.`name`,a.`power`,a.`age`,a.`address`,a.`saleCount`,b.`stock`\n" +
                "FROM joinView a LEFT JOIN source1 b ON a.`id`=b.`id`;\n" +
                "\n" +
                "CREATE TEMPORARY TABLE target1 (\n" +
                "  `id` INT,\n" +
                "  `name` STRING,\n" +
                "  `power` STRING,\n" +
                "  `age` INT,\n" +
                "  `address` STRING,\n" +
                "  `saleCount` INT,\n" +
                "  `stock` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "\n" +
                "INSERT INTO target SELECT * FROM joinView;\n";
        String filePath = "src/test/resources/simpleJoin.sql";
        System.out.println(flinkSqlBeautifier.format(sql));
    }

}
