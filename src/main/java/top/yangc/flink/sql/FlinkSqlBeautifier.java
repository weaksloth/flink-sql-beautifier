package top.yangc.flink.sql;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkSqlBeautifier {

    private final CalciteParser calciteParser;

    public FlinkSqlBeautifier() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        calciteParser = getFlinkCalciteParser(tableEnv.getConfig());
    }

    /**
     * 格式化SQL文本字符串，可以包含一个或者多个SQL
     *
     * @param sql 一个或者多个sql组成的字符串，一般由分号分割
     * @return 格式化后的SQL语句
     */
    public String format(String sql) {
        String[] split = sql.split(SqlUtils.SEMICOLON);
        return formatSqlList(Arrays.asList(split));
    }

    /**
     * 格式化文件中的SQL语句
     *
     * @param absoluteFilePath 文件的绝对路劲
     * @return 格式化后的SQL语句
     */
    public String formatSqlFromFile(String absoluteFilePath) {
        try (FileInputStream inputStream = new FileInputStream(new File(absoluteFilePath))){
            List<String> sqlList = SqlUtils.readSqlFile(inputStream);
            return formatSqlList(sqlList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 格式化SQL集合中的SQL语句
     *
     * @param sqlList SQL集合
     * @return 格式化后的SQL语句
     */
    public String formatSqlList(List<String> sqlList) {
        List<String> beautifySqlList = new ArrayList<>(sqlList.size());
        for(String str : sqlList) {
            // 解析获取该SQL的行注释，并且移除掉
            List<String> commentList = Stream.concat(
                    RegexUtils.init().regexList(str, "--.*\r\n").getList().stream(),
                    RegexUtils.init().regexList(str, "--.*\n").getList().stream())
                    .collect(Collectors.toList());

            SqlPrettyWriter writer =
                    new SqlPrettyWriter(
                            SqlPrettyWriter.config()
                                    .withDialect(AnsiSqlDialect.DEFAULT)
                                    .withAlwaysUseParentheses(true)
                                    .withQuoteAllIdentifiers(false)                                         // 是否包含``
                                    .withSelectListItemsOnSeparateLines(true)
                                    .withIndentation(4));
            SqlNode node = calciteParser.parse(processSql(str));
            String formatSql = writer.format(node);                 // 不包含注释的SQL语句
            if(!commentList.isEmpty()) {
                formatSql = String.join("",commentList).concat(formatSql);      // 将注释归还给该SQL
            }
            beautifySqlList.add(formatSql);
        }
        return String.join(";\r\n\r\n",beautifySqlList).concat(";");
    }

    /**
     * 获取calcite parser
     *
     * @param tableConfig
     * @return
     */
    private CalciteParser getFlinkCalciteParser(TableConfig tableConfig){
        return new CalciteParser(getSqlParserConfig(tableConfig));
    }

    private SqlParser.Config getSqlParserConfig(TableConfig tableConfig) {
        return JavaScalaConversionUtil.<SqlParser.Config>toJava(
                getCalciteConfig(tableConfig).getSqlParserConfig())
                .orElseGet(
                        // we use Java lex because back ticks are easier than double quotes in
                        // programming
                        // and cases are preserved
                        () -> {
                            SqlConformance conformance = getSqlConformance(tableConfig.getSqlDialect());
                            return SqlParser.configBuilder()
                                    .setParserFactory(FlinkSqlParserFactories.create(conformance))
                                    .setConformance(conformance)
                                    .setLex(Lex.JAVA)
                                    .setIdentifierMaxLength(256)
                                    .build();
                        });
    }

    private CalciteConfig getCalciteConfig(TableConfig tableConfig) {
        return TableConfigUtils.getCalciteConfig(tableConfig);
    }

    private FlinkSqlConformance getSqlConformance(SqlDialect sqlDialect) {
        switch (sqlDialect) {
            case HIVE:
                return FlinkSqlConformance.HIVE;
            case DEFAULT:
                return FlinkSqlConformance.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }

    /**
     * 处理SQL
     * @param sql sql语句
     * @return
     */
    private String processSql(String sql) {
        return sql.replaceAll("--.*\r\n", "\r\n").replaceAll("--.*\n","\n").replaceAll("/\\*.*?\\*/","")  // 移除注释
                .replaceAll("\r\n"," ").replaceAll("\n"," ");     // 移除换行符
    }
}
