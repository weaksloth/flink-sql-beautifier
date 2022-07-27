package top.yangc.flink.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SqlUtils {

    public static final String SQL_COMMENT = "--";
    public static final String SEMICOLON = ";";

    /**
     * Reads a sql file from local
     * @param inputStream sql文件流
     */
    public static List<String> readSqlFile(InputStream inputStream) throws IOException {
        List<String> sqlList = new ArrayList<>();

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        StringBuilder stringBuilder = new StringBuilder();

        while ((line = br.readLine()) != null ) {
            // Ignores empty line and comment line
            if (isEmptyLine(line) || isCommentLine(line)) {
                continue;
            }

            // Removes comment(s)
            if (line.contains(SQL_COMMENT)) {
                line = line.substring(0, line.indexOf(SQL_COMMENT));
            }

            // 处理逗号
            if (line.contains(SEMICOLON)) {
                int commaIndex = line.indexOf(SEMICOLON);
                // remove ';' at the end
                String lineWithoutComma = line.substring(0, commaIndex).trim();
                String sqlWithoutComma = stringBuilder.append(lineWithoutComma).toString();
                // 移除多余的空白
                String stmt = sqlWithoutComma.replaceAll("\\s{2,}", " ");
                sqlList.add(stmt);
                stringBuilder = new StringBuilder();
            } else {
                // 每行默认增加一个空格
                stringBuilder.append(line).append(" ");
            }
        }

        return sqlList;
    }

    /**
     * 判断是否为空行
     */
    private static boolean isEmptyLine(String line) {
        return line == null || "".equals(line.trim());
    }

    /**
     * 判断是否为注释行
     */
    private static boolean isCommentLine(String line) {
        return line != null && line.trim().startsWith(SQL_COMMENT);
    }
}
