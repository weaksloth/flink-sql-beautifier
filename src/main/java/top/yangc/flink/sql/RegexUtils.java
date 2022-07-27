package top.yangc.flink.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexUtils {

    private List<String> tempList;
    private String tempStr;

    private RegexUtils() {
        this.tempList = new ArrayList<>();
        this.tempStr = null;
    }

    public static RegexUtils init() {
        return new RegexUtils();
    }

    /**
     * 基于text做正则表达式，只取符合正则条件的一个结果默认是最开始被匹配到的那个
     *
     * @param text
     * @param regex
     * @return
     */
    public RegexUtils regexOne(String text, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            this.tempStr = matcher.group();
            break;
        }
        return this;
    }

    /**
     * 基于上一次的结果做正则表达式,只取符合正则条件的一个结果默认是最开始被匹配到的那个
     *
     * @param regex
     * @return
     */
    public RegexUtils regexOne(String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(tempStr);
        while (matcher.find()) {
            this.tempStr = matcher.group();
            break;
        }
        return this;
    }

    public RegexUtils regexList(String text, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            this.tempList.add(matcher.group());
        }
        return this;
    }


    public String get() {
        return tempStr;
    }

    public Integer getInteger() {
        return Integer.parseInt(tempStr);
    }

    public Double getDouble() {
        return Double.parseDouble(tempStr);
    }

    /**
     * 获取所有匹配的结果
     *
     * @return
     */
    public List<String> getList() {
        return tempList;
    }

    /**
     * 返回int集合
     *
     * @return
     */
    public List<Integer> getIntegerList() {
        return tempList.stream().map(Integer::parseInt).collect(Collectors.toList());
    }

    /**
     * 返回double集合
     *
     * @return
     */
    public List<Double> getDoubleList() {
        return tempList.stream().map(Double::parseDouble).collect(Collectors.toList());
    }
}
