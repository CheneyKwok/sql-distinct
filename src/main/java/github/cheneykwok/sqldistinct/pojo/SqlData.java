package github.cheneykwok.sqldistinct.pojo;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;

/**
 * @author gzc
 * @date 2023-11-06
 */
@Data
public class SqlData {

    @ExcelProperty("SQL 语句")
    private String sql;

    @ExcelProperty("执行时间（s）")
    private String costTime;

//    @ExcelProperty("返回行数")
//    private Integer returnRows;
}
