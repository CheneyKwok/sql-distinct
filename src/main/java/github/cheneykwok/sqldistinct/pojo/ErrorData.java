package github.cheneykwok.sqldistinct.pojo;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gzc
 * @date 2023-11-07
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorData {

    @ExcelProperty("行数")
    private Integer rowCount;

    @ExcelProperty("错误Sql")
    private String errorSql;

    @ExcelProperty("错误日志")
    private String error;
}
