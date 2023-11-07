package github.cheneykwok.sqldistinct;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.support.ExcelTypeEnum;
import github.cheneykwok.sqldistinct.pojo.SqlData;
import github.cheneykwok.sqldistinct.support.SqlDataListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.File;

/**
 * @author gzc
 */
@SpringBootApplication
public class SqlDistinctApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SqlDistinctApplication.class, args);
        ResourcePatternResolver resourceLoader = new PathMatchingResourcePatternResolver();
        Resource[] resources = resourceLoader.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + "/excel/*.csv");

        String outputFile = System.getProperty("user.dir") + File.separator + "sql.csv";
        SqlDataListener sqlDataListener = new SqlDataListener(outputFile);
        sqlDataListener.setLastExcel(false);
        int counter = 0;
        for (Resource resource : resources) {
            if (++counter == resources.length) {
                sqlDataListener.setLastExcel(true);
            }
            String path = resource.getFile().getAbsolutePath();
            EasyExcel.read(path, SqlData.class, sqlDataListener)
                    .excelType(ExcelTypeEnum.CSV)
                    .sheet()
                    .doRead();
        }
    }

}
