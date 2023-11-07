package github.cheneykwok.sqldistinct.support;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.excel.support.ExcelTypeEnum;
import com.alibaba.fastjson.JSON;
import github.cheneykwok.sqldistinct.pojo.ErrorData;
import github.cheneykwok.sqldistinct.pojo.SqlData;
import github.cheneykwok.sqldistinct.pojo.SqlParseData;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.springframework.util.StringUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author gzc
 * @date 2023-11-06
 */
@Slf4j
@Data
public class SqlDataListener implements ReadListener<SqlData> {

    private final String outputFile;

    private boolean lastExcel = true;

    private static final int BATCH_COUNT = 1000;

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 16, 5, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    private final Map<SqlParseData, SqlData> sqlMap = new ConcurrentHashMap<>();

    private final List<ErrorData> errorDataList = new CopyOnWriteArrayList<>();

    private final List<CompletableFuture<Void>> tasks = new ArrayList<>();

    private final LongAdder counter = new LongAdder();

    private final String selectKw = "SELECT";

    private final String spaceChar = "\\s";

    public SqlDataListener(String outputFile) {
        this.outputFile = outputFile;
    }

    @Override
    public void invoke(SqlData sqlData, AnalysisContext analysisContext) {
        CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
            try {
                if (!StringUtils.hasLength(sqlData.getSql())) {
                    return;
                }
                counter.increment();
                String sql = sqlData.getSql();
                sql = sql.replaceAll(spaceChar, "");
                log.info("解析到 {} 条数据：{}", counter.intValue(), JSON.toJSONString(sql));
                if (!sql.startsWith(selectKw) && !sql.startsWith(selectKw.toLowerCase())) {
                    return;
                }
                PlainSelect select = (PlainSelect) CCJSqlParserUtil.parse(sqlData.getSql());
                SqlParseData sqlParseData = new SqlParseData(select);
                sqlMap.put(sqlParseData, sqlData);
            } catch (Exception e) {

                ErrorData errorData = new ErrorData(counter.intValue(), sqlData.getSql(), e.toString());
                errorDataList.add(errorData);
                log.error("第 {} 行解析sql失败，详情请查看 error.csv", counter.intValue());
            }
        }, executor);
        tasks.add(task);
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        if (lastExcel) {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

            List<SqlData> values = new ArrayList<>(sqlMap.values());
            EasyExcel.write(outputFile, SqlData.class)
                    .excelType(ExcelTypeEnum.CSV)
                    .sheet("sheet1")
                    .doWrite(values);
            if (!errorDataList.isEmpty()) {
                errorDataList.add(new ErrorData(1, "1", "1"));
                EasyExcel.write(System.getProperty("user.dir") + File.separator + "error.csv", ErrorData.class)
                        .excelType(ExcelTypeEnum.CSV)
                        .sheet("sheet1")
                        .doWrite(errorDataList);
            }
            executor.shutdown();
            log.info("执行结束，总解析行数：{}, 去重后行数：{}", counter.intValue(), values.size());
        }
    }

    public static void main(String[] args) throws JSQLParserException {
        String sqlStr = "SELECT\n" +
                "            tudj.join_id                                    AS tudjjoin_id,\n" +
                "            tu.user_inner_id                                AS user_inner_id,\n" +
                "            tu.user_avatar                                  AS tuuser_avatar,\n" +
                "            IFNULL( IF(LENGTH( tu.user_name )>0, tu.user_name, NULL), IF (LENGTH(tu.user_nickname)> 0, tu.user_nickname, NULL ) )         AS tuuser_name,\n" +
                "            tu.user_phone                                   AS tuuser_phone,\n" +
                "            tudj.user_status                                AS tudjuser_status,\n" +
                "            0                                               AS ischannel,\n" +
                "\n" +
                "            tu.user_gender                                  AS tuuser_gender,\n" +
                "            DATE_FORMAT( tu.user_birthday, '%Y-%m-%d' )     AS tuuser_birthday,\n" +
                "            tu.create_date                                  AS tucreate_date,\n" +
                "            tust.user_source_name                           AS tustuser_source_name,\n" +
                "\n" +
                "            tudj.member_card_number                         AS tudjmember_card_number,\n" +
                "            tudj.member_create_date                         AS tudjmember_create_date,\n" +
                "            tudj.member_level                               AS tudjmember_level,\n" +
                "\n" +
                "            tudj.last_consumption_date                      AS tudjlast_consumption_date,\n" +
                "            tudj.total_cost                                 AS tudjtotal_cost,\n" +
                "            tudj.actually_paid                              AS tudjactually_paid,\n" +
                "            tudj.cost_count                                 AS tudjcost_count,\n" +
                "            tudj.customer_price                             AS tudjcustomer_price,\n" +
                "            tudj.total_refund                               AS tudjtotal_refund,\n" +
                "            tudj.refund_count                               AS tudjrefund_count,\n" +
                "            tudj.seller_create_date                         AS tudjseller_create_date,\n" +
                "            tea.tea_name                                    AS other1,\n" +
                "            tea.tea_phone                                   AS tea_phone,\n" +
                "            ts.store_name                                   AS tudjstore_id,\n" +
                "\n" +
                "            tudj.store_id                                   AS store_id,\n" +
                "            tust.user_source_id                             AS user_source_id,\n" +
                "            ts.dealer_id                                    AS dealer_id,\n" +
                "            tudj.assistant_id                               AS assistant_id,\n" +
                "            tudj.last_access_time                           AS last_access_time,\n" +
                "\n" +
                "            tudj.seller_level AS tudjseller_level,\n" +
                "            IF ( tudj.belong_seller = - 1, NULL, tudj.belong_seller ) AS tudjbelong_seller,\n" +
                "            (SELECT member_level_name FROM tenant_member_card_option WHERE member_level = tudj.member_level) AS memberLevelName\n" +
                "        FROM\n" +
                "            tenant_user_dealer_join tudj\n" +
                "            LEFT JOIN tenant_user tu ON tu.user_inner_id = tudj.user_id\n" +
                "            LEFT JOIN tenant_store ts ON tudj.store_id = ts.store_inner_id\n" +
                "            LEFT JOIN tenant_user_source_type tust ON tu.user_source_id = tust.user_source_id\n" +
                "            AND tust.is_del = 0\n" +
                "            LEFT JOIN tenant_employee_account tea ON tudj.assistant_id = tea.tea_id\n" +
                "            LEFT JOIN ( SELECT tust.user_inner_id, tust.user_store_track_mode FROM tenant_user_store_track tust GROUP BY tust.user_inner_id ) tusts ON tu.user_inner_id = tusts.user_inner_id\n" +
                "         WHERE tudj.join_id <= 851052\n" +
                "            AND tu.is_del = 0\n" +
                "            \n" +
                "            \n" +
                "                \n" +
                "\n" +
                "                        AND tu.user_phone LIKE CONCAT( '%', '13468936213', '%' ) \n" +
                "        GROUP BY\n" +
                "            tudj.join_id\n" +
                "        ORDER BY\n" +
                "            tudj.join_id DESC\n" +
                "        LIMIT 10";
        sqlStr = "select count(1) from user a left join b On a.id = b.id and a.is_del = 1 and (a.name = 'a' OR b.age = 1) group by a.id = 1";
        PlainSelect select1 = (PlainSelect) CCJSqlParserUtil.parse(sqlStr);
        String s = "select count(1) from user a left join b On a.id = b.id and a.is_del = 3 and (a.name = 'asfsfsd' OR a.id = 1) group by a.id = 3";
        PlainSelect select2 = (PlainSelect) CCJSqlParserUtil.parse(s);
        System.out.println(new SqlParseData(select1).equals(new SqlParseData(select2)));

    }


}
