package com.nero.geektime;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.$;

public class SpendReport {
    private static final String TRANSACTIONS_SQL = "CREATE TABLE transactions (\n" +
            " account_id BIGINT,\n" +
            " amount BIGINT,\n" +
            " transaction_time TIMESTAMP(3),\n" +
            " WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
            ") WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'transactions',\n" +
            " 'properties.bootstrap.servers' = 'kafka:9092',\n" +
            " 'format' = 'csv'\n" +
            ")";

    private static final String SPEND_REPORT_SQL = "CREATE TABLE spend_report (\n" +
            " account_id BIGINT,\n" +
            " log_ts TIMESTAMP(3),\n" +
            " amount BIGINT\n," +
            " PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
            ") WITH (\n" +
            " 'connector' = 'jdbc',\n" +
            " 'url' = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
            " 'table-name' = 'spend_report',\n" +
            " 'driver' = 'com.mysql.jdbc.Driver',\n" +
            " 'username' = 'sql-demo',\n" +
            " 'password' = 'demo-sql'\n" +
            ")";

    /*public static Table report(Table transactions) {
        return transactions;
    }

    public static void executeInsert(StreamTableEnvironment tEnv, Table transactions, String tableName) {
        DataStream<Transactions> result = tEnv.toAppendStream(transactions, Transactions.class);
        result.map(new MapFunction<Transactions, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Transactions result) throws Exception {
                Long accountId = result.getAccountId();
                Long amount = result.getAmount();
                String transactionTime = result.getTransactionTime();
                tEnv.sqlUpdate("insert into " + tableName + " values ("+accountId+","+transactionTime+","+amount+")");
                return Tuple2.of(accountId, amount);
            }
        });

        //表中的三个字段的数据不知道如何在TableSink组织，并生成什么格式的数据
        TableSink tableSink = new TableSink() {
            @Override
            public TableSink configure(String[] strings, TypeInformation[] typeInformations) {
                return null;
            }
        };
        //设置字段名
        String[] filedNames = {"account_id", "log_ts", "amount"};
        //设置字段类型
        TypeInformation[] filedTypes = {Types.LONG(), Types.STRING(), Types.LONG()};
        tEnv.registerTableSink("SQLTEXT", filedNames, filedTypes, tableSink);
        transactions.insertInto("SQLTEXT");
    }

    public static void main(String[] args) throws Exception {
        //register
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.enableCheckpointing(5000);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        tEnv.sqlUpdate(KAFKA_SQL);
        tEnv.sqlUpdate(SINK_KAFKA_SQL);

        Table transactions = report(tEnv.sqlQuery("select * from transactions"));
        executeInsert(tEnv, transactions, "spend_report");
        bsEnv.execute();
    }*/

    public static Table report(Table transactions) {
        return transactions.select(
                $("account_id"),
                $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                $("amount"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts"),
                        $("amount").sum().as("amount"));
    }


    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        //输入表transaction，用于读取信用卡交易数据，其中包含账户ID(account_id)，美元金额和时间戳
        tEnv.executeSql(TRANSACTIONS_SQL);
        //输出表spend_report存储聚合结果，是mysql表
        tEnv.executeSql(SPEND_REPORT_SQL);
        //将transactions表经过report函数处理后写入到spend_report表
        Table transactions = tEnv.from("transactions");
        report(transactions).executeInsert("spend_report");
    }

}
