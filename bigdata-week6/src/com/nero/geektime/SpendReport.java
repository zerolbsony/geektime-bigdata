package com.nero.geektime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

public class SpendReport {
    private static final String KAFKA_SQL = "CREATE TABLE transactions (\n" +
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

    private static final String SINK_KAFKA_SQL = "CREATE TABLE spend_report (\n" +
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

    public static Table report(Table transactions) {
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
    }

}
