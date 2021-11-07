package com.nero.geektime;

import java.io.Serializable;

public class Transactions implements Serializable {
    private static final long serialVersionUID = 1834879532291132590L;
    private Long accountId;
    private Long amount;
    private String transactionTime;

    public Transactions(Long accountId, Long amount, String transactionTime) {
        this.accountId = accountId;
        this.amount = amount;
        this.transactionTime = transactionTime;
    }

    public Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public String getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(String transactionTime) {
        this.transactionTime = transactionTime;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "accountId=" + accountId +
                ", amount=" + amount +
                ", transactionTime='" + transactionTime + '\'' +
                '}';
    }
}
