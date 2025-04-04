SELECT
    transactionStatus, COUNT(*) as countStatus
FROM
    OPENROWSET(
        BULK 'https://SUSTITUIR.dfs.core.windows.net/datalake/bronze/transactions.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
    GROUP by transactionStatus;
