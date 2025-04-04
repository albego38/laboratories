SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[FactTransactions] (
    [transactionId] NVARCHAR(50) NOT NULL,
    [userId] NVARCHAR(50) NOT NULL,
    [timestamp] DATETIME2 NOT NULL,
    [amount] MONEY NOT NULL,
    [currency] NVARCHAR(3) NOT NULL,
    [transactionType] NVARCHAR(50) NOT NULL,
    [merchantName] NVARCHAR(255) NOT NULL,
    [merchantCategory] NVARCHAR(100) NOT NULL,
    [paymentMethod] NVARCHAR(50) NOT NULL,
    [transactionStatus] NVARCHAR(50) NOT NULL,
    [deviceType] NVARCHAR(50) NOT NULL,
    [deviceOs] NVARCHAR(50) NOT NULL,
    [deviceIpAddress] NVARCHAR(50) NULL,
    [idCity] INT NOT NULL,
    [userLocationCity] NVARCHAR(100) NOT NULL,
    [userLocationCountry] NVARCHAR(100) NOT NULL,
    [userLocationLatitude] DECIMAL(9,6) NOT NULL,
    [userLocationLongitude] DECIMAL(9,6) NOT NULL
);
