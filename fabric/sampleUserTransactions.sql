CREATE TABLE transactionsByUser (
    userId INT,
    firstName VARCHAR(50),
    transactions INT
);

INSERT INTO transactionsByUser (userId, firstName, transactions) VALUES
(1, 'Ana', 5),
(2, 'Luis', 3),
(3, 'María', 7),
(4, 'Carlos', 2);
