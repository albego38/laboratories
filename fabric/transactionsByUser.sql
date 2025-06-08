SELECT
    u.userId,
    u.firstName,
    COUNT(*) AS transactions
FROM 
    [LKH_MDE].[silver].[users] u
JOIN 
    [LKH_MDE].[silver].[h_transactions] h ON u.userId = h.userId
GROUP BY 
    u.userId, u.firstName
ORDER BY 
    transactions DESC;
