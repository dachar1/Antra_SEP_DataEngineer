SELECT DISTINCT Sales.Customers.CustomerName, Sales.CustomerTransactions.TransactionDate
FROM Sales.Customers
INNER JOIN
Sales.CustomerTransactions ON Sales.Customers.CustomerID = Sales.CustomerTransactions.CustomerID
WHERE Sales.CustomerTransactions.TransactionDate <= '2016-01-01' AND Sales.CustomerTransactions.TransactionDate !='2016-01-01'
ORDER BY TransactionDate;

SELECT DISTINCT Sales.Customers.CustomerName, Sales.CustomerTransactions.TransactionDate
FROM Sales.Customers
INNER JOIN
Sales.CustomerTransactions ON Sales.Customers.CustomerID = Sales.CustomerTransactions.CustomerID
WHERE Sales.CustomerTransactions.TransactionDate < '2016-01-01' 
ORDER BY TransactionDate;
