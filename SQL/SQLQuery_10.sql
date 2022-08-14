SELECT Sales.Customers.CustomerName, Sales.Customers.PhoneNumber,Application.People.FullName AS Primay_Contact
FROM 
Sales.Customers
INNER JOIN
Application.People
ON
Sales.Customers.PrimaryContactPersonID= Application.People.PersonID
INNER JOIN
Warehouse.StockItems
ON Warehouse.StockItems.LastEditedBy=Application.People.LastEditedBy
RIGHT JOIN 
Warehouse.StockItemTransactions
ON Warehouse.StockItemTransactions.StockItemID=Warehouse.StockItems.StockItemID
WHERE Warehouse.StockItems.StockItemName  LIKE '%mug%'
AND 
Warehouse.StockItems.QuantityPerOuter <=10
AND YEAR(TransactionOccurredWhen)='2016';
