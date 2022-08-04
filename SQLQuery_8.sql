SELECT DISTINCT StateProvinceName, DateDiff(MONTH, OrderDate, ExpectedDeliveryDate) As Average_DeliveryMonth
FROM Sales.Orders 
INNER JOIN Sales.Customers ON Sales.Orders.CustomerID = Sales.Customers.CustomerID
INNER JOIN Application.StateProvinces ON Sales.Orders.LastEditedBY = Application.StateProvinces.LastEditedBY
ORDER BY StateProvinceName ASC;