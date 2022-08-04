-- 12.	List all the Order Detail (Stock Item name, delivery address, 
-- delivery state, city, country, customer name, 
-- customer contact person name, customer phone, quantity) 
-- for the date of 2014-07-01. Info should be relevant to that date.

SELECT Warehouse.StockItems.StockItemName,Sales.Orders.OrderDate,Sales.Customers.DeliveryAddressLine1,Sales.Customers.DeliveryAddressLine2
FROM
Warehouse.StockItems
LEFT JOIN
Sales.Orders
ON Warehouse.StockItems.SupplierID = Sales.Orders.SalespersonPersonID
INNER JOIN
Sales.Customers
ON Sales.Orders.SalespersonPersonID = Sales.Customers.CustomerID
WHERE
Orders.OrderDate = '2014-07-01'