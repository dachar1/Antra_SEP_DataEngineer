SELECT Warehouse.StockItems.StockItemName,Warehouse.StockItems.QuantityPerOuter
FROM Warehouse.StockItems
INNER JOIN
Purchasing.PurchaseOrders ON Warehouse.StockItems.SupplierID=Purchasing.PurchaseOrders.SupplierID
WHERE YEAR(OrderDate) ='2013'
