SELECT Warehouse.StockItems.StockItemName, Warehouse.StockItemTransactions.TransactionOccurredWhen, Application.StateProvinces.StateProvinceName
FROM ((Warehouse.StockItems
INNER JOIN Warehouse.StockItemTransactions ON Warehouse.StockItems.StockItemID = Warehouse.StockItemTransactions.StockItemID)
INNER JOIN Application.StateProvinces ON Warehouse.StockItems.LastEditedBY = Application.StateProvinces.LastEditedBY)
WHERE YEAR(TransactionOccurredWhen)='2014' 
AND NOT Application.StateProvinces.StateProvinceName  ='Alabama'
AND NOT Application.StateProvinces.StateProvinceName  ='Georgia'
