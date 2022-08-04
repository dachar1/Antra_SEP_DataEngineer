SELECT Warehouse.StockItems.StockItemName, Warehouse.StockItemTransactions.Quantity, Warehouse.StockItemHoldings.LastStocktakeQuantity
FROM
Warehouse.StockItems
INNER JOIN
Warehouse.StockItemTransactions
ON
Warehouse.StockItems.StockItemID=Warehouse.StockItemTransactions.StockItemID
INNER JOIN
Warehouse.StockItemHoldings
ON
Warehouse.StockItemTransactions.StockItemID=Warehouse.StockItemHoldings.StockItemID
WHERE YEAR(TransactionOccurredWhen)='2015'