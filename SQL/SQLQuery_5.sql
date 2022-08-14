SELECT StockItemName FROM 
Warehouse.StockItems
WHERE LEN (Warehouse.StockItems.StockItemName) > 10;
