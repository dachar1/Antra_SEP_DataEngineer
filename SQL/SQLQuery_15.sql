SELECT 
	OrderID, JSON_VALUE(ReturnedDeliveryData, '$.Events[1].Event') AS Delivery_Attempt
FROM
	Sales.Invoices AS SI
WHERE OrderID IN (
	SELECT OrderID
	FROM Sales.Invoices
	GROUP BY OrderID
	HAVING COUNT(OrderID) > 1
)
ORDER BY SI.OrderID;
