SELECT StateProvinceName, OrderDate , 
DateDiff(Day, OrderDate, ExpectedDeliveryDate) As Average_DeliveryDate
from Sales.Orders 
INNER JOIN Sales.Customers ON Sales.Orders.CustomerID = Sales.Customers.CustomerID
INNER JOIN Application.StateProvinces ON Sales.Orders.LastEditedBY = Application.StateProvinces.LastEditedBY;