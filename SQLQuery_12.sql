SELECT
	ws.StockItemName, si.DeliveryInstructions, ac.CityName, asp.StateProvinceName, act.CountryName, 
    sc.CustomerName, ap.FullName, sc.PhoneNumber, sol.Quantity
FROM
	Sales.Orders AS so, Sales.OrderLines AS sol,
	Sales.Invoices AS si, Sales.Customers AS sc,
	Warehouse.StockItems AS ws, Application.People AS ap,
	Application.Cities AS ac, Application.StateProvinces AS asp,
	Application.Countries As act

WHERE
	so.OrderID = sol.OrderID
	AND si.OrderID = so.OrderID
	AND sol.StockItemID = ws.StockItemID
	AND so.CustomerID = sc.CustomerID
	AND so.ContactPersonID = ap.PersonID
	AND sc.DeliveryCityID = ac.CityID
	AND ac.StateProvinceID = asp.StateProvinceID
	AND asp.CountryID = act.CountryID
	AND so.OrderDate = '2014-07-01';