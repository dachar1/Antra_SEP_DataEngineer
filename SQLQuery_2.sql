SELECT CustomerName
FROM
(
    SELECT PersonID, PhoneNumber FROM Application.People
    UNION
    SELECT PersonID, PhoneNumber FROM Application.People_Archive
)
AS People

JOIN
(
	SELECT CustomerName, PrimaryContactPersonID, PhoneNumber FROM Sales.Customers
	UNION
	SELECT CustomerName, PrimaryContactPersonID, PhoneNumber FROM Sales.Customers_Archive
	) as Company
on People.PersonID = Company.PrimaryContactPersonID where People.PhoneNumber = Company.PhoneNumber;