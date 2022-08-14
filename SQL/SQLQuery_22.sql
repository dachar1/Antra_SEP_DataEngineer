CREATE SCHEMA obs;
SELECT 
	[StockItemID], [StockItemName], [SupplierID], [ColorID], [UnitPackageID], [OuterPackageID], [Brand], [Size], [LeadTimeDays], [QuantityPerOuter],
	[IsChillerStock], [Barcode], [TaxRate], [UnitPrice], [RecommendedRetailPrice], [TypicalWeightPerUnit], [MarketingComments], [InternalComments],
	JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS [CountryOfManufacture], JSON_VALUE(CustomFields, '$.Range') AS [Range],
	JSON_VALUE(CustomFields, '$.ShelfLife') AS [ShelfLife]
INTO
	obs.StockItems
FROM
	Warehouse.StockItems

SELECT * FROM obs.StockItems;