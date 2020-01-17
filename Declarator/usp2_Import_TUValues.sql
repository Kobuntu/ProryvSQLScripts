



--переименоввываем.. в usp2_Import_InegralValues
IF OBJECT_ID('usp2_Import_TUValues', 'P') IS NOT NULL
    DROP PROCEDURE usp2_Import_TUValues;
GO


if(exists (select top 1 1 from systypes where name like 'ImportedTUValueTableType'))
	DROP Type ImportedTUValueTableType
GO 