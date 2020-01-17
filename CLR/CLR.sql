 if (exists (select top 1 1 from sysobjects where (xtype like 'FS' or xtype like 'F%') and name like 'usf2_CLR_Generate_NewID'))
drop FUNCTION usf2_CLR_Generate_NewID
go
 
 
CREATE FUNCTION usf2_CLR_Generate_NewID
(@Object_Type int)
RETURNS [nchar](22)
AS
EXTERNAL NAME [Proryv.CLR.Common].[Proryv.CLR.Common.Functions.ProryvCLR].sfclr_Generate_NewID
go

GRANT EXECUTE ON [dbo].usf2_CLR_Generate_NewID TO UserCalcService
go
GRANT EXECUTE ON [dbo].usf2_CLR_Generate_NewID TO UserDeclarator
go



if (exists (select top 1 1 from sysobjects where (xtype like 'FS' or xtype like 'F%') and name like 'usf2_CLR_Get_Rights'))
drop FUNCTION usf2_CLR_Get_Rights
go  
 
create FUNCTION dbo.usf2_CLR_Get_Rights()
RETURNS table (ID nvarchar(200),
StringName nvarchar(200),
Description nvarchar(200),
RightBindType tinyint)
as
EXTERNAL NAME [Proryv.CLR.Common].[Proryv.CLR.Common.Functions.ProryvCLR].sfclr_Get_Rights
go

GRANT select ON [dbo].usf2_CLR_Get_Rights TO UserCalcService
go
GRANT select ON [dbo].usf2_CLR_Get_Rights TO UserDeclarator
go

