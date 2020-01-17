if not exists (select 1
          from sysobjects
          where  id = object_id('usf2_Settings_UseLossesCoefficients')
          and type in ('IF', 'FN', 'TF'))
begin 
	--разрешить использовать галку "домножить данные на коэффициент потерь"
	DECLARE @sqltxt varchar(100)
    SET @sqltxt = 'CREATE function dbo.usf2_Settings_UseLossesCoefficients() returns bit AS  begin RETURN 0 end'
    EXEC(@sqltxt)	
end
go 

grant EXEC on  usf2_Settings_UseLossesCoefficients  to [UserCalcService]
go
grant EXEC on  usf2_Settings_UseLossesCoefficients  to [UserDeclarator]
go
grant EXEC on  usf2_Settings_UseLossesCoefficients  to [UserExportService]
go
grant EXEC on  usf2_Settings_UseLossesCoefficients  to [UserImportService]
go
