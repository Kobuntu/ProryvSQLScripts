if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ForecastObjects')
          and type in ('P','PC'))
   drop procedure usp2_ForecastObjects
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2017
--
-- Описание:
--
--		Все объекты прогнозирования, со своими параметрами
--
-- ======================================================================================
create proc [dbo].[usp2_ForecastObjects]
	@forecastObjectUns varchar(max)
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
--Перебираем все объекты прогнозирования

declare @forecastObjectUn nvarchar(22)

select COUNT(*) from Forecast_Objects
where @forecastObjectUns is null or (@forecastObjectUns is not null and ForecastObject_UN in (select Item from usf2_Utils_SplitString(@forecastObjectUns, ',')))

--Все объекты прогнозирования
declare forecastCursor cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select ForecastObject_UN from Forecast_Objects
where @forecastObjectUns is null or (@forecastObjectUns is not null and ForecastObject_UN in (select Item from usf2_Utils_SplitString(@forecastObjectUns, ',')))
open forecastCursor;
FETCH NEXT FROM forecastCursor into @forecastObjectUn
WHILE @@FETCH_STATUS = 0
BEGIN
	exec dbo.usp2_ForecastObject @forecastObjectUn
	FETCH NEXT FROM forecastCursor into @forecastObjectUn
END

CLOSE forecastCursor
DEALLOCATE forecastCursor

end
go
   grant EXECUTE on usp2_ForecastObjects to [UserCalcService]
go