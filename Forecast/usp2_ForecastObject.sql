if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ForecastObject')
          and type in ('P','PC'))
   drop procedure usp2_ForecastObject
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
--		Все объект прогнозирования, со своими параметрами
--
-- ======================================================================================
create proc [dbo].[usp2_ForecastObject]
	@forecastObjectUn nvarchar(22) --идентификатор объекта
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Все объекты прогнозирования
select o.*,ForecastObjectType_ID from Forecast_Objects o
left join [dbo].[Forecast_Objects_To_ObjectTypes] ot on ot.ForecastObject_UN = o.ForecastObject_UN
where o.ForecastObject_UN = @forecastObjectUn

--Все дочерние объекты
--select ForecastObject_UN from Forecast_Hierarchy where ParentForecastObject_UN = @forecastObjectUn

--Модели прогнозирования для этого объекта
select ForecastCalculateModel_ID 
from Forecast_CalculateModels_To_ObjectTypes cm
where cm.ForecastObjectType_ID = (select top 1 ForecastObjectType_ID from [dbo].[Forecast_Objects_To_ObjectTypes]
where ForecastObject_UN = @forecastObjectUn)

--ТИ, ТП или формула связанная с этим объектом прогнозирования
select top 1 pv.* from [dbo].[Forecast_InputParams_To_PhysicalValues] ipt
join [dbo].[Forecast_PhysicalValues] pv on pv.ForecastPhysicalValue_UN = ipt.ForecastPhysicalValue_UN
where [ForecastObject_UN] = @forecastObjectUn

end
go
   grant EXECUTE on usp2_ForecastObject to [UserCalcService]
go