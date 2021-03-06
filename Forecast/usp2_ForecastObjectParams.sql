if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ForecastObjectParams')
          and type in ('P','PC'))
   drop procedure usp2_ForecastObjectParams
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
--		Конфигурация объектов для обсчета прогноза по этим объектам
--
-- ======================================================================================
create proc [dbo].[usp2_ForecastObjectParams]

	@forecastObjectUns varchar(max), --Идентификаторы объектов
	@dtStart DateTime, -- Начальная дата
	@dtEnd DateTime -- Конечная
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select Item as ForecastObject_UN
into #objects
from usf2_Utils_SplitString(@forecastObjectUns, ',')

--Результат 1 (usp2_ForecastObjectParamsResult1)
select o.ForecastObject_UN, ForecastCalculateModel_ID, [Priority] from #objects o
left join Forecast_Objects_To_ObjectTypes oto on oto.ForecastObject_UN = o.ForecastObject_UN
left join Forecast_CalculateModels_To_ObjectTypes omto on omto.ForecastObjectType_ID = oto.ForecastObjectType_ID
order by o.ForecastObject_UN, [Priority]

--Результат 2 (usp2_ForecastObjectParamsResult2)
select o.ForecastObject_UN, itp.ForecastInputParam_UN, p.TI_ID, p.TP_ID, p.Formula_UN, p.UANode_ID, 
p.ChannelType, StartDateTime, FinishDateTime, ip.MeasureQuantityType_UN
from #objects o 
join Forecast_InputParams_To_PhysicalValues itp on itp.ForecastObject_UN = o.ForecastObject_UN
join Forecast_PhysicalValues p on p.ForecastPhysicalValue_UN = itp.ForecastPhysicalValue_UN
join Forecast_InputParams ip on ip.ForecastInputParam_UN = itp.ForecastInputParam_UN
WHERE (itp.StartDateTime is null or itp.StartDateTime <= @dtEnd) and (itp.FinishDateTime is null or itp.FinishDateTime >= @dtStart)
order by o.ForecastObject_UN, itp.ForecastInputParam_UN, itp.ForecastPhysicalValue_UN, itp.StartDateTime, itp.FinishDateTime

end
go
   grant EXECUTE on usp2_ForecastObjectParams to [UserCalcService]
go