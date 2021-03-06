--Создаем тип, если его нет
IF  NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ForecastWriterType' AND ss.name = N'dbo') BEGIN
	CREATE TYPE [dbo].[ForecastWriterType] AS TABLE(
	[ForecastObject_UN] varchar(22) NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[User_ID] varchar(22) NOT NULL,
	[DispatchDateTime] DateTime NOT NULL,
	[AUTOBITS] [bigint],
	[MANUALBITS] [bigint],
	[FACTBITS] [bigint]
)
END
go

grant EXECUTE on TYPE::ForecastWriterType to [UserCalcService]
go

set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Forecast_UserHasRight_ForecastFix')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Forecast_UserHasRight_ForecastFix
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2017
--
-- Описание:
--
--		Определяем права на сохранение/фиксацию данных по объекту прогнозирования
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Forecast_UserHasRight_ForecastFix] (
		@forecastWriteTable ForecastWriterType READONLY, --Таблицу которую пишем в базу данных
		@rightId uniqueidentifier,
		@treeID int = null
)

 RETURNS nvarchar(max) 
	AS BEGIN

		declare @objectUnRight nvarchar(max);
		set @objectUnRight = '';

		select distinct @objectUnRight = @objectUnRight + o.ForecastObjectName + ', '
		from @forecastWriteTable s
		join Forecast_Objects o on o.ForecastObject_UN = s.ForecastObject_UN
		where [dbo].[usf2_UserHasRight](s.User_ID, @rightId, s.ForecastObject_UN, 'Forecast_Objects', 29, @treeID, null) = 0

		return @objectUnRight;
	
	RETURN 0;
END;
go
grant EXECUTE on usf2_Forecast_UserHasRight_ForecastFix to [UserCalcService]
go

----------------------------------------------------------------------------------

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_UserHasRight_PlanTimeRules')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_UserHasRight_PlanTimeRules
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
--		Март, 2017
--
-- Описание:
--
--		Определяем разрешено ли по времени сохранение/фиксация данных по объекту прогнозирования
--
-- ======================================================================================
create PROCEDURE  [dbo].[usp2_Forecast_UserHasRight_PlanTimeRules] 
		@forecastWriteTable ForecastWriterType READONLY, --Таблицу которую пишем в базу данных
		@dntWriteManualList nvarchar(max) OUTPUT, @dntWriteFactList nvarchar(max) OUTPUT
AS
BEGIN

set @dntWriteManualList = '';
set @dntWriteFactList = ''

select @dntWriteManualList = @dntWriteManualList + (case when CanWriteManual = 1 then '' else  ('<' + o.ForecastObjectName + '> разрешено сохранять с ' + convert(varchar, resolvedPlanDate, 104) + ', ') end),
@dntWriteFactList = @dntWriteFactList + (case when CanWriteFact = 1 then '' else  ('<' + o.ForecastObjectName + '> разрешено сохранять с ' + convert(varchar, resolverFactDate, 104) + ' по ' + convert(varchar, DispatchDateTime, 104) + ', ')  end)
from 
	(
		select t.ForecastObject_UN,
		case when t.MANUALBITS > 0 then --Определяемся что у нас EventDate больше (DispatchDateTime + дельта)
		 case when t.EventDate >=resolvedPlanDate then 1 else 0 end else 1 end as CanWriteManual, --Разница в минутах между разрешенным временем и текущим для плана

		case when t.FACTBITS > 0 then --Определяемся что у нас EventDate между (DispatchDateTime - дельта) и DispatchDateTime
		 case when  t.EventDate between
			  resolverFactDate and DispatchDateTime then 1 else 0 end else 1 end as CanWriteFact, --Разница в минутах между разрешенным временем и текущим для факта
		resolvedPlanDate, --Это дата с которой можно писать план
		resolverFactDate, --Это дата с которой можно писать факт до текущего момента времени
		DispatchDateTime
		from (
				select t.ForecastObject_UN,t.EventDate, t.MANUALBITS,t.FACTBITS, cast(floor(cast(t.DispatchDateTime as float)) as DateTime) as DispatchDateTime, cast(floor(cast(DATEADD(minute,
					ISNULL(re.MaxMinutesPlanEventTime, ISNULL(r.MaxMinutesPlanEventTime, 
						(select top 1 MaxMinutesPlanEventTime from Forecast_Objects where ForecastObject_UN = t.ForecastObject_UN))),
						t.DispatchDateTime) as float)) as DateTime) as resolvedPlanDate,
				cast(floor(cast(DATEADD(minute,
					-ISNULL(re.MaxMinutesEventTimeFact, ISNULL(r.MaxMinutesEventTimeFact, 
						(select top 1 MaxMinutesEventTimeFact from Forecast_Objects where ForecastObject_UN = t.ForecastObject_UN))),
						t.DispatchDateTime) as float)) as DateTime) as resolverFactDate
				from @forecastWriteTable t
				left join [dbo].[Forecast_Objects_PlanTimeRules_Exceptions] re on re.ForecastObject_UN = t.ForecastObject_UN and re.EventDate = t.EventDate
				outer apply
				(
					select top 1 * from [dbo].[Forecast_Objects_PlanTimeRules] r  
					where r.ForecastObject_UN = t.ForecastObject_UN and r.StartDate <= t.EventDate and (r.FinishDate is null or r.FinishDate >= t.EventDate)
					order by r.StartDate
				) r
			)  t
) f 
join Forecast_Objects o on o.ForecastObject_UN = f.ForecastObject_UN
where CanWriteManual = 0 or CanWriteFact = 0

END;
go
   grant EXECUTE on usp2_Forecast_UserHasRight_PlanTimeRules to [UserCalcService]
go