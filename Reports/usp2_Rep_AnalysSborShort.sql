if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_AnalysSborShort')
          and type in ('P','PC'))
   drop procedure usp2_Rep_AnalysSborShort
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
--		Май, 2012
--
-- Описание:
--
--		Для отчета анализ сбора данных укрупненный по периоду 
--
-- ======================================================================================
create proc [dbo].[usp2_Rep_AnalysSborShort]
@StartDT DateTime, @EndDT DateTime
as
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	select TI_ID, ИмяПлощадки
	--Сокращаем статистику (0 - нет данных, 1 - частичное присутствие данных, 2 - полное присутствие данных)
	,cast(floor((AVG(IsHavingIntegralOnEventDate) * 2) / 48) as tinyint) as IntegralStatistic
	,cast(floor((AVG(IsHavingHalfHoursOnEventDate) * 2) / 48) as tinyint) as HalfHoursStatistic 
	into #tmp
	from (
				select dt as EventDate, TI_ID, ИмяПлощадки
				,dbo.usf2_DataCollect_HavingIntegralOnEventDate(TI_ID, TIType, AbsentChannelsMask,AIATSCode, AOATSCode, RIATSCode, ROATSCode, dt) as IsHavingIntegralOnEventDate
				,dbo.usf2_DataCollect_HavingHalfHoursOnEventDate(TI_ID, TIType, AbsentChannelsMask,AIATSCode, AOATSCode, RIATSCode, ROATSCode, dt) as IsHavingHalfHoursOnEventDate
				from  dbo.usf2_Utils_HalfHoursByPeriod(@StartDT, @EndDT), 
				 (
				   select ti.*, dt.StringName  as ИмяПлощадки from Info_TI ti
				   join Dict_TI_Types dt on dt.TIType = ti.TIType
				   where ti.TIType >= 11 and ti.TIType <=15
				   --where ti.TIType = 15
				   --order by TIType
				  ) t
		) g
		group by TI_ID, ИмяПлощадки
	
	select distinct ИмяПлощадки 
	into #ИмяПлощадки
	from #tmp

	--select * from Dict_TI_Types

	--Разделяем статистику на отдельные части, считаем количество по каждой части, затем соединяем статистику
	select 
	#ИмяПлощадки.ИмяПлощадки,
					
	ISNULL(ОтсутствуюПолностьюИнтегралы.[count], 0) + 
	ISNULL(ЕстьПолностьюИнтегралы.[count], 0) + 
	ISNULL(ЕстьЧастичноИнтегралы.[count], 0) as ВсегоТИ,
	
	ISNULL(ЕстьПолностьюИнтегралы.[count], 0) as ЕстьПолностьюИнтегралы,
	ISNULL(ЕстьЧастичноИнтегралы.[count], 0) as ЕстьЧастичноИнтегралы, 

	ISNULL(ЕстьПолностьюПолучасовки.[count], 0) as ЕстьПолностьюПолучасовки,
	ISNULL(ЕстьЧастичноПолучасовки.[count], 0) as ЕстьЧастичноПолучасовки
	from #ИмяПлощадки
	left join 
	(
		select ИмяПлощадки, COUNT(TI_ID) as [count]  from #tmp 
		where IntegralStatistic = 0
		group by ИмяПлощадки
	) ОтсутствуюПолностьюИнтегралы on #ИмяПлощадки.ИмяПлощадки = ОтсутствуюПолностьюИнтегралы.ИмяПлощадки
	left join 
	(
		select ИмяПлощадки, COUNT(TI_ID) as [count]  from #tmp 
		where IntegralStatistic = 2
		group by ИмяПлощадки
	) ЕстьПолностьюИнтегралы on #ИмяПлощадки.ИмяПлощадки = ЕстьПолностьюИнтегралы.ИмяПлощадки
	left join 
	(
		select ИмяПлощадки, COUNT(TI_ID) as [count]  from #tmp 
		where IntegralStatistic = 1
		group by ИмяПлощадки
	) ЕстьЧастичноИнтегралы on #ИмяПлощадки.ИмяПлощадки = ЕстьЧастичноИнтегралы.ИмяПлощадки
	left join
	(
		select ИмяПлощадки, COUNT(TI_ID) as [count]  from #tmp 
		where HalfHoursStatistic = 0
		group by ИмяПлощадки
	) ОтсутствуюПолностьюПолучасовки on #ИмяПлощадки.ИмяПлощадки = ОтсутствуюПолностьюПолучасовки.ИмяПлощадки
	left join 
	(
		select ИмяПлощадки, COUNT(TI_ID) as [count]  from #tmp 
		where HalfHoursStatistic = 2
		group by ИмяПлощадки
	) ЕстьПолностьюПолучасовки on #ИмяПлощадки.ИмяПлощадки = ЕстьПолностьюПолучасовки.ИмяПлощадки
	left join 
	(
		select ИмяПлощадки, COUNT(TI_ID) as [count]  from #tmp 
		where HalfHoursStatistic = 1
		group by ИмяПлощадки
	) ЕстьЧастичноПолучасовки on #ИмяПлощадки.ИмяПлощадки = ЕстьЧастичноПолучасовки.ИмяПлощадки

drop table #ИмяПлощадки;
drop table #tmp;

go
   grant EXECUTE on usp2_Rep_AnalysSborShort to [UserCalcService]
go



