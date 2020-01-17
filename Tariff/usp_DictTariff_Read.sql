if exists (select 1
          from sysobjects
          where  id = object_id('usp2_DictTariff_Read')
          and type in ('P','PC'))
   drop procedure usp2_DictTariff_Read
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
--		Апрель, 2011
--
-- Описание:
--
--		Выбираем информацию по тарифам по группе точек
--
-- ======================================================================================
create proc [dbo].[usp2_DictTariff_Read]

	@TI_Array varchar(4000), --Идентификатор объекта, тип объекта;
	@DateStart datetime,
	@DateEnd datetime

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	select u.STRnumber as ID, u.CHnumber as [TypeHierarchy]
		,t.Tariff_ID, dbo.usf2_Utils_DateTimeRoundToHalfHour(t.StartDateTime, 1) as TariffStartDateTime, DateAdd(minute,-30,dbo.usf2_Utils_DateTimeRoundToHalfHour(t.FinishDateTime, 0)) as TariffFinishDateTime --Все что относится к тарифу
	into #tblTOtoTariff	
	from 
		 (
		   select distinct STRnumber, CHnumber from usf2_Utils_iter_strintlist_to_table(@TI_Array)
		 ) u
	cross apply usf2_DictTariff_SelectTariffByTI(u.STRnumber, @DateStart, @DateEnd, u.CHnumber) t 
				
	select * from #tblTOtoTariff
		
	select t.Tariff_ID
	,z.TariffZone_ID, dbo.usf2_Utils_DateTimeRoundToHalfHour(z.StartDateTime, 1) as ZoneStartDateTime, DateAdd(minute,-30,dbo.usf2_Utils_DateTimeRoundToHalfHour(z.FinishDateTime,0)) as ZoneFinishDateTime  --Все что относится к зоне
	,i.TariffZoneTimeInterval_ID, i.MonthMask, i.DowMask, i.DayMask, i.HalfHoursMask, dbo.usf2_Utils_DateTimeRoundToHalfHour(i.StartDateTime, 1) as IntervalStartDateTime, DateAdd(minute,-30,dbo.usf2_Utils_DateTimeRoundToHalfHour(i.FinishDateTime, 0)) as IntervalFinishDateTime  --Все что относится к интервалу зоны
	from 
	 (
	   select distinct Tariff_ID from #tblTOtoTariff
	 ) t
	left join dbo.DictTariffs_Zones z on z.Tariff_ID = t.Tariff_ID 
		and (z.StartDateTime <= @DateEnd and (z.FinishDateTime is null OR @DateStart <= z.FinishDateTime))
	left join dbo.DictTariffs_Zones_Time_Intervals i on i.TariffZone_ID = z.TariffZone_ID 
		and (i.StartDateTime <= @DateEnd and (i.FinishDateTime is null OR @DateStart <= i.FinishDateTime))
	order by t.Tariff_ID, z.TariffZone_ID, i.TariffZoneTimeInterval_ID	
		
end
go
   grant EXECUTE on usp2_DictTariff_Read to [UserCalcService]
go