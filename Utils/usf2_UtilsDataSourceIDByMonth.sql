if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UtilsDataSourceIDByMonth')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UtilsDataSourceIDByMonth
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь, 2013
--
-- Описание:
--
--		Список идентификаторов источников по которым нужно отфильтровывать архивы по своим месяцам
--	
-- ======================================================================================
create FUNCTION [dbo].[usf2_UtilsDataSourceIDByMonth] 
(
		@ti_id int, --ТИ
		@tp_id int, --ТП
		@dtStart DateTime, --Начало
		@dtEnd DateTime, --Окончание
		@ClosedPeriod_ID uniqueidentifier -- Закрытый период, если нужно
)
      RETURNS @tbl TABLE --Результирующая таблица
	  (
		dtstart DateTime, -- Диапазон
		dtEnd DateTime, 
		DataSource_ID int, -- Источник по которому фильтруем данные
		PRIMARY KEY CLUSTERED (dtstart)
		WITH (IGNORE_DUP_KEY = ON) 
	 )
AS
BEGIN
declare 
@monthYearStart int, @monthYearEnd int;

set @monthYearStart = Year(@dtStart) * 12 + Month(@dtStart);
set @monthYearEnd = Year(@dtEnd) * 12 + Month(@dtEnd);

if (@ClosedPeriod_ID is null) begin
	insert into @tbl
	select dateadd(mm,([Year]-1900)* 12 + [Month] - 1,0) as dtStart, dateadd(mm,([Year]-1900)* 12 + [Month],0) as dtEnd,DataSource_ID 
	from Expl_DataSource_To_TI_TP 
	where TI_ID = @ti_ID and tp_id = @tp_id and [Year] * 12 + [Month] between @monthYearStart and @monthYearEnd

	insert into @tbl
	select dateadd(mm,(l.[Year]-1900)* 12 + l.[Month] - 1,0) as dtStart, dateadd(mm,(l.[Year]-1900)* 12 + l.[Month],0) as dtEnd,DataSource_ID
	from [dbo].[Expl_DataSource_PriorityList] l
	join 
	(
		select [Year], [Month], Max(Priority) as maxPriority from [dbo].[Expl_DataSource_PriorityList]
		where [Year] * 12 + [Month] between @monthYearStart and @monthYearEnd
		group by [Year], [Month]
	) mp on mp.Year = l.Year and mp.Month = l.Month and mp.maxPriority = l.Priority
	where l.[Year] * 12 + l.[Month] between @monthYearStart and @monthYearEnd
end else begin
	insert into @tbl
	select dateadd(mm,([Year]-1900)* 12 + [Month] - 1,0) as dtStart, dateadd(mm,([Year]-1900)* 12 + [Month],0) as dtEnd,DataSource_ID 
	from Expl_DataSource_To_TI_TP_Closed titp
	join Expl_ClosedPeriod_List cl on cl.ClosedPeriod_ID = titp.ClosedPeriod_ID
	where TI_ID = @ti_ID and tp_id = @tp_id and [Year] * 12 + [Month] between @monthYearStart and @monthYearEnd and titp.ClosedPeriod_ID = @ClosedPeriod_ID

	insert into @tbl
	select dateadd(mm,(cl.[Year]-1900)* 12 + cl.[Month] - 1,0) as dtStart, dateadd(mm,(cl.[Year]-1900)* 12 + cl.[Month],0) as dtEnd,DataSource_ID
	from [dbo].[Expl_DataSource_PriorityList_Closed] l
	join Expl_ClosedPeriod_List cl on cl.ClosedPeriod_ID = l.ClosedPeriod_ID
	join 
	(
		select [Year], [Month], Max(Priority) as maxPriority 
		from [dbo].[Expl_DataSource_PriorityList_Closed] l
		join Expl_ClosedPeriod_List cl on cl.ClosedPeriod_ID = l.ClosedPeriod_ID
		where [Year] * 12 + [Month] between @monthYearStart and @monthYearEnd and l.ClosedPeriod_ID = @ClosedPeriod_ID
		group by [Year], [Month]
	) mp on mp.Year = cl.Year and mp.Month = cl.Month and mp.maxPriority = l.Priority
	where cl.[Year] * 12 + cl.[Month] between @monthYearStart and @monthYearEnd  and l.ClosedPeriod_ID = @ClosedPeriod_ID

end;
	RETURN
END
go
grant select on usf2_UtilsDataSourceIDByMonth to [UserCalcService]
go