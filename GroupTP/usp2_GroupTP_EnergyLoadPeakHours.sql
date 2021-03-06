if exists (select 1
          from sysobjects
          where  id = object_id('usp2_GroupTP_EnergyLoadPeakHours')
          and type in ('P','PC'))
   drop procedure usp2_GroupTP_EnergyLoadPeakHours
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
--		Ноябрь, 2012
--
-- Описание:
--
--		Возвращаем пиковые часы нагрузки
--
-- ======================================================================================
create proc [dbo].[usp2_GroupTP_EnergyLoadPeakHours]
	@PeakDate date, -- Месяц, год на который ищем часы пиковой нагрузки
	@JuridicalPersonList varchar(4000) = null -- Список гарантирующих поставщиков, через запятую (фильтр, если нужен)
as

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
	declare 
	@i tinyint,
	@daysInMonth tinyint,
	@mask bigint,
	@year int,
	@month int;

	set @year = Year(@PeakDate);
set @month = Month(@PeakDate)
set @i=0;
set @daysInMonth = (select datediff(day, @PeakDate, dateadd(month, 1, @PeakDate)));
set @mask = ISNULL((SELECT TOP 1 [DayMask] FROM [InfoCalc_Holydays_Time_Intervals]
  where [Year] = @year and [Month] = @month), 0);


declare 
@dtEnd DateTime;

set @dtEnd = DATEADD(month, 1, @PeakDate);
--set @dtEnd = DATEADD(minute, -30, @PeakDate);

--Таблица для списка гар. пост
--Здесь будет таблица с рабочими днями из нашего месяца
create table #days
(
[day] date
);

--Перебираем маску, ищем рабочие дни
while @i<@daysInMonth 
begin

if (dbo.sfclr_Utils_BitOperations2(@mask, @i) = 0)
insert into #days values (dateadd(mm, (@year - 1900) * 12 + @month - 1 , @i))

set @i = @i+1;
end

select d.[WarrantedSupplier_ID] as JuridicalPerson_ID, [day] as PeakDate, e.[Hour]  
from 
(
--Здесь все гарантирующие поставщики и рабочие дни для каждого
select dd.[WarrantedSupplier_ID], #days.* from 
	(
		select distinct [WarrantedSupplier_ID] from [dbo].[Dict_JuridicalPersons_Contracts]
		where (@JuridicalPersonList is null or (@JuridicalPersonList is not null and [WarrantedSupplier_ID] in (select * from usf2_Utils_Split(@JuridicalPersonList, ','))))
		and SignDate < @dtEnd and ISNULL(FinishDate, '21000101')>=@PeakDate 
	) as dd, #days 
) d
left join [dbo].[Info_Section_EnergyLoadPeakHours] e on e.JuridicalPerson_ID = d.[WarrantedSupplier_ID] and e.PeakDate = d.[day]
order by d.[WarrantedSupplier_ID], [day]

drop table #days

go
   grant EXECUTE on usp2_GroupTP_EnergyLoadPeakHours to [UserCalcService]
go
