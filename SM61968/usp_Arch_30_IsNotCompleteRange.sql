if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Arch_30_IsNotCompleteRange')
          and type in ('P','PC'))
 drop procedure usp2_Arch_30_IsNotCompleteRange
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2011
--
-- Описание:
--
--		Находим дыру в данных, определяем пределы времени
--
-- ======================================================================================

create proc [dbo].[usp2_Arch_30_IsNotCompleteRange]

	@SlaveSystem_ID int,
	@startFromSlave61968Order int,
	@limitCount int,
	@DTStart DateTime,
	@DTEnd DateTime,
	@DayRange int,
	@HistoryMode tinyint

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
--Заглушка для LINQ
--select ISNULL(CAST(0 as tinyint), 1) as TIType,
--	ISNULL(cast(0 as int), 1) as TI_ID, 
--	ISNULL(cast('' as  varchar(max)), '') as MRID, 
--	ISNULL(cast(0 as int), 0) as Slave61968Order,
--	cast('20111201' as DateTime) as DTStart,
--	cast('20111201' as DateTime) as DTEnd 

create table #result
(
	TIType tinyint,
	TI_ID int, 
	MRID varchar(max), 
	Slave61968Order int,
	DTStart dateTime,
	DTEnd DateTime, 
)

create table #tmp
(
	TIType tinyint,
	TI_ID int, 
	MRID varchar(max), 
	Slave61968Order int,
	AbsentChannelsMask tinyint,
	IsReverse bit,
	DTStart dateTime,
	DTEnd DateTime, 
	
	PRIMARY KEY CLUSTERED([TIType], [ti_id])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
	
)

--Набираем информацию по ТИ, счетчикам и MRID
select distinct top(@limitCount) d.MRID, a.Slave61968Order, ti.TI_ID, ti.TIType, ISNULL(ti.AbsentChannelsMask, 0) as AbsentChannelsMask
into #tmp1
from Master61968_SlaveSystems_EndDeviceAssets a
join Master61968_SlaveSystems_EndDeviceAssets_Description d on a.Slave61968EndDeviceAsset_ID = d.Slave61968EndDeviceAsset_ID
join dbo.Master61968_SlaveSystems_EndDeviceAssets_To_Meters m on a.Slave61968EndDeviceAsset_ID = m.Slave61968EndDeviceAsset_ID
join dbo.Info_Meters_TO_TI mti on mti.METER_ID = m.Meter_ID and StartDateTime = 
( 
	select max(StartDateTime) from dbo.Info_Meters_TO_TI 
	where METER_ID = m.Meter_ID and startDateTime <= @DTEnd and (FinishDateTime is null OR finishDateTime >= @DTStart)
)
join dbo.Info_TI ti on mti.TI_ID =  ti.TI_ID
where a.Slave61968System_ID = @SlaveSystem_ID and a.Slave61968Order > @startFromSlave61968Order
order by Slave61968Order

--Убираем Meter_ID у которых дублируются ТИ (это из за того что дублируются) С эти надо потом разобраться !
insert into #tmp (MRID, Slave61968Order, TI_ID, TIType, AbsentChannelsMask)
select * from #tmp1
where TI_ID not in (select TI_ID from #tmp1 group by TI_ID having COUNT(TI_ID) > 1)

--Дробим период по @DayRange дней и ищем дыры в этих промежутках
declare
@dt1 DateTime,
@dt2 DateTime

create table #dateTmp
(
	DTStart DateTime,
	DTEnd DateTime
)

set @dt1 = @DTStart;

while @dt1 < @DTEnd
	begin
		set @dt2 = DATEADD(day, @DayRange - 1, @dt1);
		if (@dt2 > @DTEnd) set @dt2 = @DTEnd;
		insert #dateTmp values (@dt1, @dt2)
		set @dt1 = dateadd(day, @DayRange, @dt1);
	end

declare
@table30Name nvarchar(100),
@tableIntegrName nvarchar(100),
@ParmDefinition NVARCHAR(1000),
@SQLString NVARCHAR(4000),
@ti_id int,
@titype tinyint,
@AbsentChannelsMask tinyint,
@IsReverse bit,
@MRID varchar(max), 
@Slave61968Order int;

SET @ParmDefinition = N'@DTStart DateTime,@DTEnd DateTime, @titype tinyint';

--Теперь определяем пределы отсутствия данных по каждой точке
declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct titype from #tmp --перебираем таблицы в зависимости от типа точки
  open t;
	FETCH NEXT FROM t into @titype
	WHILE @@FETCH_STATUS = 0
	BEGIN
	--Смотрим на тип точки
	if (@TIType < 10) begin
		set @table30Name = 'ArchComm_30_Values'
		set @tableIntegrName = 'ArchComm_Integrals'
	end else begin
		set @table30Name = 'ArchBit_30_Values_' + ltrim(str(@TIType - 10,2));
		set @tableIntegrName = 'ArchBit_Integrals_' + ltrim(str(@TIType - 10,2));
	end;

	declare tt cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct DTStart, DTEnd from #dateTmp --перебираем диапазоны дат по @DayRange дней
	open tt;
	FETCH NEXT FROM tt into @dt1, @dt2
	WHILE @@FETCH_STATUS = 0
	BEGIN
			if (@HistoryMode = 1) begin
				-- Если перебираем архим за последние 3 месяца, ищем дыры в интегралах
				SET @SQLString = N'insert into #result (TIType, TI_ID,MRID,Slave61968Order, DTStart, DTEnd)
				select TIType, a.TI_ID, MRID, Slave61968Order, a.DTStart, a.DTEnd
				from
				(
					select distinct ti_id, Min(dt) as DTStart, DateAdd(day, 1, Max(dt)) as DTEnd
					from 
					(
						select distinct t.ti_id, dt, channels.Channel, integr.Data, integr.[Status]
						from #tmp t
						cross apply usf2_Utils_HalfHoursByPeriod (@DTStart, @DTEnd) dt
						cross apply usf2_Info_GetChannelsTableForTI(t.TI_ID, t.AbsentChannelsMask, t.IsReverse) channels
						left join '+@tableIntegrName+' integr 
						on integr.ti_id = t.ti_id 
						and integr.EventDateTime = dt 
						and integr.ChannelType = channels.Channel
						where t.TIType = @titype
					) c
					where c.Data is null OR c.[Status]<>0 
					group by c.TI_ID
				) a
				join #tmp t on t.TI_ID = a.TI_ID where TIType = @titype'
				
				EXEC sp_executesql @SQLString, @ParmDefinition, @dt1, @dt2, @titype;
				
			end else begin
				--Если это текущие данные (за последнюю неделю) то просто ложим весь диапазон
				insert into #result (TIType, TI_ID,MRID,Slave61968Order, DTStart, DTEnd)
					select TIType, TI_ID, MRID, Slave61968Order, @dt1, @dt2
					from #tmp  
				
			end
			
		
		
		FETCH NEXT FROM tt into @dt1, @dt2
	end;
	CLOSE tt
	DEALLOCATE tt
	
	FETCH NEXT FROM t into @titype
	end;
	CLOSE t
	DEALLOCATE t

	select TIType, TI_ID,MRID,Slave61968Order, DTStart, DTEnd from #result order by DTEnd asc, DTStart asc


--Для увеличения скорости добавить индекс в Master61968_SlaveSystems_EndDeviceAssets по Slave61968Order
drop table #dateTmp;
drop table #tmp1
drop table #tmp
drop table #result

end
go
   grant EXECUTE on usp2_Arch_30_IsNotCompleteRange to [UserCalcService]
go
grant EXECUTE on usp2_Arch_30_IsNotCompleteRange to [UserSlave61968Service]
go
grant EXECUTE on usp2_Arch_30_IsNotCompleteRange to [UserMaster61968Service]
go
