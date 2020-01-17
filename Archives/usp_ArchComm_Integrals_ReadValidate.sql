if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Integrals_ReadValidate')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Integrals_ReadValidate
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'IntString' AND ss.name = N'dbo')
DROP TYPE [dbo].[IntString]
-- Пересоздаем заново
CREATE TYPE [dbo].[IntString] AS TABLE 
(
	Id int NOT NULL, 
	Channels nvarchar(max) not NULL,
	PRIMARY KEY CLUSTERED 
(
	Id ASC
 )WITH (IGNORE_DUP_KEY = OFF))
go

grant EXECUTE on TYPE::IntString to [UserCalcService]
go

grant EXECUTE on TYPE::IntString to [UserMaster61968Service]
go

grant EXECUTE on TYPE::IntString to UserExportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель 2011
--
-- Описание:
--
--		Выбирает значения барабанов для модуля достоверности
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Integrals_ReadValidate]

	@TI_Array IntString readonly,--Здесь указываем идентификаторы ТИ через запятую перечесляем все каналы, 
	@DateStart datetime, --Начало
	@DateEnd datetime, --Окончание
	@IsLastEnabled bit, --Возращать последние показания
	@dataSourceType tinyint = null, --Для мониторинга, признак только автосбора
	@isReadCalculatedValues bit = 0, --Расчетные показания
	@isAutoread bit = 0, --Только с признаком авточтение
	@useInactiveChannel bit = 1 --Анализировать неактивные каналы

as

begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@TIType tinyint;

select *
into #hh
from usf2_Utils_HalfHoursByPeriod (@DateStart, @DateEnd)

create table #result
(
	EventDateTime dateTime, 
	Data float, 
	[Status] int, 
	TI_ID int, 
	ChannelType int, 
	PS_ID int,
	IsChannelEnabled bit,
	groupingCh tinyint,
	isMainChannel bit,
	dataSourceType tinyint,
	IsManual bit,
	IsRequestedChannel bit,
	isActiveTariffChannel bit,
	isNotActiveTariffIntegralChanged bit,
	tiType tinyint,
	isSmallTi bit null,
	PRIMARY KEY CLUSTERED (PS_ID, TI_ID, EventDateTime, groupingCh, isMainChannel, ChannelType)
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF, IGNORE_DUP_KEY = ON)
)

create table #lastResult
(
	TI_ID int, 
	EventDateTime dateTime,
)

create table #tis
(
 Id int,
 PS_ID int,
 ChannelType tinyint,
 Channel tinyint, --Канал с учетом переворота
 tiType tinyint,
 IsChannelEnabled bit,
 IsRequestedChannel bit,
 IsActiveTariffChannel bit,
 isSmallTi bit null,
 PRIMARY KEY CLUSTERED (tiType, Id, ChannelType)
);

declare
@AbsentChannelsMask tinyint,
@IsChannelEnabled bit,
@Id int,
@PS_ID int,
@AIATSCode int,
@AOATSCode int,
@RIATSCode int,
@ROATSCode int,
@IsSmallTI bit,
@Channels nvarchar(max)

create table #channels
(
	channel tinyint,
	isActiveTariffChannel bit,
	isRequestedChannel bit,
	PRIMARY KEY CLUSTERED (channel) with (IGNORE_DUP_KEY = ON)
) 

declare typeCursor cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct a.Id, t.PS_ID, TIType, 
(case when a.Channels is not null and Len(a.Channels) > 0 then 1 else 0 end) as IsChannelEnabled, a.Channels, t.AbsentChannelsMask, 
t.AIATSCode, t.AOATSCode, t.RIATSCode, t.ROATSCode, t.IsSmallTI
from @TI_Array a																																 
join Info_TI t on t.TI_ID = a.Id order by TIType, a.Id																												 
open typeCursor;
FETCH NEXT FROM typeCursor into @Id, @PS_ID, @tiType, @IsChannelEnabled, @Channels, @AbsentChannelsMask, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode,@IsSmallTI
WHILE @@FETCH_STATUS = 0
	BEGIN

	truncate table #channels;

	--Запрошенные каналы

	if (@useInactiveChannel = 0) begin
		insert into #channels
		select distinct Items, 1 as IsActiveTariffChannel, 1 as IsRequestedChannel from dbo.usf2_Utils_Split(@Channels, ',') 
		where (ISNULL(@AbsentChannelsMask,0) / POWER(2, (Items % 10) - 1)) & 1  = 0
	end 
	else begin
		insert into #channels
		select distinct Items, 1 as IsActiveTariffChannel, 1 as IsRequestedChannel from dbo.usf2_Utils_Split(@Channels, ',')  
	end

	--Тарифные каналы для анализа
	insert into #channels
	select Items, 1 as IsActiveTariffChannel, 0 as IsRequestedChannel from 
		dbo.usf2_Utils_Split(dbo.usf2_Info_GetTariffChannelsForTI(@Id, ISNULL(@AbsentChannelsMask, 0), case when @AIATSCode=2 then 1 else 0 end), ',')
		where Items % 10 in (select channel from #channels where channel < 10)
	
	---Отсекаем одноставочники
	if (select Count(distinct channel) from #channels where channel > 10) > 1 begin
		--Неактивные каналы для анализа
		insert into #channels
		select distinct Items, 0 as IsActiveTariffChannel, 0 as IsRequestedChannel from dbo.usf2_Utils_Split('51,61,71,81,91,52,62,72,82,92,53,63,73,83,93,54,64,74,84,94', ',')
		where Items % 10 in (select channel from #channels where channel < 10)
	end

	insert into #tis select  @Id, @PS_ID, channel, 
	dbo.usf2_ReverseTariffChannel(0, channel, @AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode, @Id, @DateStart, @DateEnd), @tiType, @IsChannelEnabled, 
	isRequestedChannel, isActiveTariffChannel, @IsSmallTI
	from #channels

	FETCH NEXT FROM typeCursor into @Id, @PS_ID, @tiType, @IsChannelEnabled, @Channels, @AbsentChannelsMask, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode,@IsSmallTI
	END;
CLOSE typeCursor
DEALLOCATE typeCursor;

--select * from #tis

DECLARE @parms NVARCHAR(1000), @requestState NVARCHAR(max), @request NVARCHAR(max);
set @parms = N'@IsLastEnabled bit, @TIType tinyint, @DateStart datetime, @DateEnd datetime,@isReadCalculatedValues bit,@isAutoread bit,@dataSourceType tinyint';
set @requestState = N'insert into #result
		select cr.dt, [Data], [Status], cr.Id,  cr.Channel as ChannelType, cr.PS_ID, 1 as IsChannelEnabled, cr.Channel % 10
		, case when cr.ChannelType <= 10 then 1 else 0 end as isMainChannel, DataSourceType, IsManual, IsRequestedChannel, isActiveTariffChannel, 0, @TIType, cr.IsSmallTi
		from 
		(select * from #hh, #tis where #tis.tiType = @TIType) cr
		outer apply 
		(
			select top (1) TI_ID, ChannelType, EventDateTime, case when @isReadCalculatedValues = 1 then ISNULL([ManualEnterData], [Data]) else [Data] end as Data, [Status], DataSourceType 
			, case when @isReadCalculatedValues = 1 and [ManualEnterData] is not null then 1 else 0 end as IsManual from {archive} a
			left join [Expl_DataSource_PriorityList] pl on a.DataSource_ID = pl.DataSource_ID and pl.[Month] = Month(DateAdd(s, -1,a.EventDateTime)) and pl.[Year] = Year(DateAdd(s, -1,a.EventDateTime))
			join [Expl_DataSource_List] dsl on a.DataSource_ID = dsl.DataSource_ID 
			where TI_ID = cr.Id and ChannelType = cr.ChannelType and EventDateTime between cr.dt and DateAdd(n, 1439, cr.dt) and (@IsAutoRead = 0 OR IntegralType = 0)
			and (@dataSourceType is null or dsl.DataSourceType = @dataSourceType) and (@isReadCalculatedValues = 1 or Data >= 0)
			order by EventDateTime, ISNULL(pl.[Priority], (255 - a.DataSource_ID)) desc
		) a
		where (IsChannelEnabled = 1 OR [Data] is not null) 

		update u 
		set u.isNotActiveTariffIntegralChanged = 1
		from #result u
		inner join 
		(
			select rr.TI_ID, rr.ChannelType, rr.EventDateTime
			from #result rr
			cross apply 
			(
				select top (1) case when @isReadCalculatedValues = 1 then ISNULL([ManualEnterData], [Data]) else [Data] end as Data from {archive} a
						left join [Expl_DataSource_PriorityList] pl on a.DataSource_ID = pl.DataSource_ID and pl.[Month] = Month(DateAdd(s, -1,a.EventDateTime)) and pl.[Year] = Year(DateAdd(s, -1,a.EventDateTime))
						join [Expl_DataSource_List] dsl on a.DataSource_ID = dsl.DataSource_ID 
						where TI_ID = rr.TI_ID and ChannelType = rr.ChannelType and EventDateTime < rr.EventDateTime and (@IsAutoRead = 0 OR IntegralType = 0)
						and (@dataSourceType is null or dsl.DataSourceType = @dataSourceType) and (@isReadCalculatedValues = 1 or Data >= 0)
						order by EventDateTime desc, ISNULL(pl.[Priority], (255 - a.DataSource_ID)) desc
			) p
			where rr.tiType = @TIType and rr.isActiveTariffChannel = 0 and rr.Data is not null and rr.Data <> p.Data 
		) s on u.TI_ID = s.TI_ID and u.ChannelType = s.ChannelType % 10 and u.EventDateTime = s.EventDateTime
		where u.tiType = @TIType

		if (@IsLastEnabled = 1) begin
			insert into #lastResult
			select distinct d.TI_ID, d.DispatchDateTime	from 
			(
				select distinct Id from #tis where tiType = @TIType
			) t
			cross apply 
			(
				select top (1) a.*, dsl.DataSourceType from {archive} a
				left join [Expl_DataSource_PriorityList] pl on a.DataSource_ID = pl.DataSource_ID and pl.[Month] = Month(DateAdd(s, -1,a.EventDateTime)) and pl.[Year] = Year(DateAdd(s, -1,a.EventDateTime))
				join [Expl_DataSource_List] dsl on a.DataSource_ID = dsl.DataSource_ID 
				where a.TI_ID=t.Id and [Status] is not null and [Status] = 0
				and (@dataSourceType is null or dsl.DataSourceType = @dataSourceType)
				order by EventDateTime desc, ISNULL(pl.[Priority], (255 - a.DataSource_ID)) desc, DispatchDateTime desc
			) d 
			
		end'


declare typeCursor cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tis
open typeCursor;
FETCH NEXT FROM typeCursor into @tiType
WHILE @@FETCH_STATUS = 0
	BEGIN
		if (@tiType <= 10) begin
			set @request = REPLACE(@requestState, '{archive}', 'dbo.ArchCalc_Integrals_Virtual');
		end else begin
			set @request = REPLACE(@requestState, '{archive}', 'dbo.ArchCalcBit_Integrals_Virtual_' + + ltrim(str(@tiType - 10,2)));
		end;

		--print @request;

		EXEC sp_executesql @request, @parms, @IsLastEnabled, @TIType, @DateStart, @DateEnd,@isReadCalculatedValues,@isAutoread, @dataSourceType
	FETCH NEXT FROM typeCursor into @tiType
	END;
CLOSE typeCursor
DEALLOCATE typeCursor;

declare @defaultFinishDt DateTime;
set @defaultFinishDt = '21000101';

WITH resultTemp (EventDateTime,Data,[Status],TI_ID,ChannelType,PS_ID,
	IsChannelEnabled,groupingCh,isMainChannel, dataSourceType, IsManual, IsRequestedChannel, isNotActiveTariffIntegralChanged, Value, 
	tiType, isSmallTi) 
	as
	(
		select EventDateTime,Data,[Status],TI_ID,ChannelType,PS_ID,
			IsChannelEnabled,groupingCh,isMainChannel, dataSourceType, IsManual, IsRequestedChannel, isNotActiveTariffIntegralChanged, 
			ISNULL(Data, -1) as Value, tiType, isSmallTi from #result-- -1 пишем для отсеивания ситуации когда данных по тарифным каналам просто нету
	)
	
	--Подсчитываем сумму по тарифным каналам, вычитаем из этой суммы основной канал
	select r1.*
	--Наличие разницы основного канала с тарифными
	, cast(case when r2.SumData > -1 AND ABS(r1.Value - ISNULL(r2.SumData, r1.Value)) 
	> ISNULL((
		select top 1 MissingTariffsData from ArchCalcBit_Integrals_MissingTariffs 
		where TI_ID = r2.TI_ID and ChannelType = r2.groupingCh and r2.EventDateTime between StartDateTime and ISNULL(FinishDateTime, @defaultFinishDt)
		order by StartDateTime desc
		), 2) * 1000 
	then 1 else 0 end as bit) as IsHasInactiveTariff 
	--Наличие основного канала при отсутствии тарифных каналов
	, cast(case when tariffMustHave>0 and (r2.SumData is null or r2.SumData < 0) and r1.Value >= 0 then 1 else 0 end as bit) as IsNotFoundTariffValues
	from 
	( 
		select r.*, (select Count(ChannelType1)
		from dbo.DictTariffs_ToTI d 
		join DictTariffs_Zones z on z.Tariff_ID = d.Tariff_iD
		where TI_ID = r.TI_ID and d.StartDateTime = (select MAX(StartDateTime) 
						from dbo.DictTariffs_ToTI 
						where TI_ID = r.TI_ID) and ChannelType1 > 10) as tariffMustHave --Сколько должно быть тарифных каналов
				from resultTemp r
				where isMainChannel = 1
		
	) r1
	left join 
	(
		select PS_ID, TI_ID, EventDateTime, groupingCh, SUM(Value) as SumData, DataSourceType from resultTemp
		where isMainChannel = 0
		group by PS_ID, TI_ID, EventDateTime, groupingCh, DataSourceType
	) r2 on r1.PS_ID = r2.PS_ID and r1.TI_ID = r2.TI_ID and r1.EventDateTime = r2.EventDateTime
			and r1.groupingCh = r2.groupingCh and r1.DataSourceType = r2.DataSourceType
	union --Теперь объединяем с тарифными каналами, которые были запрошены
	select r.*, 0
	, cast(0 as  bit) as IsHasInactiveTariff
	, cast(0 as  bit) as IsNotFoundTariffValues
	from resultTemp r where isMainChannel = 0 and IsChannelEnabled = 1 
	and IsRequestedChannel = 1 --Отсеиваем каналы, которые не запрашивали
	
	order by PS_ID, TI_ID, ChannelType, EventDateTime --Сортируем так как нужно в нашей структуре

	if (@IsLastEnabled = 1) begin
		select * from #lastResult order by TI_ID
	end

drop table #channels
drop table #tis
drop table #result
drop table #lastResult
drop table #hh

end
go
   grant EXECUTE on usp2_ArchComm_Integrals_ReadValidate to [UserCalcService]
go
