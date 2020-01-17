if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Integral_Last')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Integral_Last
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_ReadArrayWithCA')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_ReadArrayWithCA
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Integrals_ReadArray')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Integrals_ReadArray
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Integrals_ExtendedChannels_ReadArray')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Integrals_ExtendedChannels_ReadArray
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_FindTiWhereChangedNsi')
          and type in ('P','PC'))
   drop procedure usp2_Expl_FindTiWhereChangedNsi
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetChannelsTarifForTIs')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetChannelsTarifForTIs
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchBit_SelectNearestIntegral')
          and type in ('P','PC'))
   drop procedure usp2_ArchBit_SelectNearestIntegral
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchIntegral_ReadArrayLast')
          and type in ('P','PC'))
   drop procedure usp2_ArchIntegral_ReadArrayLast
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_usp2_ArchCalcBitIntegrals_Last')
          and type in ('P','PC'))
   drop procedure usp2_usp2_ArchCalcBitIntegrals_Last
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchCalcBitIntegrals_Last')
          and type in ('P','PC'))
   drop procedure usp2_ArchCalcBitIntegrals_Last
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Replace_ActUndercounts')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Replace_ActUndercounts
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchCalcBitIntegrals')
          and type in ('P','PC'))
   drop procedure usp2_ArchCalcBitIntegrals
go

--Обновляем тип
--Удаляем если есть
IF  NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'TiChannelType' AND ss.name = N'dbo')
-- Если нет, создаем
CREATE TYPE [dbo].[TiChannelType] AS TABLE 
(
	TI_ID int NOT NULL, 
	ChannelType tinyint NOT NULL,
	IsCA bit NOT NULL,
	TP_ID int NULL,
	ClosedPeriod_ID uniqueidentifier NULL,
	DataSourceType tinyint NULL
)
go

grant EXECUTE on TYPE::TiChannelType to [UserCalcService]
go

grant EXECUTE on TYPE::TiChannelType to [UserMaster61968Service]
go

grant EXECUTE on TYPE::TiChannelType to UserExportService
go

grant EXECUTE on TYPE::TiChannelType to UserImportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Выбирает получасовки для указанных параметров  (эта процедура работает и с КА)
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_ReadArrayWithCA]

	@TIArray TiChannelType readonly, --ТИ, канал, признак стороны, идентификатор закрытого периода, если необходимо читать из закрытого периода
	@DateStart datetime,
	@DateEnd datetime,
	@isCoeffEnabled bit,
	@IsCAEnabled bit,
	@IsOVEnabled bit,
	@isOVIntervalEnabled bit,
	@isValidateOtherDataSource bit = null, -- Нужны достоверности по остальным источникам
	@IsReadCalculatedValues bit,

	@IsNeedWsTable bit = 0, --Необходима таблица с доп 30 минутками отмены летнего времени
	@IsNeedResiduesTable bit = 0, --Необходима талица с округлением 80020

	@HalfHoursShiftClientFromServer int = 0, --Смещение количество получасовок между сервером и клиентом для 80020
	@UseInactiveChannel bit = 0, -- Отображать отключенные каналы

	@UseLossesCoefficient bit = 0, --Использовать ли коэфф. потерь для ТИ
	@excludedActUndercountUns varchar(max) = null, --Список идентификаторов актов недоучета, которые исключаем из чтения (для модуля ручного ввода по акту недоучета)
	@isReturnPreviousDispatchDateTime bit = 0, --Возвращать DispatchDateTime предыдущего поступления данных
	@UseActUndercount bit = 1 -- Получать данные по акту недочета
as
declare
@Num int,
@TI_ID int,
@DataSourceType tinyint = null, -- Источник, если не указывается то в порядке приоритета
@IsCA bit

begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--все ТИ
select distinct TI_ID, ChannelType, TP_ID, ClosedPeriod_ID, DataSourceType
into #tiOurSideArray
from @TIArray where IsCA = 0

--Все ТИ КА
select distinct TI_ID, ChannelType, TP_ID, ClosedPeriod_ID, DataSourceType
into #tiContrSideArray
from @TIArray where IsCA = 1

--Привязка ТИ к формуле (работаем только с нашей строной)
if (@IsReadCalculatedValues = 1) begin
	select f.TI_ID, f.TIChannelType as ChannelType, f.Formula_UN, f.StartDateTime, f.FinishDateTime 
	from #tiOurSideArray ti
	join Info_Formula_To_TI f on f.TI_ID = ti.TI_ID and f.TIChannelType = ti.ChannelType 
		and StartDateTime <= @DateEnd and (FinishDateTime is null or FinishDateTime >= @DateStart)
		and FormulaCalcType = 0 --пока только расчитываемые на лету
	order by f.TI_ID, f.TIChannelType, f.StartDateTime
end

--Таблица с коэфф. потерь для ТИ
if (@UseLossesCoefficient = 1) begin
	SELECT c.[TI_ID],c.[StartDateTime],c.[FinishDateTime],c.[LossesCoefficient]
	FROM Info_TI_LossesCoefficients c
	where c.TI_ID in (select distinct TI_ID from #tiOurSideArray) and 
	c.StartDateTime <= @DateEnd and (c.FinishDateTime is null or (c.FinishDateTime is not null and c.FinishDateTime>= @DateStart))
	order by TI_ID, StartDateTime
end

--Талица с округлением 80020, только наша сторона, нужны только сутки до запрашиваемых
if (@IsNeedResiduesTable = 1) begin
	SELECT r.[TI_ID]
      ,r.[EventDate]
      ,r.[ChannelType]
      ,r.[DataSource_ID]
      ,r.[HalfHoursShiftFromUTC]
      ,case when @IsReadCalculatedValues = 1 then r.[CAL] else r.[VAL] end as VAL
      ,r.[LatestDispatchDateTime]
	FROM #tiOurSideArray a
	join [dbo].[ExplDoc_Residues_XML80020] r on r.TI_ID = a.TI_ID and r.ChannelType = a.ChannelType
	where r.EventDate = DateAdd(day, -1, floor(cast(DATEADD(minute, -ISNULL(@HalfHoursShiftClientFromServer,0) * 30, @DateStart) as float))) and r.HalfHoursShiftFromUTC = ISNULL(@HalfHoursShiftClientFromServer,0) and UseLossesCoefficient = @UseLossesCoefficient
end

declare
@yearStart int,
@monthStart int,
@yearEnd int,
@monthEnd int;

select @yearStart=Year(@DateStart), @monthStart=Month(@DateStart), @yearEnd=Year(@DateEnd), @monthEnd=Month(@DateEnd);

--Приоритеты источников по ТИ, ТП (прописнные вручную)
select distinct e.TI_ID, e.TP_ID, e.DataSource_ID, [Year] * 12 + [Month] as MonthNumber, null as ClosedPeriod_ID
from [dbo].[Expl_DataSource_To_TI_TP] e
join #tiOurSideArray a on a.TI_ID = e.TI_ID and a.TP_ID = e.TP_ID 
where a.ClosedPeriod_ID is null and [Year] * 12 + [Month]  >= Year(@DateStart) * 12 + Month(@DateStart) and [Year] * 12 + [Month] <= Year(@DateEnd) * 12 + Month(@DateEnd)
--Закрытый период
union
select distinct e.TI_ID, e.TP_ID, e.DataSource_ID, [Year] * 12 + [Month] as MonthNumber, e.ClosedPeriod_ID
from [dbo].[Expl_DataSource_To_TI_TP_Closed] e
join Expl_ClosedPeriod_List cl on cl.ClosedPeriod_ID = e.ClosedPeriod_ID
join #tiOurSideArray a on a.TI_ID = e.TI_ID and a.TP_ID = e.TP_ID and a.ClosedPeriod_ID = e.ClosedPeriod_ID
where a.ClosedPeriod_ID is not null and [Year] * 12 + [Month]  >= Year(@DateStart) * 12 + Month(@DateStart) and [Year] * 12 + [Month] <= Year(@DateEnd) * 12 + Month(@DateEnd)
order by TI_ID, Year * 12 + Month


--Описания источников
select [DataSource_ID], [DataSourceType] from [dbo].[Expl_DataSource_List]

--Общие приоритеты
select [Year] * 12 + [Month] as MonthNumber, [Priority], DataSource_ID, null as ClosedPeriod_ID
from Expl_DataSource_PriorityList e
where [Year] * 12 + [Month] >= @yearStart * 12 + @monthStart and [Year] * 12 + [Month] <= @yearEnd * 12 + @monthEnd
--Приоритеты из закрытого периода
union
select [Year] * 12 + [Month] as MonthNumber, [Priority], DataSource_ID, e.ClosedPeriod_ID
from Expl_DataSource_PriorityList_Closed e
join Expl_ClosedPeriod_List cl on cl.ClosedPeriod_ID = e.ClosedPeriod_ID
where e.ClosedPeriod_ID in (select distinct ClosedPeriod_ID from @TIArray where ClosedPeriod_ID is not null) 
and [Year] * 12 + [Month] >= @yearStart * 12 + @monthStart and [Year] * 12 + [Month] <= @yearEnd * 12 + @monthEnd
order by ClosedPeriod_ID, [Year] * 12 + [Month], e.[Priority] desc


if (@isCoeffEnabled = 1) begin

	--Выборка коэфф. трансформации
	select ti_id, cast(COEFU*COEFI as float(26)) as Coeff, 
	dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime < @DateStart then @DateStart else StartDateTime end, 1) as StartDateTime,
	case when FinishDateTime > @DateEnd then @DateEnd else FinishDateTime end as FinishDateTime, ClosedPeriod_ID
	from 
	(
		select distinct ti_id, COEFU, COEFI, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, null as ClosedPeriod_ID 
		from dbo.Info_Transformators where ti_id in (select distinct ti_id from #tiOurSideArray where ClosedPeriod_ID is null)
		and dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
		union 
		select distinct it.ti_id, COEFU, COEFI, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, it.ClosedPeriod_ID 
		from dbo.Info_Transformators_Closed it
		join #tiOurSideArray a on a.ClosedPeriod_ID = it.ClosedPeriod_ID and a.TI_ID = it.TI_ID
		where a.ClosedPeriod_ID is not null	and dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
	) t
	order by ti_id, ClosedPeriod_ID, StartDateTime

	--Выборка периодов, когда коэфф. трансформации был заблокирован
	select distinct ti_id, IsCoeffTransformationDisabled, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, null as ClosedPeriod_ID 
	from dbo.ArchCalc_CoeffTransformation_DisabledStatus where ti_id in (select distinct ti_id from #tiOurSideArray where ClosedPeriod_ID is null)
	and dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
	order by ti_id, ClosedPeriod_ID, StartDateTime
end;

--Выборка параметров точек, для ТИ
select distinct PS_ID, ISNULL(TPCoefOurSide,1) as TPCoef, TIType, IsCoeffTransformationDisabled,
		AbsentChannelsMask,	case when AIATSCode = 2 then 1 else 0 end as IsChannelsInverted,
		dbo.usf2_ReverseTariffChannel(0, ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, ti.TI_ID, @DateStart, @DateEnd) as ChannelForRequest,
		--case when AIATSCode = 1 then 0 else 1 end as IsChannelReverse, -- проверяем перевернут ли канал по основной ТИ
		0 as IsCA, ti.TI_ID, a.ChannelType, a.TP_ID, ClosedPeriod_ID, DataSourceType, IsSmallTI, AIATSCode, MeasureUnit_UN
		,cast(case when hv.TI_ID is null then 0 else 1 end as bit) as isOV --признак ОВ
into #tiParams
from Info_TI ti
left join Hard_OV_List hv on hv.TI_ID = ti.TI_ID
join #tiOurSideArray a on a.TI_ID = ti.TI_ID
outer apply --Ед. измерения ТИ
(
	select top 1 MeasureUnit_UN from Info_TI_To_MeasureUnit
	where TI_ID = a.TI_ID and ChannelType = a.ChannelType
) mu

--Параметры для КА
insert into #tiParams
select distinct Contr_PS_ID, ISNULL(TPCoefContr, 1) as TPCoef, TIType, 0 as IsCoeffTransformationDisabled,
 0 as AbsentChannelsMask, case when AIATSCode = 2 then 1 else 0 end as IsChannelsInverted, 
 dbo.usf2_ReverseTariffChannel(1, 
case ChannelType --Переворачиваем канал для КА
when 1 then 2
when 2 then 1
when 3 then 4
when 4 then 3 end, 
AIATSCode,AOATSCode,RIATSCode,ROATSCode, ContrTI_ID, @DateStart, @DateEnd),
1 as IsCA, ContrTI_ID, a.ChannelType, a.TP_ID, ClosedPeriod_ID, DataSourceType, 0, AIATSCode, null, 0 as IsOv
from Info_Contr_TI ti
join #tiContrSideArray a on a.TI_ID = ti.ContrTI_ID


--Параметры точки
declare
@PS_ID int,
@TP_ID int, --ТП с которой запрашиваются значения
@ClosedPeriod_ID uniqueidentifier,
@TIType tinyint,
@TPCoef float,
@ChannelForRequest tinyint,
@ChannelType tinyint,
@IsCoeffTransformationDisabled bit,
@AbsentChannelsMask tinyint,
@IsAbsentChannel bit, 
@IsOVon bit,
@IsSmallTI bit,
@IsChannelsInverted bit,
@MeasureUnit_UN varchar(128)

select Count(*) as puNumbers from #tiParams;

if (@IsNeedWsTable = 1) begin
--Это период отмены зимнего времени, читаем промежуточную таблицу
--Переворот и маску учитываем на клиенте, на источники не заморачиваемся, читаем что есть
	SELECT distinct a.[TI_ID]
      ,a.[EventDate]
      ,tis.[ChannelType]
	  ,ds.DataSourceType
      ,a.[VAL_01]
      ,a.[VAL_02]
      ,a.[VAL_03]
      ,a.[VAL_04]
	  ,a.[CAL_01]
      ,a.[CAL_02]
      ,a.[CAL_03]
      ,a.[CAL_04]
      ,a.[ValidStatus]
      ,a.[DispatchDateTime]
      ,a.[CUS_ID]
      ,a.[Status]
	FROM #tiParams tis
	join [dbo].[ArchBit_30_Values_WS] a on a.TI_ID = tis.Ti_ID and a.EventDate = '20141026' and 
	a.ChannelType = tis.ChannelType --TODO доработать переворот по-старому
	join Expl_DataSource_List ds on ds.DataSource_ID = a.DataSource_ID and (tis.DataSourceType is null 
		or tis.DataSourceType = ds.DataSourceType)
end


declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select PS_ID, TPCoef, TIType, IsCoeffTransformationDisabled, TP_ID, AbsentChannelsMask, IsCA, TI_ID, 
	ChannelType, ClosedPeriod_ID, DataSourceType, IsSmallTI,IsChannelsInverted,ChannelForRequest,MeasureUnit_UN, isOV from #tiParams
  open t;
	FETCH NEXT FROM t into @PS_ID, @TPCoef, @TIType, @IsCoeffTransformationDisabled, @TP_ID, @AbsentChannelsMask, @IsCA, @TI_ID, 
		@ChannelType, @ClosedPeriod_ID, @DataSourceType, @IsSmallTI, @IsChannelsInverted, @ChannelForRequest, @MeasureUnit_UN, @IsOVon
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		set @IsAbsentChannel = 0;
		--Один из каналов  отстутствует
		if (@AbsentChannelsMask is not null) and (@AbsentChannelsMask <> 0) begin
			declare @ch tinyint;
			if (@ChannelForRequest > 10) set @ch = @ChannelForRequest % 10; --Тарифный канал
			else set @ch = @ChannelForRequest;
			if (@ch = 1) set @IsAbsentChannel = @AbsentChannelsMask & 1;
			else if (@ch = 2) set @IsAbsentChannel = (@AbsentChannelsMask / 2) & 1;
			else if (@ch = 3) set @IsAbsentChannel = (@AbsentChannelsMask / 4) & 1;
			else if (@ch = 4) set @IsAbsentChannel = (@AbsentChannelsMask / 8) & 1;
		end;

		--set @IsOVon = ISNULL(case when @isOVIntervalEnabled = 0 then dbo.usf2_Utils_IsOVOn(@TI_ID,@IsCA, @DateStart,@DateEnd) else cast(0 as bit) end, 0);
		--Параметры точки
		select @IsOVon as IsOV,
		@PS_ID as PS_ID, @TPCoef as TPCoef, @TIType as TIType, @TP_ID as TP_ID, @IsCA as IsCA, @TI_ID as TI_ID, @ChannelType as ChannelType, 
		@isValidateOtherDataSource as isValidateOtherDataSource, @IsReadCalculatedValues as IsReadCalculatedValues, @isCoeffEnabled as isCoeffEnabled,
		@ClosedPeriod_ID as ClosedPeriod_ID, @DataSourceType as DataSourceType, dbo.usf2_Info_GetTINotWorkedPeriod(@TI_ID, @DateStart, @DateEnd) as NotWorkedPeriod, 
		@IsSmallTI as IsSmallTI, @IsCoeffTransformationDisabled as IsCoeffTransformationDisabled,@IsAbsentChannel as IsAbsentChannel, @MeasureUnit_UN as MeasureUnit_UN
		
		exec usp2_ArchComm_ReadWithCA @TI_ID,@DateStart,@DateEnd,@DataSourceType,@ClosedPeriod_ID,@isCoeffEnabled,@IsCAEnabled,@IsOVEnabled,@ChannelType,@isCA, 
		@isOVIntervalEnabled, @isValidateOtherDataSource, @IsReadCalculatedValues,  
		@IsOVon, @TIType, @UseInactiveChannel,@excludedActUndercountUns,@isReturnPreviousDispatchDateTime, @IsChannelsInverted, @UseActUndercount, @IsAbsentChannel
		

		--select * from @channels
	FETCH NEXT FROM t into @PS_ID, @TPCoef, @TIType, @IsCoeffTransformationDisabled, @TP_ID, @AbsentChannelsMask, @IsCA, @TI_ID, @ChannelType, @ClosedPeriod_ID, @DataSourceType,
		@IsSmallTI, @IsChannelsInverted,@ChannelForRequest, @MeasureUnit_UN, @IsOVon
	end;
	CLOSE t
	DEALLOCATE t
end

drop table #tiOurSideArray
go

grant EXECUTE on usp2_ArchComm_ReadArrayWithCA to [UserCalcService]
go

grant EXECUTE on usp2_ArchComm_ReadArrayWithCA to [UserSlave61968Service]
go

grant EXECUTE on usp2_ArchComm_ReadArrayWithCA to UserExportService
go

grant EXECUTE on usp2_ArchComm_ReadArrayWithCA to UserImportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2008
--
-- Описание:
--
--		Выбирает значения барабанов
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Integrals_ReadArray]

	@TI_Array dbo.TiChannelType READONLY,
	@dateStart datetime,
	@dateEnd datetime,
	@isStartAndEndDateOnly bit,
	@isAutoRead bit,
	@isReadCalculatedValues bit,
	@isFindBack bit,
	@UseLossesCoefficient bit = 0 --Использовать ли коэфф. потерь для ТИ

as
declare
@TI_ID int,
@PS_ID int,
@ChannelType tinyint,
@titype tinyint,
@IsCoeffTransformationDisabled bit,
@TypeArchTable tinyint,
@closedPeriod_ID uniqueidentifier,
@tp_id int,
@DataSourceType tinyint

begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@IsChannelsInverted bit,
@AbsentChannelsMask tinyint,
@IsAbsentChannel bit


select distinct usf.TI_ID, ChannelType, usf.ClosedPeriod_ID, dsl.DataSource_ID, usf.DataSourceType, usf.TP_ID, 
IsCoeffTransformationDisabled, titype, ISNULL(AbsentChannelsMask, 0) as AbsentChannelsMask, PS_ID,
case when AIATSCode = 2 then 1 else 0 end as IsChannelsInverted
into #tis 
from @TI_Array usf
join Info_TI ti on ti.TI_ID = usf.TI_ID
left join [dbo].[Expl_DataSource_List] dsl on dsl.DataSourceType = usf.DataSourceType
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Выбираем смену счетчиков
select usf.* 
into #replaceHistory from 
(select distinct TI_ID from #tis) t
cross apply dbo.usf2_Utils_Monit_Exchanges_Meters_TO(t.TI_ID, @DateStart, @DateEnd) usf

--1 результат (Данные по времени действия счетчика)
select * from #replaceHistory r order by TI_ID, StartDateTime;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Таблица с коэфф. потерь для ТИ
if (@UseLossesCoefficient = 1) begin
	SELECT c.[TI_ID],c.[StartDateTime],c.[FinishDateTime],c.[LossesCoefficient]
	FROM Info_TI_LossesCoefficients c
	where c.TI_ID in (select distinct TI_ID from #tis) and 
	c.StartDateTime <= @DateEnd and (c.FinishDateTime is null or (c.FinishDateTime is not null and c.FinishDateTime>= @DateStart))
	order by TI_ID, StartDateTime
end

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 2 результат (Данные по последним данным каналов при смене счетчика)
select rh.TI_ID, cast(c.ChannelType as tinyint) as ChannelType,
 h.LastData, h.FirstData, h.MetersReplaceSession_ID, rh.DigitСapacity
from #tis ti 
cross apply dbo.usf2_ArchCalcChannelInversionStatus(ti.TI_ID, ti.ChannelType, @DateStart, @DateEnd, ti.IsChannelsInverted) c 
join #replaceHistory rh  on rh.TI_ID = ti.TI_ID and rh.StartDateTime <= c.FinishChannelStatus and (rh.FinishDateTime is null or rh.FinishDateTime >= c.StartChannelStatus)
join dbo.Info_Meters_ReplaceHistory_Channels h on h.MetersReplaceSession_ID = rh.MetersReplaceSession_ID and h.ChannelType=c.ChannelType
order by rh.TI_ID, h.MetersReplaceSession_ID 

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 3 результат (Данные по времени действия коэффициента трансформации)
select TI_ID, cast(COEFU*COEFI as float(26)) as Coeff, 
case when StartDateTime < @DateStart then @DateStart else StartDateTime end as StartDateTime,
case when FinishDateTime > @DateEnd then @DateEnd else ISNULL(FinishDateTime, '21000101') end as FinishDateTime
from dbo.Info_Transformators  it
where it.TI_ID in (select distinct TI_ID from #tis) and StartDateTime <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
order by StartDateTime

--4 Выборка периодов, когда коэфф. трансформации был заблокирован
select distinct ti_id, IsCoeffTransformationDisabled, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, null as ClosedPeriod_ID 
from dbo.ArchCalc_Integrals_CoeffTransformation_DisabledStatus where ti_id in (select distinct ti_id from #tis where ClosedPeriod_ID is null)
and dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
order by ti_id, ClosedPeriod_ID, StartDateTime

-- Количество ПУ
select Count(*) as puNumbers from #tis;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select distinct ClosedPeriod_ID 
into #cIds
from #tis
declare @closedEnabled bit, @openEnabled bit, @defaultGuide uniqueidentifier
set @closedEnabled = case when exists (select top 1 1 from #tis) then 1 else 0 end;
set @openEnabled = case when exists (select top 1 1 from @TI_Array where ClosedPeriod_ID is null) then 1 else 0 end;
set @defaultGuide = NEWID(); --Для открытых таблиц

--Формирование таблицы общих приоритетных источников с действием по своим месяца
create table #monthOpen -- Таблица источников
(
	ClosedPeriod_ID uniqueidentifier,
	startMonthYear DateTime,
	endMonthYear DateTime,
	dataSource_ID int,
	dataSourceType tinyint,
	[Priority] int,
	PRIMARY KEY CLUSTERED (ClosedPeriod_ID, startMonthYear, [Priority] desc)
		WITH (IGNORE_DUP_KEY = ON) 
)

declare @date datetime, @dataSource_id int;
set @date = dateadd(mm,(Year(@DateStart)-1900)* 12 + Month(@DateStart) - 1,0) --Дата на начало месяца
while @date <= dateadd(mm,(Year(@DateEnd)-1900)* 12 + Month(@DateEnd) - 1,0)
begin
	if (@openEnabled = 1) begin 
		insert into  #monthOpen 
		select @defaultGuide, @date, DateAdd(month, 1, @date), ISNULL(pl.DataSource_ID, 0), isnull(l.DataSourceType, 0), isnull(pl.[Priority], -1)
		from  Expl_DataSource_List l
		left join Expl_DataSource_PriorityList pl on pl.DataSource_ID = l.DataSource_ID
		and pl.[Year] = Year(@date) and pl.[Month] = Month(@date)
		--where ISNULL(pl.DataSource_ID, -1) > 0 
	end 

	if (@closedEnabled = 1) begin 
		insert into  #monthOpen 
		select c.ClosedPeriod_ID, @date, DateAdd(month, 1, @date), ISNULL(p.DataSource_ID, 0), isnull(l.DataSourceType, 0), isnull(p.[Priority], -1)
		from Expl_DataSource_PriorityList_Closed p 
		join Expl_DataSource_List l on p.DataSource_ID = l.DataSource_ID
		join Expl_ClosedPeriod_List c on p.ClosedPeriod_ID = c.ClosedPeriod_ID
		where p.ClosedPeriod_ID in (select ClosedPeriod_ID from #cIds) and  [Year] = Year(@date) and [Month] = Month(@date) 
		--and ISNULL(p.DataSource_ID, -1) > 0
	end;

	set @date = dateadd(month, 1, @date)
end

declare @monthYearStart int, @monthYearEnd int
set @monthYearStart = Year(@dateStart) * 12 + Month(@dateStart);
set @monthYearEnd = Year(@dateEnd) * 12 + Month(@dateEnd);

--create table #filter -- Таблица источников
--(
--	TI_ID int, 
--	TP_ID int null, 
--	monthYear DateTime, 
--	ChannelType tinyint, 
--	DataSource_ID int, 
--	DataSourceType tinyint,
--	ClosedPeriod_ID uniqueidentifier
--)

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

declare 
@ParmDefinition NVARCHAR(1000),
@SQLString NVARCHAR(4000),
@SqlPrefix NVARCHAR(700),
@SqlTable NVARCHAR(200),
@SqlWhere NVARCHAR(200),
@sqlClosedFilter NVARCHAR(100),
@sqlDataSourceFilter NVARCHAR(500),
@sqlTableNumber NVARCHAR(3);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

set @sqlDataSourceFilter = '(select top 1 DataSource_ID from #monthOpen where EventDateTime between startMonthYear and endMonthYear) '

SET @ParmDefinition = N'@TI_ID int,@DateStart DateTime,@DateEnd DateTime,@ChannelType tinyint,@ClosedPeriod_ID uniqueidentifier,@TP_ID int, @dataSource_ID int,@monthYearStart int, @monthYearEnd int,@DataSourceType tinyint,@isAutoRead bit, @IsChannelsInverted bit'
set @SqlPrefix = N'select a.[EventDateTime],a.[Data],a.[IntegralType],a.[DispatchDateTime],a.[Status] ';
set @SqlWhere =' where a.TI_ID = @ti_id and ChannelType = @ChannelType and EventDateTime between @dateStart and @dateEnd and (@isAutoRead = 0 or IntegralType = 0)';

declare @backStart DateTime;
if (@isFindBack = 1) set @backStart = DateAdd(hh, -2, @DateStart);
else set @backStart = @DateStart;

--Теперь перебираем ПУ и возвращаем информацию по каждому
declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select TI_ID, ChannelType, ISNULL(closedPeriod_ID, @defaultGuide), ISNULL(TP_ID, -1), 
	DataSource_ID,DataSourceType, IsCoeffTransformationDisabled, titype, AbsentChannelsMask, PS_ID, IsChannelsInverted from #tis
  open t;
	FETCH NEXT FROM t into @TI_ID, @ChannelType, @closedPeriod_ID, @TP_ID, @dataSource_ID, @DataSourceType, @IsCoeffTransformationDisabled, 
	@titype, @AbsentChannelsMask, @PS_ID, @IsChannelsInverted

	WHILE @@FETCH_STATUS = 0
	BEGIN

		set @IsAbsentChannel = 0;
		set @SQLString = '';

		--4 результат (Параметры ПУ)
		select @TI_ID as TI_ID, @ChannelType as ChannelType, 
		case when (@closedPeriod_ID = @defaultGuide) then null else @closedPeriod_ID end as closedPeriod_ID, 
		case when @TP_ID < 0 then null else @TP_ID end as TP_ID, 
		@dataSourceType as  dataSourceType,
		@PS_ID as PS_ID, @IsCoeffTransformationDisabled as IsCoeffTransformationDisabled, @titype as titype, @IsAbsentChannel as IsAbsentChannel, @IsChannelsInverted as IsChannelReverse

		--5 результат (Данные)
		--if (@IsAbsentChannel = 0) begin --Продолжаем дальше смотреть только при наличии канала		
			
			--Таблицы для чтения
			--ArchCalcBit_Integrals_Closed_  --закрытый период, бытовая точка
			--ArchBit_Integrals_ -- бытовая точка открытый период тип принудительно выбран 0
			--ArchCalcBit_Integrals_Virtual_ -- бытовая точка открытый период тип любой кроме 0
			--ArchCalc_Integrals_Closed  --не бытовая, закрытый период
			--ArchComm_Integrals -- не бытовая, открытый период тип принудительно выбран 0
			--ArchCalc_Integrals_Virtual --не бытовая, открытый период тип любой кроме 0

			--Таблица из которой читаем
			if (@titype > 10) begin
				--Бытовая точка
				set @sqlTableNumber = ltrim(str(@titype - 10,2));
				if (@ClosedPeriod_ID <> @defaultGuide) begin
					set @SqlTable = 'ArchCalcBit_Integrals_Closed_' + @sqlTableNumber + ' a ';
					set @sqlClosedFilter = ' and f.ClosedPeriod_ID = a.ClosedPeriod_ID ';
				end else begin 
					if (@DataSourceType = 0 and @IsReadCalculatedValues = 0) set @SqlTable = 'ArchBit_Integrals_' + @sqlTableNumber + ' a '; --Это старые таблицы основного профиля
					else set @SqlTable = 'ArchCalcBit_Integrals_Virtual_' + @sqlTableNumber + ' a ';
					set @sqlClosedFilter = ' ';
				end
			end else begin
				if (@ClosedPeriod_ID <> @defaultGuide) begin 
					set @SqlTable = 'ArchCalc_Integrals_Closed' + ' a ';
					set @sqlClosedFilter = ' and f.ClosedPeriod_ID = a.ClosedPeriod_ID ';
				end else begin
					if (@DataSourceType = 0 and @IsReadCalculatedValues = 0) set @SqlTable = 'ArchComm_Integrals' + ' a '; --Это старые таблицы основного профиля
					else set @SqlTable = 'ArchCalc_Integrals_Virtual' + ' a ';
					set @sqlClosedFilter = ' ';
				end
			end;

			--Условия чтения
			if (@dataSource_ID is null) begin 
				--С автоматическим выбором приоритетного источника
				--SET @SQLString = @SqlPrefix + N', a.ManualEnterData 
				--from #filter f
				--join ' + @SqlTable + ' on f.TI_ID = a.TI_ID and a.ChannelType = f.ChannelType and a.DataSource_ID = f.DataSource_ID 
				--and a.EventDateTime > f.monthYear and a.EventDateTime <= DateAdd(month, 1, f.monthYear) ' + @sqlClosedFilter +
				--'where f.TI_ID = @ti_id and f.ChannelType = @ChannelType and f.TP_ID = @tp_id
				--and (@isAutoRead = 0 or a.IntegralType = 0) and a.EventDateTime between @dateStart and @dateEnd '

				SET @SQLString = @SqlPrefix + ', a.ManualEnterData, dsl.DataSourceType, pl.[Priority], case when titp.TI_ID is not null then 1 else 0 end as IsManulaySetPriority'
					+ ' from ' + @SqlTable + N' join [Expl_DataSource_PriorityList] pl on pl.DataSource_ID = a.DataSource_ID and pl.[Month] = Month(DateAdd(s, -1,a.EventDateTime)) and pl.[Year] = Year(DateAdd(s, -1,a.EventDateTime)) 
					left join [dbo].[Expl_DataSource_To_TI_TP] titp on titp.TI_ID = a.TI_ID and titp.TP_ID = @tp_id and titp.DataSource_ID = a.DataSource_ID and titp.[Month] = Month(DateAdd(s, -1,a.EventDateTime)) and titp.[Year] = Year(DateAdd(s, -1,a.EventDateTime)) 
					join dbo.usf2_ArchCalcChannelInversionStatus(@TI_ID, @ChannelType, @DateStart, @DateEnd, @IsChannelsInverted) c on a.ChannelType=c.ChannelType and a.EventDateTime between c.StartChannelStatus and c.FinishChannelStatus
					join [Expl_DataSource_List] dsl on a.DataSource_ID = dsl.DataSource_ID 
					where a.TI_ID = @ti_id and a.EventDateTime between @dateStart and @dateEnd and (@isAutoRead = 0 or a.IntegralType = 0) and (Data >= 0 OR ManualEnterData >= 0)' 

			end else begin 
				--С прямым указанием приоритетного источника
				SET @SQLString = @SqlPrefix + ',@dataSourceType as DataSourceType, 0 as [Priority], 0 as IsManulaySetPriority';

				if (@DataSourceType = 0 and @IsReadCalculatedValues = 0) SET @SQLString = @SQLString + ' from ' + @SqlTable +  @SqlWhere;
				else set @SQLString = @SQLString + ', a.[IsUsedForFillHalfHours], a.ManualEnterData from ' + @SqlTable +  @SqlWhere + ' and a.DataSource_ID = @DataSource_ID and (Data >= 0 OR ManualEnterData >= 0)';
					
			end;

			if (@closedPeriod_ID <> @defaultGuide) SET @SQLString += ' and a.ClosedPeriod_ID = @ClosedPeriod_ID';

			--Сортировка 
			SET @SQLString += ' order by a.[EventDateTime], IsManulaysetPriority desc, [Priority] desc'

			print @SQLString;
			EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID, @backStart, @DateEnd, @ChannelType, @ClosedPeriod_ID, @TP_ID, @dataSource_ID, @monthYearStart, @monthYearEnd, 
			@DataSourceType, @isAutoRead, @IsChannelsInverted

		--end;		
		
	FETCH NEXT FROM t into @TI_ID, @ChannelType, @closedPeriod_ID, @TP_ID, @dataSource_ID, @DataSourceType, 
		@IsCoeffTransformationDisabled, @titype, @AbsentChannelsMask, @PS_ID, @IsChannelsInverted		

	end;
	CLOSE t
	DEALLOCATE t
	
--drop table #filter;
drop table #cIds;
drop table #tis;
drop table #monthOpen;
end
  go
  grant EXECUTE on usp2_ArchComm_Integrals_ReadArray to [UserCalcService]
  go
  grant EXECUTE on usp2_ArchComm_Integrals_ReadArray to [UserSlave61968Service]
  go

grant EXECUTE on usp2_ArchComm_Integrals_ReadArray to UserExportService
go

grant EXECUTE on usp2_ArchComm_Integrals_ReadArray to UserImportService
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Июнь, 2013
--
-- Описание:
--
--		Выбирает значения барабанов для дополнительных каналов
--
-- ======================================================================================

CREATE proc [dbo].[usp2_ArchComm_Integrals_ExtendedChannels_ReadArray]
	@TI_ID int,
	@dateStart datetime,
	@dateEnd datetime
as
begin

set transaction isolation level read uncommitted
set nocount on

declare 
@tableName varchar(40), @TIType int
select @TIType = TIType from dbo.Info_TI where TI_ID = @TI_ID

if (@TIType > 10) set @tableName ='ArchBit_ExtendedData_' + ltrim(rtrim(str(@TIType-10)));
else set @tableName ='ArchComm_ExtendedData';

DECLARE @ParmDefinition NVARCHAR(1000);
SET @ParmDefinition = N'@TI_ID int,@DateStart DateTime,@DateEnd DateTime'
DECLARE @SQLString NVARCHAR(4000);
	
SET @SQLString = N'select t1.EventDateTime,t1.Data,t1.ValidStatus,t1.ExtendedChannelType_ID ' +
	' from '+ @tableName + ' t1 
	where t1.TI_ID = @TI_ID and (EventDateTime between @DateStart and @DateEnd)		
	order by t1.EventDateTime' 	 

	--select @SQLString
	EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID, @DateStart,@DateEnd;

end
go
   grant EXECUTE on usp2_ArchComm_Integrals_ExtendedChannels_ReadArray to [UserCalcService]
go
   grant EXECUTE on usp2_ArchComm_Integrals_ExtendedChannels_ReadArray to UserExportService
go

grant EXECUTE on usp2_ArchComm_Integrals_ExtendedChannels_ReadArray to UserImportService
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
--		Выборка точек у которых обнаружены изменения НСИ между открытыми данными и закрытыми
--
-- ======================================================================================
create proc [dbo].[usp2_Expl_FindTiWhereChangedNsi]
	@dtStart DateTime,
	@dtEnd DateTime,
	@tiArray dbo.TiChannelType READONLY
as

--Необходимо проверить изменения
--Info_Section_Description_Closed
--Info_TP2_OurSide_Formula_List_Closed
--Info_TP2_OurSide_Formula_Description_Closed
--Коэфф. трансф.
--Info_Transformators_Closed
begin
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

create table #result
(
	TI_ID int,
	TP_ID int,
	Section_ID int,
	DetectedNsiChanges int,
	ChangesKey varchar(4000),
	PRIMARY KEY CLUSTERED (TI_ID, TP_ID, Section_ID)
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF, IGNORE_DUP_KEY = ON)
)


--Изменения в составе сечений
insert into #result (TI_ID, TP_ID, Section_ID,  DetectedNsiChanges, ChangesKey)
select distinct TI_ID, TP_ID, Section_ID, 1, '|1:' + convert(varchar, StartDateTime, 120) + ';'+ convert(varchar, ISNULL(FinishDateTime, '21000101'), 120) + ';' + ltrim(str(IsTransit))  from 
(
	select c.Section_ID, c.TP_ID, c.StartDateTime, c.FinishDateTime, c.IsTransit, c.CUS_ID, ti.TI_ID from @tiArray ti
	join Info_Section_Description_Closed c on c.TP_ID = ti.TP_ID and c.ClosedPeriod_ID = ti.ClosedPeriod_ID
	where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
	except
	select o.Section_ID, o.TP_ID, o.StartDateTime, o.FinishDateTime, o.IsTransit, o.CUS_ID, ti.TI_ID from @tiArray ti
	join Info_Section_Description2 o on o.TP_ID = ti.TP_ID
	where o.StartDateTime <= @dtEnd and ISNULL(o.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
) e
union 
select distinct TI_ID, TP_ID, Section_ID, 1, '|1:' + convert(varchar, StartDateTime, 120) + ';' + convert(varchar, ISNULL(FinishDateTime, '21000101'), 120) + ';' + ltrim(str(IsTransit)) from 
(
	select o.Section_ID, o.TP_ID, o.StartDateTime, o.FinishDateTime, o.IsTransit, o.CUS_ID, ti.TI_ID from @tiArray ti
	join Info_Section_Description2 o on o.TP_ID = ti.TP_ID
	where o.StartDateTime <= @dtEnd and ISNULL(o.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
	except
	select c.Section_ID, c.TP_ID, c.StartDateTime, c.FinishDateTime, c.IsTransit, c.CUS_ID, ti.TI_ID from @tiArray ti
	join Info_Section_Description_Closed c on c.TP_ID = ti.TP_ID and c.ClosedPeriod_ID = ti.ClosedPeriod_ID
	where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
) e

--Изменения в формулах, делим на разные этапы и используем option т.к. выполняется этот участок чрезвычайно долго

	select distinct TI_ID, TP_ID, Section_ID,ChangesKey
	into #ttt
	from 
	(
		select distinct ti.TI_ID, c.TP_ID, c.Formula_UN, c.ChannelType, c.FormulaType_ID, c.StartDateTime, c.FinishDateTime, c.ForAutoUse, sd.Section_ID,
		'|2:' + c.Formula_UN + ltrim(convert(varchar, d.ChannelType)) + ltrim(convert(varchar, c.ChannelType)) + ltrim(convert(varchar, ForAutoUse))
		+ convert(varchar, c.StartDateTime, 120) + ';' + convert(varchar, ISNULL(c.FinishDateTime, '21000101'), 120)
		+ ltrim(convert(varchar, isnull(d.TI_ID, 0))) + ';' + ltrim(convert(varchar, isnull(d.TP_ID, 0))) + ';' + OperAfter + OperBefore + ';' + ISNULL(UsedFormula_UN, '') as ChangesKey
		from 
		@tiArray ti 
		join Info_TP2_OurSide_Formula_List c on ti.TP_ID = c.TP_ID and c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart
		join Info_TP2_OurSide_Formula_Description d on d.Formula_UN = c.Formula_UN and d.TI_ID = ti.TI_ID
		join Info_Section_Description2 sd on sd.TP_ID = ti.TP_ID
		where ti.ClosedPeriod_ID is null
		except
		select distinct ti.TI_ID, c.TP_ID, c.Formula_UN, c.ChannelType, c.FormulaType_ID, c.StartDateTime, c.FinishDateTime, c.ForAutoUse, sd.Section_ID,
		'|2:' + c.Formula_UN + ltrim(convert(varchar, d.ChannelType)) + ltrim(convert(varchar, c.ChannelType)) + ltrim(convert(varchar, ForAutoUse))
		+ convert(varchar, c.StartDateTime, 120) + ';' + convert(varchar, ISNULL(c.FinishDateTime, '21000101'), 120)
		+ ltrim(convert(varchar, isnull(d.TI_ID, 0))) + ';' + ltrim(convert(varchar, isnull(d.TP_ID, 0))) + ';' + OperAfter + OperBefore + ';' + ISNULL(UsedFormula_UN, '') as ChangesKey
		from @tiArray ti 
		join Info_TP2_OurSide_Formula_List_Closed c on ti.TP_ID = c.TP_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID and c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart 
		join Info_TP2_OurSide_Formula_Description_Closed d on d.Formula_UN = c.Formula_UN and d.ClosedPeriod_ID = c.ClosedPeriod_ID and d.TI_ID = ti.TI_ID
		join Info_Section_Description_Closed sd on sd.TP_ID = ti.TP_ID and sd.ClosedPeriod_ID = ti.ClosedPeriod_ID
		where ti.ClosedPeriod_ID is not null
	) a
	option (hash join, merge join)

	insert into #ttt
	select distinct TI_ID, TP_ID, Section_ID,ChangesKey
	from 
	(
		select distinct ti.TI_ID, c.TP_ID, c.Formula_UN, c.ChannelType, c.FormulaType_ID, c.StartDateTime, c.FinishDateTime, c.ForAutoUse, sd.Section_ID,
		'|2:' + c.Formula_UN + ltrim(convert(varchar, d.ChannelType)) + ltrim(convert(varchar, c.ChannelType)) + ltrim(convert(varchar, ForAutoUse))
		+ convert(varchar, c.StartDateTime, 120) + ';' + convert(varchar, ISNULL(c.FinishDateTime, '21000101'), 120)
		+ ltrim(convert(varchar, isnull(d.TI_ID, 0))) + ';' + ltrim(convert(varchar, isnull(d.TP_ID, 0))) + ';' + OperAfter + OperBefore + ';' + ISNULL(UsedFormula_UN, '') as ChangesKey
		from 
		@tiArray ti 
		join Info_TP2_OurSide_Formula_List_Closed c on ti.TP_ID = c.TP_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID and c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart 
		join Info_TP2_OurSide_Formula_Description_Closed d on d.Formula_UN = c.Formula_UN and d.ClosedPeriod_ID = c.ClosedPeriod_ID and d.TI_ID = ti.TI_ID
		join Info_Section_Description_Closed sd on sd.TP_ID = ti.TP_ID and sd.ClosedPeriod_ID = ti.ClosedPeriod_ID
		where ti.ClosedPeriod_ID is not null
		except
		select distinct ti.TI_ID, c.TP_ID, c.Formula_UN, c.ChannelType, c.FormulaType_ID, c.StartDateTime, c.FinishDateTime, c.ForAutoUse, sd.Section_ID,
		'|2:' + c.Formula_UN + ltrim(convert(varchar, d.ChannelType)) + ltrim(convert(varchar, c.ChannelType)) + ltrim(convert(varchar, ForAutoUse))
		+ convert(varchar, c.StartDateTime, 120) + ';' + convert(varchar, ISNULL(c.FinishDateTime, '21000101'), 120)
		+ ltrim(convert(varchar, isnull(d.TI_ID, 0))) + ';' + ltrim(convert(varchar, isnull(d.TP_ID, 0))) + ';' + OperAfter + OperBefore + ';' + ISNULL(UsedFormula_UN, '') as ChangesKey
		from @tiArray ti 
		join Info_TP2_OurSide_Formula_List c on ti.TP_ID = c.TP_ID and c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart
		join Info_TP2_OurSide_Formula_Description d on d.Formula_UN = c.Formula_UN and d.TI_ID = ti.TI_ID
		join Info_Section_Description2 sd on sd.TP_ID = ti.TP_ID
		where ti.ClosedPeriod_ID is null
	) a
	where not exists (select top 1 1 from #ttt where TI_ID = a.TI_ID and TP_ID = a.TP_ID and Section_ID = a.Section_ID)
	option (hash join, merge join)

merge #result as r 
using 
(
	select TI_ID,TP_ID,Section_ID, 2 as DetectedNsiChanges,
	stuff((select ';' + ChangesKey from #ttt where TI_ID = t.TI_ID and TP_ID = t.TP_ID and Section_ID = t.Section_ID FOR XML PATH(''), TYPE).value('.','varchar(max)'), 1,1,'') as ChangesKey
	from #ttt t
	group by TI_ID, TP_ID, Section_ID
) n 
on r.TI_ID = n.TI_ID and r.TP_ID = n.TP_ID and r.Section_ID = n.Section_ID
when matched then update set DetectedNsiChanges = r.DetectedNsiChanges | n.DetectedNsiChanges, ChangesKey = r.ChangesKey + n.ChangesKey
WHEN NOT MATCHED THEN  insert (TI_ID, TP_ID, Section_ID, DetectedNsiChanges, ChangesKey)
values (n.TI_ID, n.TP_ID, n.Section_ID, n.DetectedNsiChanges,ChangesKey);

drop table #ttt;

--Изменения в коэффициентах трансформации
merge #result as r 
using (select n.TI_ID, TP_ID, DetectedNsiChanges, Section_ID, 
ISNULL('|4:' + convert(varchar, t.StartDateTime, 120) + ';' + convert(varchar, ISNULL(t.FinishDateTime, '21000101'), 120) + ';' + ltrim(convert(varchar, t.COEFI)) + ';' + ltrim(convert(varchar, t.COEFU)), 'Удален') as ChangesKey 
from 
	(
		select distinct TI_ID, TP_ID, 4 as DetectedNsiChanges, Section_ID from 
		(
			select c.TI_ID, ti.TP_ID, c.COEFI, c.COEFU, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime, 
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_Transformators_Closed c on ti.TI_ID = c.TI_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
			except
			select c.TI_ID, ti.TP_ID, c.COEFI, c.COEFU, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_Transformators c on ti.TI_ID = c.TI_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
		) e where e.Section_ID is not null
		union
		select distinct TI_ID, TP_ID, 4 as DetectedNsiChanges, Section_ID from 
		(
			select c.TI_ID, ti.TP_ID, c.COEFI, c.COEFU, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_Transformators c on ti.TI_ID = c.TI_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
			except
			select c.TI_ID, ti.TP_ID, c.COEFI, c.COEFU, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_Transformators_Closed c on ti.TI_ID = c.TI_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
		) e where e.Section_ID is not null
	) n 
	outer apply 
	(
		select top 1 * from Info_Transformators
		where TI_ID = n.TI_ID and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
		order by StartDateTime desc
	) t
) as n on r.TI_ID = n.TI_ID and r.TP_ID = n.TP_ID and r.Section_ID = n.Section_ID
when matched then update set DetectedNsiChanges = r.DetectedNsiChanges | n.DetectedNsiChanges, ChangesKey = r.ChangesKey + n.ChangesKey
WHEN NOT MATCHED THEN  insert (TI_ID, TP_ID, Section_ID, DetectedNsiChanges, ChangesKey)
values (n.TI_ID, n.TP_ID, n.Section_ID, n.DetectedNsiChanges, ChangesKey);

--Изменения максимальной мощности
merge #result as r 
using (select TI_ID, n.TP_ID, DetectedNsiChanges, Section_ID , 
ISNULL('|16:' + convert(varchar, t.StartDateTime, 120) + ';' + convert(varchar, ISNULL(t.FinishDateTime, '21000101'), 120) + ';' + ltrim(convert(varchar, AssertedPower)), 'Удален') as ChangesKey 
from 
	(
		select distinct TI_ID, TP_ID, 16 as DetectedNsiChanges, Section_ID from 
		(
			select ti.TI_ID, ti.TP_ID, c.MaximumPower, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID 
			from @tiArray ti
			join Info_TP2_Power_Closed c on ti.TP_ID = c.TP_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
			except
			select ti.TI_ID, ti.TP_ID, c.MaximumPower, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID 
			from @tiArray ti
			join Info_TP2_Power c on ti.TP_ID = c.TP_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
		) e where e.Section_ID is not null
		union
		select distinct TI_ID, TP_ID, 16 as DetectedNsiChanges, Section_ID from 
		(
			select ti.TI_ID, ti.TP_ID, c.MaximumPower, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_TP2_Power c on ti.TP_ID = c.TP_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
			except
			select ti.TI_ID, ti.TP_ID, c.MaximumPower, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_TP2_Power_Closed c on ti.TP_ID = c.TP_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
		) e where e.Section_ID is not null
	) n 
	outer apply 
	(
		select top 1 * from Info_TP2_Power
		where TP_ID = n.TP_ID and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
		order by StartDateTime desc
	) t
) as n on r.TI_ID = n.TI_ID and r.TP_ID = n.TP_ID and r.Section_ID = n.Section_ID
when matched then update set DetectedNsiChanges = r.DetectedNsiChanges | n.DetectedNsiChanges, ChangesKey = r.ChangesKey + n.ChangesKey
WHEN NOT MATCHED THEN  insert (TI_ID, TP_ID, Section_ID, DetectedNsiChanges, ChangesKey)
values (n.TI_ID, n.TP_ID, n.Section_ID, n.DetectedNsiChanges, n.ChangesKey);

--Изменения тарифного расписания
merge #result as r 
using (select TI_ID, n.TP_ID, DetectedNsiChanges, Section_ID, 
ISNULL('|32:' + convert(varchar, t.StartDateTime, 120) + ';' + convert(varchar, ISNULL(t.FinishDateTime, '21000101'), 120) + ';' + ltrim(convert(varchar, VoltageLevel)), 'Удален') as ChangesKey 
from 
	(
		select distinct TI_ID, TP_ID, 32 as DetectedNsiChanges, Section_ID from 
		(
			select ti.TI_ID, ti.TP_ID, c.VoltageLevel, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID 
			from @tiArray ti
			join Info_TP_VoltageLevel_Closed c on ti.TP_ID = c.TP_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
			except
			select ti.TI_ID, ti.TP_ID, c.VoltageLevel, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID 
			from @tiArray ti
			join Info_TP_VoltageLevel c on ti.TP_ID = c.TP_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
		) e where e.Section_ID is not null
		union
		select distinct TI_ID, TP_ID, 32 as DetectedNsiChanges, Section_ID from 
		(
			select ti.TI_ID, ti.TP_ID, c.VoltageLevel, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_TP_VoltageLevel c on ti.TP_ID = c.TP_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
			except
			select ti.TI_ID, ti.TP_ID, c.VoltageLevel, ISNULL(c.FinishDateTime, '21000101') as FinishDateTime, c.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID
			from @tiArray ti
			join Info_TP_VoltageLevel_Closed c on ti.TP_ID = c.TP_ID and ti.ClosedPeriod_ID = c.ClosedPeriod_ID
			where c.StartDateTime <= @dtEnd and ISNULL(c.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
		) e where e.Section_ID is not null
	) n 
	outer apply 
	(
		select top 1 * from Info_TP_VoltageLevel
		where TP_ID = n.TP_ID and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
		order by StartDateTime desc
	) t
) as n on r.TI_ID = n.TI_ID and r.TP_ID = n.TP_ID and r.Section_ID = n.Section_ID
when matched then update set DetectedNsiChanges = r.DetectedNsiChanges | n.DetectedNsiChanges, ChangesKey = r.ChangesKey + n.ChangesKey
WHEN NOT MATCHED THEN  insert (TI_ID, TP_ID, Section_ID, DetectedNsiChanges, ChangesKey)
values (n.TI_ID, n.TP_ID, n.Section_ID, n.DetectedNsiChanges, n.ChangesKey);


--Изменения составных формул
merge #result as r 
using (select TI_ID, n.TP_ID, DetectedNsiChanges, Section_ID, 
ISNULL('|2048:' + convert(varchar, t.StartDateTime, 120) + ';' + convert(varchar, ISNULL(t.FinishDateTime, '21000101'), 120) + ';' + ltrim(convert(varchar, VoltageLevel)), 'Удален') as ChangesKey 
from 
	(
		select distinct TI_ID, TP_ID, 2048 as DetectedNsiChanges, Section_ID from 
		(
			select ti.TI_ID, ti.TP_ID, l.ChannelType, ISNULL(l.FinishDateTime, '21000101') as FinishDateTime, l.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID,
			LinkType, Formula_UN 
			from @tiArray ti
			join Info_TP_LinkedFormulas_List_Closed l on ti.TP_ID = l.TP_ID and ti.ClosedPeriod_ID = l.ClosedPeriod_ID
			join Info_TP_LinkedFormulas_OurSide_Description_Closed d on d.LinkedFormula_UN = l.LinkedFormula_UN
			where l.StartDateTime <= @dtEnd and ISNULL(l.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
			except
			select ti.TI_ID, ti.TP_ID, l.ChannelType, ISNULL(l.FinishDateTime, '21000101') as FinishDateTime, l.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID,
			LinkType, Formula_UN				 
			from @tiArray ti
			join Info_TP_LinkedFormulas_List l on ti.TP_ID = l.TP_ID
			join Info_TP_LinkedFormulas_OurSide_Description d on d.LinkedFormula_UN = l.LinkedFormula_UN
			where l.StartDateTime <= @dtEnd and ISNULL(l.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
		) e where e.Section_ID is not null
		union
		select distinct TI_ID, TP_ID, 2048 as DetectedNsiChanges, Section_ID from 
		(
			select ti.TI_ID, ti.TP_ID, l.ChannelType, ISNULL(l.FinishDateTime, '21000101') as FinishDateTime, l.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description2 where TP_ID = ti.TP_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID,
			LinkType, Formula_UN				 
			from @tiArray ti
			join Info_TP_LinkedFormulas_List l on ti.TP_ID = l.TP_ID
			join Info_TP_LinkedFormulas_OurSide_Description d on d.LinkedFormula_UN = l.LinkedFormula_UN
			where l.StartDateTime <= @dtEnd and ISNULL(l.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is null
			except
			select ti.TI_ID, ti.TP_ID, l.ChannelType, ISNULL(l.FinishDateTime, '21000101') as FinishDateTime, l.StartDateTime,
			(select top 1 Section_ID from Info_Section_Description_Closed where TP_ID = ti.TP_ID  and ClosedPeriod_ID = ti.ClosedPeriod_ID
				and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart ) as Section_ID,
			LinkType, Formula_UN 
			from @tiArray ti
			join Info_TP_LinkedFormulas_List_Closed l on ti.TP_ID = l.TP_ID and ti.ClosedPeriod_ID = l.ClosedPeriod_ID
			join Info_TP_LinkedFormulas_OurSide_Description_Closed d on d.LinkedFormula_UN = l.LinkedFormula_UN
			where l.StartDateTime <= @dtEnd and ISNULL(l.FinishDateTime, '21000101') >= @dtStart and ti.ClosedPeriod_ID is not null
		) e where e.Section_ID is not null
	) n 
	outer apply 
	(
		select top 1 * from Info_TP_VoltageLevel
		where TP_ID = n.TP_ID and StartDateTime<=@dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
		order by StartDateTime desc
	) t
) as n on r.TI_ID = n.TI_ID and r.TP_ID = n.TP_ID and r.Section_ID = n.Section_ID
when matched then update set DetectedNsiChanges = r.DetectedNsiChanges | n.DetectedNsiChanges, ChangesKey = r.ChangesKey + n.ChangesKey
WHEN NOT MATCHED THEN  insert (TI_ID, TP_ID, Section_ID, DetectedNsiChanges, ChangesKey)
values (n.TI_ID, n.TP_ID, n.Section_ID, n.DetectedNsiChanges, n.ChangesKey);

select * from #result;
end
go
   grant EXECUTE on usp2_Expl_FindTiWhereChangedNsi to [UserCalcService]
go
   grant EXECUTE on usp2_Expl_FindTiWhereChangedNsi to UserExportService
go

grant EXECUTE on usp2_Expl_FindTiWhereChangedNsi to UserImportService
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2013
--
-- Описание:
--
--		Последние показания на начало и окончание по списку точек
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Integral_Last]
	@tiArray dbo.TiChannelType READONLY,
	@DaysLimit tinyint,
	@dtStart datetime,
	@dtEnd datetime = null,
	@IsMeterChangeInfoRead bit = 0,
	@IsSearchBack bit = 0
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
create table #result
(
	TI_ID int,
	ChannelType tinyint,
	ClosedPeriod_ID uniqueidentifier NULL,
	DataSourceType tinyint NULL,
	TP_ID int NULL,
	JuridicalPersonContract_ID int NULL,
	MeterSerialNumber varchar(255),
	DataStart float,
	EventDateTimeStart dateTime,
	ManualEnterDataStart float,
	CoeffStart int, 
	DataSourceTypeStart tinyint,
	StatusStart int,
	DataEnd float,
	EventDateTimeEnd dateTime,
	ManualEnterDataEnd float,
	CoeffEnd int, 
	DataSourceTypeEnd tinyint,
	StatusEnd int
);
select @dtStart = DateAdd(hour, 2, @dtStart), @dtEnd = DATEADD(hour, 2, @dtEnd);


select distinct ids.TI_ID, ids.TP_ID, ids.ClosedPeriod_ID, ids.DataSourceType, ids.ChannelType as DirectChannel, TIType, AIATSCode,AOATSCode,RIATSCode,ROATSCode
,dbo.usf2_ReverseTariffChannel(0,ids.ChannelType,ti.AIATSCode,ti.AOATSCode,ti.RIATSCode,ti.ROATSCode,ti.TI_ID,@dtStart, @dtStart) as ChannelType
into #tmp
from @tiArray ids
join Info_TI ti on ids.TI_ID = ti.TI_ID
order by TIType

declare @isEndNotExists bit;
if (@dtEnd is not null) set @isEndNotExists = 0;
else set @isEndNotExists = 1;

set @IsSearchBack = (case when @IsSearchBack = 1 then 1 else @isEndNotExists end);

DECLARE @sqlString NVARCHAR(max);

set @SQLString =N'insert into #result(TI_ID, ChannelType,	ClosedPeriod_ID, DataSourceType, TP_ID,	JuridicalPersonContract_ID,	MeterSerialNumber,
	DataStart, EventDateTimeStart, ManualEnterDataStart, CoeffStart, DataSourceTypeStart, StatusStart,
	DataEnd, EventDateTimeEnd, ManualEnterDataEnd, CoeffEnd, DataSourceTypeEnd,	StatusEnd)

select ti.TI_ID, ti.ChannelType, ti.ClosedPeriod_ID, ti.DataSourceType, ti.TP_ID, sjc.JuridicalPersonContract_ID, m.MeterSerialNumber
, sa.Data, sa.EventDateTime, sa.ManualEnterData, sc.Coeff, sa.DataSourceType, sa.Status
, ea.Data, ea.EventDateTime, ea.ManualEnterData, ec.Coeff, ea.DataSourceType, ea.Status
from #tmp ti
outer apply
( 
	select top (1) a.*, (select top 1 DataSourceType from Expl_DataSource_List where DataSource_ID = a.DataSource_ID) as DataSourceType 
	from {ArchiveTable} a  WITH (NOLOCK)  
	left join Expl_DataSource_PriorityList pl on pl.dataSource_id = a.dataSource_id  
	where TI_ID = ti.TI_ID and EventDateTime <= @dtStart and ChannelType = ti.ChannelType
	and (Data >= 0 OR ManualEnterData >= 0) 
	order by EventDateTime desc
) sa  
outer apply
( 
	select top (1) a.*, (select top 1 DataSourceType from Expl_DataSource_List where DataSource_ID = a.DataSource_ID) as DataSourceType 
	from {ArchiveTable} a   WITH (NOLOCK) 
	left join Expl_DataSource_PriorityList pl on pl.dataSource_id = a.dataSource_id  
	where @dtEnd is not null and TI_ID = ti.TI_ID and EventDateTime <= @dtEnd and ChannelType = ti.ChannelType
	and (Data >= 0 OR ManualEnterData >= 0) 
	order by EventDateTime desc
) ea
left join Info_Section_Description2 sd 
	on @dtEnd is not null and ti.TP_ID is not null and sd.TP_ID = ti.TP_ID and sd.StartDateTime <= @dtEnd and ISNULL(sd.FinishDateTime, ''21000101'') >= @dtStart
left join Info_Section_To_JuridicalContract sjc on sjc.Section_ID = sd.Section_ID
outer apply
(
	select top 1 MeterSerialNumber 
	from Info_Meters_TO_TI mti 
	join Hard_Meters hm on hm.Meter_ID = mti.METER_ID
	where mti.TI_ID = ti.TI_ID and StartDateTime <= ISNULL(@dtEnd, @dtStart) and ISNULL(FinishDateTime, ''21000101'') >= @dtStart
	order by StartDateTime desc
) m
outer apply
(
	select top (1) COEFU*COEFI as Coeff from Info_Transformators it
	where it.TI_ID = ti.TI_ID and StartDateTime <= @dtStart	and ISNULL(FinishDateTime, ''21000101'') >= @dtStart 
	order by StartDateTime desc
) sc
outer apply
(
	select top (1) COEFU*COEFI as Coeff from Info_Transformators it
	where @dtEnd is not null and it.TI_ID = ti.TI_ID and StartDateTime <= @dtEnd and ISNULL(FinishDateTime, ''21000101'') >= @dtEnd 
	order by StartDateTime desc
) ec
where ti.TIType = @TIType'

if (@IsSearchBack = 0) begin
	--Ищем справа от даты
	set @sqlString = REPLACE(@SQLString, 'EventDateTime <= ', 'EventDateTime >= ')
	set @sqlString = REPLACE(@SQLString, 'EventDateTime desc,', 'EventDateTime desc,')
end

declare @TIType tinyint;
DECLARE @sqlExec NVARCHAR(max), @ParmDefinition NVARCHAR(1000);

SET @ParmDefinition = N'@TIType tinyint, @dtStart DateTime, @dtEnd DateTime'

declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tmp
  open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN
		if (@TIType < 10) begin
			set @sqlExec =REPLACE(@sqlString, '{ArchiveTable}',  'ArchCalc_Integrals_Virtual'); 
		end else begin
			set @sqlExec =REPLACE(@sqlString, '{ArchiveTable}',  'ArchCalcBit_Integrals_Virtual_' + ltrim(str(@TIType - 10,2))); 
		end;

		EXEC sp_executesql @SQLExec,@ParmDefinition,@TIType,@dtStart,@dtEnd;
		--select @SQLExec
		FETCH NEXT FROM t into @TIType
	end;
	CLOSE t
	DEALLOCATE t

--Первый результат
select * from #result 
order by TI_ID, ChannelType, TP_ID,  ClosedPeriod_ID

drop table #tmp;

--Выбираем смену счетчиков
if (@IsMeterChangeInfoRead = 1 AND @dtEnd is not null) begin
	select mti.TI_ID, h.ChannelType, h.LastData, h.FirstData,
	mti.StartDateTime as ExchangeData, hm.MeterSerialNumber as MeterSerialNumberFirst,
	(select MeterSerialNumber 
		from Hard_Meters 
		where Meter_ID = (select top 1 Meter_ID from Info_Meters_TO_TI where TI_ID = mti.TI_ID and ISNULL(FinishDateTime, '21000101') <= mti.StartDateTime order by StartDateTime))
		as MeterSerialNumberLast
	from  #result ti
	join [dbo].[Info_Meters_TO_TI] mti on mti.TI_ID = ti.TI_ID
	join Info_Meters_ReplaceHistory_Channels h on h.MetersReplaceSession_ID = mti.MetersReplaceSession_ID and h.ChannelType = ti.ChannelType
	join Hard_Meters hm on mti.METER_ID = hm.Meter_ID
	where (mti.StartDateTime between @dtStart and @dtEnd)
	order by mti.TI_ID, mti.METER_ID, StartDateTime;
end

drop table #result;

end
go
   grant EXECUTE on usp2_ArchComm_Integral_Last to [UserCalcService]
go
   grant EXECUTE on usp2_ArchComm_Integral_Last to UserExportService
go

grant EXECUTE on usp2_ArchComm_Integral_Last to UserImportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2013
--
-- Описание:
--
--		Выбор тарифных каналов идущих совместно с основными
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetChannelsTarifForTIs]

	@tiArray dbo.TiChannelType READONLY,
	@dtStart datetime,
	@dtEnd datetime

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select ch.TI_ID, ClosedPeriod_ID, DataSourceType, ch.TP_ID, ChannelType 
from
(
	select TI_ID, ClosedPeriod_ID, DataSourceType, TP_ID, ChannelType from @tiArray
	union all
	select ti.TI_ID, ti.ClosedPeriod_ID, ti.DataSourceType, ti.TP_ID, 
	case ti.ChannelType 
		when 1 then dz.ChannelType1
		when 2 then dz.ChannelType2
		when 3 then dz.ChannelType3
		when 4 then dz.ChannelType4
	end as ChannelType from @tiArray ti 
	join DictTariffs_ToTI dt on ti.TI_ID = dt.TI_ID and dt.StartDateTime <= @dtEnd and (dt.FinishDateTime is null OR (dt.FinishDateTime >= @dtStart))
	join DictTariffs_Zones dz on dt.Tariff_ID = dz.Tariff_ID and dz.StartDateTime <= @dtEnd and (dz.FinishDateTime is null OR (dz.FinishDateTime >= @dtStart))
) ch
join Info_TI iti on iti.TI_ID = ch.TI_ID
--Исключаем отсутствующие каналы, учитываем переворот каналов
where (ISNULL(case when ISNULL(AIATSCode, 1) = 1 then ISNULL(iti.AbsentChannelsMask, 0) else ISNULL(iti.AbsentChannelsMask, 0) end, 0) / power(2, ((ChannelType % 10) - 1))) & 1 = 0
order by TI_ID, ClosedPeriod_ID, DataSourceType, TP_ID
end

go
   grant EXECUTE on usp2_Info_GetChannelsTarifForTIs to [UserCalcService]
go
   grant EXECUTE on usp2_Info_GetChannelsTarifForTIs to UserExportService
go

grant EXECUTE on usp2_Info_GetChannelsTarifForTIs to UserImportService
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
--		Выбираем 
--			значение бытового барабанов на 1 число предыдущего месяца до указанной даты (кВт*ч)
--			последнее значение бытового барабанов до указанной даты (кВт*ч)
--			значение бытового барабанов на указанную дату (кВт*ч)
--			первое значение бытового барабанов после указанной даты (кВт*ч)
--
-- ======================================================================================

create proc [dbo].[usp2_ArchBit_SelectNearestIntegral]

	@TIArray dbo.TiChannelType READONLY,
	@EventDate1 datetime

as
begin

set transaction isolation level read uncommitted
set nocount on

declare 
@TIType tinyint,
@prevMonthDateTime DateTime

set @prevMonthDateTime = DateAdd(month, -1, @EventDate1);
set @prevMonthDateTime = DateAdd(day, -day(@prevMonthDateTime) + 1, @prevMonthDateTime);

select usf.TI_ID, usf.ChannelType as DirectChannel, TIType, ti.AIATSCode, dl.DataSource_ID, Coeff = ISNULL((select COEFU*COEFI from Info_Transformators it where it.TI_ID = usf.TI_ID and it.StartDateTime =
							(
							select max(Info_Transformators.StartDateTime)
							from Info_Transformators
							where Info_Transformators.TI_ID = usf.TI_ID
								and Info_Transformators.StartDateTime <= @EventDate1 
								and ISNULL(Info_Transformators.FinishDateTime, '21000101') >= @EventDate1
							)), 1)
into #tmp
from  @TIArray usf
join Info_TI ti on usf.TI_ID = ti.TI_ID
left join Expl_DataSource_List dl on dl.DataSourceType = ISNULL(usf.DataSourceType, 4)
where TIType > 10;


--Результирующая таблица
create table #resultTable
(
 TI_ID int,
 EventDateTime DateTime,
 Data float,
 Coeff int,
 ChannelType tinyint,
 [Param] tinyint, --Параметр (0 - данные за предыдущий месяц, 1 - предыдущие данные, 2 - следующие данные, 3 - данные на ту же дату)
 LastData float, --Последние показания  счетчика
 FirstData float, --Показания нового счетчика с которого начался отчет
 DigitСapacity float, -- Максимальный размер показаний
 [Priority] int, --Приоритет показания
 DataSource_ID int --Идентификатор источника
 PRIMARY KEY CLUSTERED (TI_ID, ChannelType, [Param], [Priority] desc)
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF, IGNORE_DUP_KEY = ON)
)

--Участвуют только бытовые точки
DECLARE @ParmDefinition NVARCHAR(1000), @sqlString1 NVARCHAR(4000), @sqlString2 NVARCHAR(4000)
,@sqlString11 NVARCHAR(4000), @sqlString22 NVARCHAR(4000),
@statement NVARCHAR(MAX), @sqlExec nvarchar(4000);
	SET @ParmDefinition = N'@EventDate1 DateTime, @prevMonthDateTime DateTime, @TIType tinyint'

	--На таблицу приоритетов ТИ-ТП не заморачиваемся
	--Проверяем еще по журналу замены
	insert into #resultTable (TI_ID, EventDateTime, Data, Coeff, ChannelType, [Param]
			, LastData, FirstData, DigitСapacity, [Priority], DataSource_ID)
			select usf.TI_ID, 
			--case when mt.StartDateTime is not null then mt.StartDateTime else EventDateTime end as EventDateTime, 
			EventDateTime,
			case when FirstData is not null then FirstData else (case when ManualEnterData is null or ManualEnterData < 0 then Data  / 1000 else ManualEnterData / 1000 end) end as Data, 
			--Data, 
			Coeff, cast(DirectChannel as tinyint) as ChannelType, 1 as [Param], FirstData, LastData, hm.DigitСapacity, ISNULL(pl.[Priority], 0) as [Priority], a.DataSource_ID
			from #tmp usf
			cross apply
			(
				select top 1 a.* from dbo.usf2_ArchCalcChannelInversionStatus(usf.TI_ID, usf.DirectChannel, DateAdd(month, -3, @EventDate1), @EventDate1, case when usf.AIATSCode = 1 then 0 else 1 end) c
				join vw_ArchCalcBitIntegralsVirtual a on a.ChannelType = c.ChannelType and a.EventDateTime between c.StartChannelStatus and c.FinishChannelStatus
				where a.TiType =usf.TIType and a.TI_ID = usf.TI_ID and a.EventDateTime < @EventDate1
				and  ([Data] >= 0 OR ManualEnterData >= 0)
				order by EventDateTime desc 
			) a
			outer apply
			(
			 select top 1 * from dbo.Info_Meters_TO_TI where a.EventDateTime is not null and TI_ID = usf.TI_ID 
			 and StartDateTime <= @EventDate1 and ISNULL(FinishDateTime, '21000101') > @EventDate1 and StartDateTime > a.EventDateTime
			 order by StartDateTime
			) mt
			left join Info_Meters_ReplaceHistory_Channels rh on rh.MetersReplaceSession_ID = mt.MetersReplaceSession_ID and rh.ChannelType = a.ChannelType and rh.FirstData is not null
			left join Info_Meters_TO_TI mt1 on mt1.TI_ID = usf.TI_ID and mt1.StartDateTime = (select Max(StartDateTime) from Info_Meters_TO_TI where TI_ID = usf.TI_ID and ISNULL(a.EventDateTime, @EventDate1) between StartDateTime and ISNULL(FinishDateTime, '21000101'))
			left join Hard_Meters hm on hm.Meter_ID = mt1.Meter_ID
			left join Expl_DataSource_PriorityList pl on pl.Year = Year(a.EventDateTime) 
			and pl.Month = Month(a.EventDateTime) and pl.DataSource_ID = a.DataSource_ID
			--where TIType = @TIType 
	union all
			select usf.TI_ID, EventDateTime,(case when ManualEnterData is null or ManualEnterData < 0 then Data  / 1000 else ManualEnterData / 1000 end) as Data,
			 Coeff, cast(DirectChannel as tinyint) as ChannelType, 3, null, null, null, ISNULL(pl.[Priority], 0), a.DataSource_ID
			from  #tmp usf
			cross apply
			(
				select top 1 a.* from dbo.usf2_ArchCalcChannelInversionStatus(usf.TI_ID, usf.DirectChannel, @EventDate1, DateAdd(month, 3, @EventDate1), case when usf.AIATSCode = 1 then 0 else 1 end) c
				join vw_ArchCalcBitIntegralsVirtual a on a.ChannelType = c.ChannelType and a.EventDateTime between c.StartChannelStatus and c.FinishChannelStatus
				where a.TiType =usf.TIType and a.TI_ID = usf.TI_ID and a.EventDateTime > @EventDate1
				and  ([Data] >= 0 OR ManualEnterData >= 0)
				order by EventDateTime asc 
			) a
			left join Expl_DataSource_PriorityList pl on pl.Year = Year(a.EventDateTime) 
			and pl.Month = Month(a.EventDateTime) and pl.DataSource_ID = a.DataSource_ID 
			--where TIType = @TIType 
	 union all
			select a.TI_ID, EventDateTime, (case when ManualEnterData is null or ManualEnterData < 0 then Data  / 1000 else ManualEnterData / 1000 end) as Data, 
			 Coeff, cast(DirectChannel as tinyint) as ChannelType, 0, rh.LastData, rh.FirstData, DigitСapacity, ISNULL(pl.[Priority], 0), a.DataSource_ID
			from #tmp usf
			cross apply
			(
				select top 1 a.* from dbo.usf2_ArchCalcChannelInversionStatus(usf.TI_ID, usf.DirectChannel, DateAdd(month, -3, @EventDate1), @EventDate1, case when usf.AIATSCode = 1 then 0 else 1 end) c
				join vw_ArchCalcBitIntegralsVirtual a on a.ChannelType = c.ChannelType and a.EventDateTime between c.StartChannelStatus and c.FinishChannelStatus
				where a.TiType =usf.TIType and a.TI_ID = usf.TI_ID and EventDateTime >= @prevMonthDateTime and a.EventDateTime < @EventDate1
				and  ([Data] >= 0 OR ManualEnterData >= 0)
				order by EventDateTime asc 
			) a
			left join dbo.Info_Meters_TO_TI mt on mt.TI_ID = a.TI_ID and @EventDate1 between StartDateTime and FinishDateTime  and EventDateTime <= StartDateTime
			left join Info_Meters_ReplaceHistory_Channels rh on rh.MetersReplaceSession_ID = mt.MetersReplaceSession_ID and rh.ChannelType = a.ChannelType and rh.FirstData is not null
			left join Hard_Meters hm on hm.Meter_ID = mt.Meter_ID
			left join Expl_DataSource_PriorityList pl on pl.Year = Year(a.EventDateTime) 
			and pl.Month = Month(a.EventDateTime) and pl.DataSource_ID = a.DataSource_ID
			where (a.Data >= 0 OR a.ManualEnterData >= 0) --TIType = @TIType and
	union all
			select usf.TI_ID, EventDateTime, 
			(case when ManualEnterData is null or ManualEnterData < 0 then Data / 1000 else ManualEnterData / 1000 end) as Data, Coeff, cast(DirectChannel as tinyint) as ChannelType, 2,
			rh.LastData, rh.FirstData, DigitСapacity, ISNULL(pl.[Priority], 0), a.DataSource_ID
			from #tmp usf 
			cross apply dbo.usf2_ArchCalcChannelInversionStatus(usf.TI_ID, usf.DirectChannel, @EventDate1, @EventDate1, case when usf.AIATSCode = 1 then 0 else 1 end) c
			left join vw_ArchCalcBitIntegralsVirtual a on a.TiType =usf.TIType and a.TI_ID = usf.TI_ID and a.ChannelType=c.ChannelType and EventDateTime = @EventDate1
			left join dbo.Info_Meters_TO_TI mt on mt.TI_ID = usf.TI_ID and StartDateTime <= @EventDate1 and FinishDateTime >= @EventDate1 and StartDateTime >= EventDateTime
			left join Info_Meters_ReplaceHistory_Channels rh on rh.MetersReplaceSession_ID = mt.MetersReplaceSession_ID and rh.ChannelType = 1 and rh.FirstData is not null
			left join Hard_Meters hm on hm.Meter_ID = mt.Meter_ID
			left join Expl_DataSource_PriorityList pl on pl.Year = Year(a.EventDateTime) 
			and pl.Month = Month(a.EventDateTime) and pl.DataSource_ID = a.DataSource_ID
			where (a.Data >= 0 OR a.ManualEnterData >= 0) --TIType = @TIType and 
			order by EventDateTime asc;



--declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tmp
--  open t;
--	FETCH NEXT FROM t into @TIType
--	WHILE @@FETCH_STATUS = 0
--	BEGIN
--			set @sqlString11 = REPLACE(@sqlString1, '{tableNumber}', ltrim(str(@TIType - 10,2)));
--			set @sqlString22 = REPLACE(@sqlString2, '{tableNumber}', ltrim(str(@TIType - 10,2)));

--			SET @statement = CAST (@sqlString11 AS nvarchar(MAX)) + CAST (@sqlString22 AS nvarchar(MAX))
--			--print @statement;
--			EXEC sp_executesql @statement, @ParmDefinition, @EventDate1, @prevMonthDateTime,@TIType;
--	FETCH NEXT FROM t into @TIType
--	end;
--	CLOSE t
--	DEALLOCATE t			
			
	--Выбор значений с максимальным приоритетом
	select distinct TI_ID, EventDateTime, Data, Coeff, ChannelType, [Param], LastData, FirstData, DigitСapacity
	, (select top 1 DataSourceType from Expl_DataSource_List where DataSource_ID = r.DataSource_ID) as DataSourceTypeByte, [Priority]
	from #resultTable r
	where  [Param] = 2 or --На текущий момент возвращаем все показания которые есть
	([Param]<>2 and [Priority] = (select Max([Priority]) from #resultTable where TI_ID = r.TI_ID and ChannelType = r.ChannelType and [Param] = r.[Param]))
	order by TI_ID, EventDateTime, ChannelType, [Priority] desc

	drop table #tmp			
	drop table #resultTable
	
end
go
   grant EXECUTE on usp2_ArchBit_SelectNearestIntegral to [UserCalcService]
go
   grant EXECUTE on usp2_ArchBit_SelectNearestIntegral to UserExportService
go

grant EXECUTE on usp2_ArchBit_SelectNearestIntegral to UserImportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2011
--
-- Описание:
--
--		Возвращаем последние значения счетчиков по списку точек
--
-- ======================================================================================
create proc [dbo].[usp2_ArchIntegral_ReadArrayLast]
	@TI_Array TiChannelType READONLY, --ТИ + канал
	@isAutoRead bit = 0, --Авточтение
	@dtEnd DateTime = null, --Дата около которой ищем данные
	@isRightFromDate bit = 0 --0 - Ищем ближайшее показание слева от указанной даты, 1 - ищем справа
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

if (@dtEnd is null) set @isRightFromDate = 0;

set @dtEnd = ISNULL(@dtEnd, GetDate());
create table #tmp(
	TI_ID int,
	ChannelType tinyint,
	DirectChannel tinyint,
	TIType tinyint,
	AbsentChannelsMask tinyint
	PRIMARY KEY CLUSTERED ([TIType], [TI_ID], [ChannelType])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
);
create table #result(
	TI_ID int,
	ChannelType tinyint,
	EventDateTime DateTime,
	Data float,
	[Status] int,
	Coeff float
);
insert into #tmp (TI_ID, ChannelType, DirectChannel, TIType, AbsentChannelsMask) select distinct ti.TI_ID, dbo.usf2_ReverseTariffChannel(0, ids.ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, ids.TI_ID, GetDate(), GetDate()) as CHnumber
,ids.ChannelType as DirectChannel, TIType, ISNULL(AbsentChannelsMask,0)
from @TI_Array ids
join Info_TI ti on ids.TI_ID = ti.TI_ID
order by TIType
declare @TIType tinyint;
DECLARE @ParmDefinition NVARCHAR(1000);
DECLARE @SQLString NVARCHAR(4000), @SQLExec NVARCHAR(4000);
SET @ParmDefinition = N'@TIType tinyint, @isAutoRead bit, @dtEnd DateTime'

if (@isRightFromDate = 0) begin --Ищем слева от указанной даты
	SET @SQLString = N'insert into #result
			select TI_ID, ChannelType, EventDateTime, Data, [Status], Coeff from
			(
				select arch.TI_ID, t.DirectChannel as ChannelType, arch.EventDateTime, arch.Data, arch.[Status],
				isnull((select top 1 COEFI*COEFU from Info_Transformators where TI_ID=t.TI_ID and EventDateTime between StartDateTime and ISNULL(FinishDateTime,''21000101'')),1) as Coeff
				from  #tmp t WITH (NOLOCK)
				cross apply
				(
					select top 1 * from {ArchiveTable} WITH (NOLOCK) 
					where TI_ID = t.TI_ID and EventDateTime <= @dtEnd and ChannelType = t.ChannelType and DataSource_ID = 0
					and (@IsAutoRead = 0 OR IntegralType = 0)
					order by EventDateTime desc
				) arch
				where t.TIType = @TIType
			) t'
end else begin -- Ищем справа
	SET @SQLString = N'insert into #result
			select TI_ID, ChannelType, EventDateTime, Data, [Status], Coeff from
			(
				select arch.TI_ID, t.DirectChannel as ChannelType, arch.EventDateTime, arch.Data, arch.[Status],
				isnull((select top 1 COEFI*COEFU from Info_Transformators where TI_ID=t.TI_ID and EventDateTime between StartDateTime and ISNULL(FinishDateTime,''21000101'')),1) as Coeff
				from  #tmp t WITH (NOLOCK)
				cross apply
				(
					select top 1 * from {ArchiveTable} WITH (NOLOCK) 
					where TI_ID = t.TI_ID and EventDateTime >= @dtEnd and ChannelType = t.ChannelType and DataSource_ID = 0
					and (@IsAutoRead = 0 OR IntegralType = 0)
					order by EventDateTime asc
				) arch
				where t.TIType = @TIType
			) t'
end

declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tmp
  open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN
		if (@TIType < 10) begin
			set @SQLExec =REPLACE(@SQLString, '{ArchiveTable}',  'ArchCalc_Integrals_Virtual'); 
		end else begin
			set @SQLExec =REPLACE(@SQLString, '{ArchiveTable}',  'ArchCalcBit_Integrals_Virtual_' + ltrim(str(@TIType - 10,2))); 
		end;
		EXEC sp_executesql @SQLExec, @ParmDefinition,@TIType, @isAutoRead,@dtEnd;
		FETCH NEXT FROM t into @TIType
	end;
	CLOSE t
	DEALLOCATE t
select * from #result
drop table #tmp;
drop table #result;
end
go
   grant EXECUTE on usp2_ArchIntegral_ReadArrayLast to [UserCalcService]
go
   grant EXECUTE on usp2_ArchIntegral_ReadArrayLast to [UserDeclarator]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2019
--
-- Описание:
--
--		Возвращаем последние значения счетчиков по списку точек (новый вариант)
--
-- ======================================================================================
create proc [dbo].[usp2_ArchCalcBitIntegrals_Last]
	@TI_Array TiChannelType READONLY, --ТИ + канал
	@isAutoRead bit = 0, --Авточтение
	@dtEnd DateTime = null, --Дата около которой ищем данные
	@isRightFromDate bit = 0 --0 - Ищем ближайшее показание слева от указанной даты, 1 - ищем справа
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

if (@dtEnd is null) set @isRightFromDate = 0;

set @dtEnd = ISNULL(@dtEnd, GetDate());

if (@isRightFromDate = 0) 
begin --Ищем слева от указанной даты

	select distinct t.TI_ID, t.ChannelType, m.EventDateTime
	,[Data], [Status] 
	,isnull((select top 1 COEFI*COEFU from Info_Transformators where TI_ID=t.TI_ID and m.EventDateTime >= StartDateTime and (FinishDateTime is null or m.EventDateTime < FinishDateTime)),1) as Coeff
	from @TI_Array t
	cross apply
	(
		select top 1 TiType, i.IsInverted, (case ISNULL(i.IsInverted, (case when ti.AIATSCode = 2 then 1 else 0 end))  
					when 0 then t.ChannelType
					else (t.ChannelType - t.ChannelType % 10) +
						(
							case t.ChannelType % 10
							when 1 then 2
							when 2 then 1
							when 3 then 4
							when 4 then 3
							end
						)
				end) as ChannelType  from Info_TI ti 
		outer apply
		(
			select top 1 IsInverted from [dbo].[ArchCalc_Channel_InversionStatus] i 
			where i.TI_ID = ti.TI_ID and @dtEnd >= i.StartDateTime and (i.FinishDateTime is null or i.FinishDateTime >= @dtEnd)
		) i
		where t.TI_ID = ti.TI_ID
	) ti
	cross apply
	(
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalc_Integrals_Virtual a
		where ti.TiType <= 10 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_1 a
		where ti.TiType = 11 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_2 a
		where ti.TiType = 12 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_3 a
		where ti.TiType = 13 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_4 a
		where ti.TiType = 14 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_5 a
		where ti.TiType = 15 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
		
		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_6 a
		where ti.TiType = 16 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
		
		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_7 a
		where ti.TiType = 17 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
				
		union all
		
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_8 a
		where ti.TiType = 18 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
		
		union all
		
		select top 1 EventDateTime, [Data], [Status] from
		ArchCalcBit_Integrals_Virtual_9 a
		where ti.TiType = 19 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска справа
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
		
		union all
		
		select top 1 EventDateTime, [Data], [Status] from -- <-- отличие от поиска справа 
		ArchCalcBit_Integrals_Virtual_10 a
		where ti.TiType = 20 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all
		
		select top 1 EventDateTime, [Data], [Status] from -- <-- отличие от поиска справа 
		ArchCalcBit_Integrals_Virtual_11 a
		where ti.TiType = 21 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа

		union all
		
		select top 1 EventDateTime, [Data], [Status] from -- <-- отличие от поиска справа 
		ArchCalcBit_Integrals_Virtual_12 a
		where ti.TiType = 22 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
		
		union all
		
		select top 1 EventDateTime, [Data], [Status] from -- <-- отличие от поиска справа 
		ArchCalcBit_Integrals_Virtual_13 a
		where ti.TiType = 23 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
		
		union all
		
		select top 1 EventDateTime, [Data], [Status] from -- <-- отличие от поиска справа 
		ArchCalcBit_Integrals_Virtual_14 a
		where ti.TiType = 24 and a.TI_ID = t.TI_ID
		and EventDateTime <= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime desc -- <-- отличие от поиска справа
	) m
	--where ti.TiType = 15
	order by t.TI_ID, t.ChannelType
end else begin -- Ищем справа
	select distinct t.TI_ID, t.ChannelType, m.EventDateTime
	,[Data], [Status] 
	,isnull((select top 1 COEFI*COEFU from Info_Transformators where TI_ID=t.TI_ID and m.EventDateTime >= StartDateTime and (FinishDateTime is null or m.EventDateTime < FinishDateTime)),1) as Coeff
	from @TI_Array t
	cross apply
	(
		select top 1 TiType, i.IsInverted, (case ISNULL(i.IsInverted, (case when ti.AIATSCode = 2 then 1 else 0 end))  
					when 0 then t.ChannelType
					else (t.ChannelType - t.ChannelType % 10) +
						(
							case t.ChannelType % 10
							when 1 then 2
							when 2 then 1
							when 3 then 4
							when 4 then 3
							end
						)
				end) as ChannelType  from Info_TI ti 
		outer apply
		(
			select top 1 IsInverted from [dbo].[ArchCalc_Channel_InversionStatus] i 
			where i.TI_ID = ti.TI_ID and @dtEnd >= i.StartDateTime and (i.FinishDateTime is null or i.FinishDateTime >= @dtEnd)
		) i
		where t.TI_ID = ti.TI_ID
	) ti
	cross apply
	(
		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalc_Integrals_Virtual a
		where ti.TiType <= 10 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева

		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_1 a
		where ti.TiType = 11 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева

		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_2 a
		where ti.TiType = 12 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_3 a
		where ti.TiType = 13 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_4 a
		where ti.TiType = 14 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_5 a
		where ti.TiType = 15 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_6 a
		where ti.TiType = 16 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_7 a
		where ti.TiType = 17 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_8 a
		where ti.TiType = 18 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_9 a
		where ti.TiType = 19 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_10 a
		where ti.TiType = 20 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_11 a
		where ti.TiType = 21 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_12 a
		where ti.TiType = 22 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_13 a
		where ti.TiType = 23 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
		
		union all

		select top 1 EventDateTime, [Data], [Status] from 
		ArchCalcBit_Integrals_Virtual_14 a
		where ti.TiType = 24 and a.TI_ID = t.TI_ID
		and EventDateTime >= @dtEnd -- <-- отличие от поиска слева
		and (@IsAutoRead = 0 OR a.IntegralType = 0)
		and a.ChannelType = ti.ChannelType
		order by EventDateTime asc -- <-- отличие от поиска слева
	) m
	order by t.TI_ID, t.ChannelType
end
end

go
   grant EXECUTE on usp2_ArchCalcBitIntegrals_Last to [UserCalcService]
go
   grant EXECUTE on usp2_ArchCalcBitIntegrals_Last to [UserDeclarator]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выбирает информацию по точкам замещенным по акту недоучета
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Replace_ActUndercounts]

	@TI_Array TiChannelType READONLY,
	@DateStart datetime,
	@DateEnd datetime
	

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--select * from usf2_Utils_iterCA_intlist_to_table(@TI_Array)

select arch.* from dbo.ArchCalc_Replace_ActUndercount arch
inner join @TI_Array usf
on usf.TI_ID = arch.TI_ID and usf.ChannelType = arch.ChannelType
where arch.StartDateTime <= @DateEnd and arch.FinishDateTime >= @DateStart
	and (IsInactive is null or IsInactive = 0)

end
go
   grant EXECUTE on usp2_ArchComm_Replace_ActUndercounts to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2019
--
-- Описание:
--
--		Выбирает значения барабанов (более новый вариант)
--
-- ======================================================================================
create proc [dbo].[usp2_ArchCalcBitIntegrals]

	@TI_Array dbo.TiChannelType READONLY,
	@dateStart datetime,
	@dateEnd datetime,
	@isAutoRead bit,
	@isFindBack bit,
	@UseLossesCoefficient bit = 0 --Использовать ли коэфф. потерь для ТИ

as
declare
@TI_ID int,
@PS_ID int,
@ChannelType tinyint,
@titype tinyint,
@IsCoeffTransformationDisabled bit,
@TypeArchTable tinyint,
@closedPeriod_ID uniqueidentifier,
@tp_id int,
@DataSourceType tinyint

begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@IsChannelsInverted bit,
@AbsentChannelsMask tinyint,
@IsAbsentChannel bit

declare @backStart DateTime;

if (@isFindBack = 1) set @backStart = DateAdd(hh, -2, @DateStart);
else set @backStart = @DateStart;

select * 
into #tis 
from 
(
	select distinct usf.TI_ID, ChannelType, usf.ClosedPeriod_ID, usf.DataSourceType, usf.TP_ID, 
	IsCoeffTransformationDisabled, titype, ISNULL(AbsentChannelsMask, 0) as AbsentChannelsMask, PS_ID,
	case when AIATSCode = 2 then 1 else 0 end as IsChannelsInverted
	from @TI_Array usf
	join Info_TI ti on ti.TI_ID = usf.TI_ID
) usf
order by usf.TI_ID, usf.ChannelType, usf.DataSourceType
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Выбираем смену счетчиков
select usf.* 
into #replaceHistory from 
(select distinct TI_ID from #tis) t
cross apply dbo.usf2_Utils_Monit_Exchanges_Meters_TO(t.TI_ID, @DateStart, @DateEnd) usf

--1 результат (Данные по времени действия счетчика)
select * from #replaceHistory r order by TI_ID, StartDateTime;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Таблица с коэфф. потерь для ТИ
if (@UseLossesCoefficient = 1) begin
	SELECT c.[TI_ID],c.[StartDateTime],c.[FinishDateTime],c.[LossesCoefficient]
	FROM Info_TI_LossesCoefficients c
	where c.TI_ID in (select distinct TI_ID from #tis) and 
	c.StartDateTime <= @DateEnd and (c.FinishDateTime is null or (c.FinishDateTime is not null and c.FinishDateTime>= @DateStart))
	order by TI_ID, StartDateTime
end

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 2 результат (Данные по последним данным каналов при смене счетчика)
select rh.TI_ID, cast(c.ChannelType as tinyint) as ChannelType,
 h.LastData, h.FirstData, h.MetersReplaceSession_ID, rh.DigitСapacity
from #tis ti 
cross apply dbo.usf2_ArchCalcChannelInversionStatus(ti.TI_ID, ti.ChannelType, @DateStart, @DateEnd, ti.IsChannelsInverted) c 
join #replaceHistory rh  on rh.TI_ID = ti.TI_ID and rh.StartDateTime <= c.FinishChannelStatus and (rh.FinishDateTime is null or rh.FinishDateTime >= c.StartChannelStatus)
join dbo.Info_Meters_ReplaceHistory_Channels h on h.MetersReplaceSession_ID = rh.MetersReplaceSession_ID and h.ChannelType=c.ChannelType
order by rh.TI_ID, h.MetersReplaceSession_ID 

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 3 результат (Данные по времени действия коэффициента трансформации)
select TI_ID, cast(COEFU*COEFI as float(26)) as Coeff, 
case when StartDateTime < @DateStart then @DateStart else StartDateTime end as StartDateTime,
case when FinishDateTime > @DateEnd then @DateEnd else ISNULL(FinishDateTime, '21000101') end as FinishDateTime
from dbo.Info_Transformators  it
where it.TI_ID in (select distinct TI_ID from #tis) and StartDateTime <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
order by StartDateTime

--4 Выборка периодов, когда коэфф. трансформации был заблокирован
select distinct ti_id, IsCoeffTransformationDisabled, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, null as ClosedPeriod_ID 
from dbo.ArchCalc_Integrals_CoeffTransformation_DisabledStatus where ti_id in (select distinct ti_id from #tis where ClosedPeriod_ID is null)
and dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
order by ti_id, ClosedPeriod_ID, StartDateTime

--5 результат (Параметры ПУ)
select TI_ID, ChannelType, ISNULL(closedPeriod_ID, NULL) as ClosedPeriod_ID, TP_ID, 
DataSourceType, IsCoeffTransformationDisabled, titype, AbsentChannelsMask, PS_ID, IsChannelsInverted from #tis t

--6 результат (архивные значения)
select a.TI_ID, t.ChannelType, t.DataSourceType  as RequestedDataSourceType, dsl.DataSourceType, a.[EventDateTime],
	a.[Data],a.ManualEnterData,a.[IntegralType],a.[DispatchDateTime],a.[Status], 
	ISNULL(pl.[Priority],0) as [Priority],
	case when t.DataSourceType is null AND titp.TI_ID is not null then 1 else 0 end as IsManulaySetPriority, a.[IsUsedForFillHalfHours]

from #tis t 
join dbo.vw_ArchCalcBitIntegralsVirtual a on a.TiType = t.TIType and a.TI_ID = t.TI_ID 
outer apply
(
	select top 1 IsInverted from [dbo].[ArchCalc_Channel_InversionStatus] i 
	where i.TI_ID = t.TI_ID and a.EventDateTime >= i.StartDateTime and (i.FinishDateTime is null or i.FinishDateTime >= a.EventDateTime)
) i

left join [Expl_DataSource_List] dsl on a.DataSource_ID = dsl.DataSource_ID 
left join [dbo].[Expl_DataSource_To_TI_TP] titp on t.DataSourceType is null and titp.TI_ID = a.TI_ID and titp.TP_ID = t.tp_id and titp.DataSource_ID = a.DataSource_ID and titp.[Month] = Month(a.EventDateTime) and titp.[Year] = Year(a.EventDateTime) 

outer apply
(
	select pl.[Priority]
	from [Expl_DataSource_PriorityList] pl 
	where t.DataSourceType is null and pl.[Year] = Year(a.EventDateTime) and pl.[Month] = Month(a.EventDateTime) and pl.DataSource_ID = a.DataSource_ID
) pl

where EventDateTime between @backStart and @dateEnd and (@isAutoRead = 0 or a.IntegralType = 0) and ([Data] >= 0 OR ManualEnterData >= 0)
	and (t.DataSourceType is null or a.DataSource_ID = (select top 1 DataSource_ID from Expl_DataSource_List where DataSourceType = t.DataSourceType))

--Обрабатываем инверитирование канала
and (a.ChannelType = 
	(
			case ISNULL(i.IsInverted, t.IsChannelsInverted)  
				when 0 then t.ChannelType
				else t.ChannelType - t.ChannelType % 10 +
					(
						case t.ChannelType % 10
						when 1 then 2
						when 2 then 1
						when 3 then 4
						when 4 then 3
						end
					)
			end
	)) 
order by a.TI_ID, t.ChannelType, t.DataSourceType, a.[EventDateTime], [Priority] desc 

drop table #tis;

end

go
   grant EXECUTE on usp2_ArchCalcBitIntegrals to [UserCalcService]
go