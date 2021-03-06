--Эти процедуры помещены вместе так как используют общий тип входящего параметра
--Пересоздание этого типа параметра возможно только после удаления этих процедур
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetSection')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetSection
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_ClosedPeriodbySection')
          and type in ('P','PC'))
   drop procedure usp2_Expl_ClosedPeriodbySection
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_SectionByTP')
          and type in ('P','PC'))
   drop procedure usp2_Expl_SectionByTP
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_ClosePeriod')
          and type in ('P','PC'))
   drop procedure usp2_Expl_ClosePeriod
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_OpenPeriod')
          and type in ('P','PC'))
   drop procedure usp2_Expl_OpenPeriod
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetTPParams')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetTPParams
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetPointsByDirectConsumerAndSection')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetPointsByDirectConsumerAndSection
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_DeltaFromOpenData')
          and type in ('P','PC'))
   drop procedure usp2_Expl_DeltaFromOpenData
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetDictionaryOfNames')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetDictionaryOfNames
go


if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetDictionaryOfNames')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetDictionaryOfNames
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_GetMaxPowerByObject')
          and type in ('P','PC'))
   drop procedure usp2_GetMaxPowerByObject
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_DirectConsumerByTP')
          and type in ('P','PC'))
   drop procedure usp2_Expl_DirectConsumerByTP
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_GroupTP_ReadDeltaFromOpenDataTp')
          and type in ('P','PC'))
   drop procedure usp2_GroupTP_ReadDeltaFromOpenDataTp
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetSectionForDirectConsumer')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetSectionForDirectConsumer
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Journals_E422_USPD')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Journals_E422_USPD
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchComm_Journals_E422_USPD')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchComm_Journals_E422_USPD
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Journals_Tis')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Journals_Tis
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchComm_Journals_Tis')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchComm_Journals_Tis
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_GlobalJournals')
          and type in ('V'))
   drop view vw_GlobalJournals
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_GlobalJournalTi')
          and type in ('V'))
   drop view vw_GlobalJournalTi
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_GlobalJournalE422_USPD')
          and type in ('V'))
   drop view vw_GlobalJournalE422_USPD
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Journals_Global')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Journals_Global
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Alarm_LinkWithObjects')
          and type in ('P','PC'))
   drop procedure usp2_Alarm_LinkWithObjects
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_JournalOvEventTypeInterpretation')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_JournalOvEventTypeInterpretation
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ServersGlobalJournalCodeInterpretation')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ServersGlobalJournalCodeInterpretation
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ServersGlobalJournalSourceInterpretation')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ServersGlobalJournalSourceInterpretation
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_JournalApplicationTypeInterpretation')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_JournalApplicationTypeInterpretation
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_JournalEventTypeInterpretation')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_JournalEventTypeInterpretation
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchy_GetPath')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchy_GetPath
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchy_UpdateFreeHierCach')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchy_UpdateFreeHierCach
go

--Обновляем тип
--Удаляем если есть
IF EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'RequestSectionParamsType' AND ss.name = N'dbo') 
drop type RequestSectionParamsType
go

-- Пересоздаем заново
CREATE TYPE RequestSectionParamsType AS TABLE 
(
	ID int NOT NULL,
	TypeHierarchy tinyint NOT NULL,
	ClosedPeriod_ID uniqueidentifier NULL,
	IsCustomerCoordinated bit NULL,
	SecondaryID int NOT NULL,
	StringId varchar(22) NULL,
	FreeHierItemId int NULL
)

grant EXECUTE on TYPE::RequestSectionParamsType to [UserCalcService]
grant EXECUTE on TYPE::RequestSectionParamsType to [UserSlave61968Service]
grant EXECUTE on TYPE::RequestSectionParamsType to [UserDeclarator]
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
--		Декабрь, 2008
--
-- Описание:
--
--		Выбираем ТП с параметрами входящие в сечения
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetSection]
(	
	@RequestSectionParams RequestSectionParamsType READONLY, --Сечения или контракты
	@datestart datetime = null,
	@dateend datetime = null,
	@IsOurSidebyBusRelation bit = 1, --Относительно какой стороны берем информацию (false(0) - относительно контрольной(нерасчетной))
	@MoneyOurSideMode tinyint = 0,
	@IsAllocateCA bit = 0, -- 
	@IsReturnATSCodes bit = 0,
	@ToIndexes bit = 1, --Получаем индексы смещения для диапазонов вхождения ТП в сечения, или читаем дату время напрямую
	@isCalculateFactPowerByTPVoltageLevel bit = 0,
	@factPowerCalculateMode tinyint = 0, -- Режим формирования данных: 0 - Идентификаторы закрытий явно указаны, 1 - Только закрытые данные (если есть, если нет то вернем открытые)
	@priceCategory IntType READONLY,
	@startMonthYear DateTime = null, --Расчетный период для фильтра по ценовым категориям
	@finishMonthYear DateTime = null --Расчетный период для фильтра по ценовым категориям
)
AS
BEGIN 
			set nocount on
			set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
			set numeric_roundabort off
			set transaction isolation level read uncommitted

			--Нужно ли фильтровать по ценовым категориям
			declare @isPriceCategoryFiltered bit;

			if (exists (select top 1 1 from @priceCategory)) set @isPriceCategoryFiltered = 1;
			else set @isPriceCategoryFiltered = 0;

			if (@factPowerCalculateMode > 3 or @factPowerCalculateMode = 2) set @factPowerCalculateMode = 0; --Пока можно работать только с указанными режимами

			declare 
			@dtStart DateTime, @dtEnd DateTime;
			select @dtStart=ISNULL(@datestart, '20100101'), @dtEnd = ISNULL(@dateend, '21000101')
--------------Формируем список ТП по которым запрашиваем параметры
			create table #tps
			(
			  Section_ID int not null,
			  TP_ID int not null,
			  ClosedPeriod_ID uniqueidentifier null,
			  [TimeZoneId] nvarchar(255)
			  --PRIMARY KEY CLUSTERED (section_id, tp_id) Проверить фильтр по ценовым категориям !!!
			)

			--Выбор по родителю
			--Запрос сечений или контракты из закрытого периода
			if (@factPowerCalculateMode = 0) begin
				--Объекты возвращаем как есть, ищем закрытия, если они указаны явно
				insert into #tps
				select distinct s.ID as Section_ID, isd.TP_ID, s.ClosedPeriod_ID, z.MsTimeZoneId as TimeZoneId from 
				(
					select distinct ID, ClosedPeriod_ID from @RequestSectionParams
					where ClosedPeriod_ID is not null and TypeHierarchy = 5
					union 
					select distinct Section_ID, ClosedPeriod_ID from @RequestSectionParams usf
					join Info_Section_To_JuridicalContract sjc on usf.ID = sjc.JuridicalPersonContract_ID
					where ClosedPeriod_ID is not null and TypeHierarchy = 20
					union
					select distinct Section_ID, ClosedPeriod_ID from @RequestSectionParams usf
					join Dict_JuridicalPersons_Contracts jpc on jpc.JuridicalPerson_ID = usf.ID
					join Info_Section_To_JuridicalContract sjc on sjc.JuridicalPersonContract_ID = jpc.JuridicalPersonContract_ID
					where ClosedPeriod_ID is not null and TypeHierarchy = 19
				) s
				join [dbo].[Info_Section_Description_Closed] isd on isd.Section_ID = ID and isd.ClosedPeriod_ID = s.ClosedPeriod_ID
				join [dbo].[Info_Section_List] sec on sec.Section_ID = s.ID
				left join [dbo].[Dict_TimeZone_Zones] z on z.TimeZone_ID = sec.TimeZone_ID
				--Запрос из открытого периода
				union all
				select distinct s.ID as Section_ID, isd.TP_ID, null as ClosedPeriod_ID, z.MsTimeZoneId as TimeZoneId from 
				(
					select distinct ID from @RequestSectionParams
					where ClosedPeriod_ID is null and TypeHierarchy = 5
					union 
					select distinct Section_ID from @RequestSectionParams usf
					join Info_Section_To_JuridicalContract sjc on usf.ID = sjc.JuridicalPersonContract_ID
					where ClosedPeriod_ID is null and TypeHierarchy = 20
					union
					select distinct Section_ID from @RequestSectionParams usf
					join Dict_JuridicalPersons_Contracts jpc on jpc.JuridicalPerson_ID = usf.ID
					join Info_Section_To_JuridicalContract sjc on sjc.JuridicalPersonContract_ID = jpc.JuridicalPersonContract_ID
					where ClosedPeriod_ID is null and TypeHierarchy = 19
				) s
				join [dbo].[Info_Section_Description2] isd on isd.Section_ID = s.ID
				join [dbo].[Info_Section_List] sec on sec.Section_ID = s.ID
				left join [dbo].[Dict_TimeZone_Zones] z on z.TimeZone_ID = sec.TimeZone_ID
				where isd.StartDateTime <= @dtEnd and ISNULL(isd.FinishDateTime, '21000101') >= @dtStart

				--select * from #tps where tp_id = 76

			end else begin
				
				declare @month tinyint, @year int;
				set @month = Month(@startMonthYear);
				set @year = YEAR(@startMonthYear);

				--Сначала ищем в первых попавшихся закрытиях
				insert into #tps
				select distinct s.Section_ID, isd.TP_ID, cts.ClosedPeriod_ID, z.MsTimeZoneId as TimeZoneId from 
				(
					select distinct ID as Section_ID from @RequestSectionParams
					where TypeHierarchy = 5
					union 
					select distinct Section_ID from @RequestSectionParams usf
					join Info_Section_To_JuridicalContract sjc on usf.ID = sjc.JuridicalPersonContract_ID
					where TypeHierarchy = 20
					union
					select distinct Section_ID from @RequestSectionParams usf
					join Dict_JuridicalPersons_Contracts jpc on jpc.JuridicalPerson_ID = usf.ID
					join Info_Section_To_JuridicalContract sjc on sjc.JuridicalPersonContract_ID = jpc.JuridicalPersonContract_ID
					where TypeHierarchy = 19

				) s
				join [dbo].[Expl_ClosedPeriod_To_Section] cts on cts.Section_ID = s.Section_ID
					and ClosedPeriod_ID in (select ClosedPeriod_ID from [dbo].[Expl_ClosedPeriod_List] where [Year] = @year and [Month] = @month)
				join [dbo].[Info_Section_Description_Closed] isd on isd.Section_ID = s.Section_ID and isd.ClosedPeriod_ID = cts.ClosedPeriod_ID
				join [dbo].[Info_Section_List] sec on sec.Section_ID = s.Section_ID
				left join [dbo].[Dict_TimeZone_Zones] z on z.TimeZone_ID = sec.TimeZone_ID
				where isd.StartDateTime <= @dtEnd and ISNULL(isd.FinishDateTime, '21000101') >= @dtStart

				--Теперь добавляем то, что не добавили (что не закрыто)
				if (@factPowerCalculateMode = 3) begin
					insert into #tps
					select distinct s.Section_ID, isd.TP_ID, null as ClosedPeriod_ID, z.MsTimeZoneId as TimeZoneId from
					(
						select distinct ID as Section_ID from @RequestSectionParams
						where TypeHierarchy = 5
						union 
						select distinct Section_ID from @RequestSectionParams usf
						join Info_Section_To_JuridicalContract sjc on usf.ID = sjc.JuridicalPersonContract_ID
						where TypeHierarchy = 20
						union
						select distinct Section_ID from @RequestSectionParams usf
						join Dict_JuridicalPersons_Contracts jpc on jpc.JuridicalPerson_ID = usf.ID
						join Info_Section_To_JuridicalContract sjc on sjc.JuridicalPersonContract_ID = jpc.JuridicalPersonContract_ID
						where TypeHierarchy = 19
					) s
					join [dbo].[Info_Section_Description2] isd on isd.Section_ID = s.Section_ID
					join [dbo].[Info_Section_List] sec on sec.Section_ID = s.Section_ID
					left join [dbo].[Dict_TimeZone_Zones] z on z.TimeZone_ID = sec.TimeZone_ID
					where s.Section_ID not in (select distinct #tps.Section_ID from #tps)
						and isd.StartDateTime <= @dtEnd and ISNULL(isd.FinishDateTime, '21000101') >= @dtStart

				end
			end

			--Если нужно отфильтровать по ценовым категориям, удаляем то, что не соответствует условиям фильтра
			if (@isPriceCategoryFiltered = 1 and @startMonthYear is not null) begin
				delete from #tps
				where Section_ID not in 
				(
					select distinct Section_ID from --Сечения, которые входят в условие фильтра
					(
						select distinct #tps.Section_ID, ISNULL(PriceCategory_ID, -1) as PriceCategory_ID from #tps
						left join [dbo].[Dict_PriceCategory_To_Section] pts on pts.Section_ID = #tps.Section_ID
						and @startMonthYear <= ISNULL(FinishMonthYear, '21000101')
						and 
							(
								(@finishMonthYear is null and @startMonthYear >= StartMonthYear)
								or
								(@finishMonthYear is not null and @finishMonthYear >= StartMonthYear)
							)
					) p
					where PriceCategory_ID in (select Id from @priceCategory)
				) 
			end;

---------------Выбираем ТИ ФСК для которых считаем по коэффициентам ТИ

			--Параметры ТП
			SELECT isd.Section_ID AS [Section_ID], 
			IsMoneyOurSide,
			tf.IsCA,
			tf.EvalModeOurSide,
			tf.EvalModeContr,
			tf.PS_ID,
			isd.TP_ID,
			tf.PSProperty,
			tf.VoltageLevel, 
			tf.Voltage,
			tf.PSVoltage,
			tf.[MeterSerialNumber],
			tf.StringName as TPName,
			tf.PSName,
			tf.DirectConsumer_ID,
			dco.StringName as DirectConsumerName,
			cast(case when tf.TPMode = 4 or tf.TPMode = 5 then 1 else 0 end as bit) as IsTransit,
			tf.TPMode,
			sjc.JuridicalPersonContract_ID,
			jc.JuridicalPerson_ID,
			isd.ClosedPeriod_ID,
			--'' as RangeInSection
			dbo.usf2_Info_GetRangeTpInSection(isd.Section_ID, isd.TP_ID, isd.ClosedPeriod_ID, @dtStart, @dtEnd, @ToIndexes) as RangeInSection,
			dbo.usf2_Info_GetRangeHoursVoltageLevelInTp(isd.TP_ID, @dtStart, @dtEnd, isd.ClosedPeriod_ID) as RangeHoursVoltageLevel,
			isd.TimeZoneId,
			tf.IsCoeffTransformationDisabled
			FROM #tps isd
			--Параметры ТП в балансе
			outer apply usf2_Info_GetTPParams(isd.TP_ID, null,@dtStart,@dtEnd,@IsOurSidebyBusRelation,@MoneyOurSideMode, isd.ClosedPeriod_ID) tf
			left join [dbo].[Dict_DirectConsumer] dco on dco.DirectConsumer_ID = tf.DirectConsumer_ID
			left join Info_Section_To_JuridicalContract sjc on isd.Section_ID = sjc.Section_ID
			left join Dict_JuridicalPersons_Contracts jc on sjc.JuridicalPersonContract_ID = jc.JuridicalPersonContract_ID
			order by JuridicalPerson_ID, JuridicalPersonContract_ID,Section_ID, ClosedPeriod_ID,IsCA asc,PSVoltage desc, PSName asc,[Voltage] desc, [TPName] asc

			if (@IsAllocateCA = 1) begin
				--Таблица соотношений прямых потребителей с сечениями в которые они входят
				select distinct tp.DirectConsumer_ID, sd.Section_ID, 
				ISNULL(case 
					when sl.HierLev1_ID is not null then sl.HierLev1_ID 
					when sl.HierLev2_ID is not null then (select HierLev1_ID from Dict_HierLev2 where HierLev2_ID = sl.HierLev2_ID)
					when sl.HierLev3_ID is not null then (select HierLev1_ID from Dict_HierLev2 where HierLev2_ID = (select HierLev2_ID from Dict_HierLev3 where HierLev3_ID = sl.HierLev3_ID))
				end, 0 )as HierLev1_ID
					from #tps sd 
					join dbo.Info_TP2 tp on tp.TP_ID = sd.TP_ID
					join dbo.Info_Section_List sl
					on sl.Section_ID = sd.Section_ID
				where tp.DirectConsumer_ID is not null
			end;

			if (@IsReturnATSCodes = 1) begin
				select Section_Id, ATSSectionCode, ATSSubjORECOde, SubjORE_ID, IsSaldoCalculatedDirectly
				from Info_Section_List where Section_ID in ((select distinct Section_ID from #tps))
			end;

			drop table #tps
end
go
   grant EXECUTE on usp2_Info_GetSection to [UserCalcService]
go
   grant EXECUTE on usp2_Info_GetSection to [UserSlave61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2013
--
-- Описание:
--
--		Читаем закрытия на отчетный период по списку сечений
--
-- ======================================================================================

create proc [dbo].[usp2_Expl_ClosedPeriodbySection]
(	
	@RequestSectionParams RequestSectionParamsType READONLY, --Сечения или контракты
	@startMonthYear datetime = null,
	@isBySection bit = 1
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	if (@isBySection = 1) begin
		if (@startMonthYear is not null) begin
			declare @year int, @month tinyint;
			set @year = Year(@startMonthYear); set @month = Month(@startMonthYear)
			--Если набирать закрытые периоды по сечениям
			select IsCustomerCoordinated, Section_ID, dateadd(mm,([Year]-1900)* 12 + [Month] - 1,0) as StartMonthYear, cl.ClosedPeriod_ID, cl.User_ID, u.UserName, cts.ClosedPeriodType, cl.StringName, cl.DispatchDateTime  from [dbo].[Expl_ClosedPeriod_List] cl
			join [dbo].[Expl_ClosedPeriod_To_Section] cts on cts.ClosedPeriod_ID = cl.ClosedPeriod_ID
			join Expl_Users u on u.User_ID = cl.User_ID
			where cl.[Year] = @year and cl.[Month] = @month and cts.Section_ID in (select distinct ID from @RequestSectionParams)
			and ClosedPeriodType = 0 --Только окончательные закрытия
		end else begin
			--Последние закрытия
			select IsCustomerCoordinated, Section_ID, dateadd(mm,([Year]-1900)* 12 + [Month] - 1,0) as StartMonthYear, cl.ClosedPeriod_ID, cl.User_ID, u.UserName, cts.ClosedPeriodType, cl.StringName, cl.DispatchDateTime  
			from [dbo].[Expl_ClosedPeriod_To_Section] cts
			cross apply
			(
			  select top 1 * from [dbo].[Expl_ClosedPeriod_List]
			  where ClosedPeriod_ID = cts.ClosedPeriod_ID
			  order by [Year] desc, [Month] desc
			) cl 
			join Expl_Users u on u.User_ID = cl.User_ID
			where cts.Section_ID in (select distinct ID from @RequestSectionParams)
			and ClosedPeriodType = 0 --Только окончательные закрытия
		end 
	end else begin
		declare @isGetAllClosed bit;
		set @isGetAllClosed = case when exists(select top 1 1 from @RequestSectionParams) then 0 else 1 end;

		--Набираем закрытые периоды по их идентификаторам
		select IsCustomerCoordinated, cl.Section_ID, dateadd(mm,(c.[Year]-1900)* 12 + c.[Month] - 1,0) as StartMonthYear, 
		c.ClosedPeriod_ID, c.User_ID, u.UserName, c.ClosedPeriodType, c.StringName, c.DispatchDateTime  
		from 
		(
			select distinct Section_ID from [dbo].[Expl_ClosedPeriod_List] cl 
			join [dbo].[Expl_ClosedPeriod_To_Section] cts on cts.ClosedPeriod_ID = cl.ClosedPeriod_ID
			where dateadd(mm,(cl.[Year]-1900)* 12 + cl.[Month] - 1,0) >= @startMonthYear
			and (@isGetAllClosed = 1 or Section_ID in (select distinct ID from @RequestSectionParams))
		) cl
		cross apply
				(
				  select top 1 c.*, cts.Section_ID, cts.IsCustomerCoordinated, cts.ClosedPeriodType from [dbo].[Expl_ClosedPeriod_List] c
				  join [dbo].[Expl_ClosedPeriod_To_Section] cts on cts.ClosedPeriod_ID = c.ClosedPeriod_ID
				  where Section_ID = cl.Section_ID
				  and dateadd(mm,([Year]-1900)* 12 + [Month] - 1,0) >= @startMonthYear
				  order by [Year] desc, [Month] desc
				) c 
		join Expl_Users u on u.User_ID = c.User_ID
	end;

END
go
   grant EXECUTE on usp2_Expl_ClosedPeriodbySection to [UserCalcService]
go
grant EXECUTE on usp2_Expl_ClosedPeriodbySection to [UserSlave61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2013
--
-- Описание:
--
--		Читаем список сечений для ТП в открытом и закрытом периоде
--
-- ======================================================================================

create proc [dbo].[usp2_Expl_SectionByTP]
(	
	@RequestSectionParams RequestSectionParamsType READONLY, --Точки поставок с закрытими периодами
	@startMonthYear datetime
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	declare
	@dtEnd DateTime;

	set @dtEnd = DATEADD(month, 1, @startMonthYear);

	select distinct Section_ID, null as ClosedPeriod_ID, TP_ID  from Info_Section_Description2
	where TP_ID in (select ID from @RequestSectionParams where ClosedPeriod_ID is null) and StartDateTime < @dtEnd and ISNULL(FinishDateTime, '21000101') >= @startMonthYear
	union 
	select distinct Section_ID, s.ClosedPeriod_ID, TP_ID from Info_Section_Description_Closed s
	join @RequestSectionParams p on p.ID = s.TP_ID and p.ClosedPeriod_ID = s.ClosedPeriod_ID and StartDateTime < @dtEnd and ISNULL(FinishDateTime, '21000101') >= @startMonthYear

END
go
   grant EXECUTE on usp2_Expl_SectionByTP to [UserCalcService]
go
grant EXECUTE on usp2_Expl_SectionByTP to [UserSlave61968Service]
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
--		Закрываем расчетный период, умеем закрывать по сечению и контракту.
--		В таблицу пишем только закрытие по сечению
--		Если в таблице закрытия сечения есть уже окончательно закрытое сечение, то больше его не закрываем
--		Возвращаем список сечений которые до этого были окончательно закрыты. Их не перезакрываем.
--
-- ======================================================================================
create proc [dbo].[usp2_Expl_ClosePeriod]
	@requestSectionParams RequestSectionParamsType READONLY, --Сечения или контракты
	@startMonthYear DateTime, -- Расчетный период
	@User_ID varchar(22),
	@CUS_ID tinyint,
	@closedPeriodType tinyint = 0, --Тип закрытия
	@dtStartServer DateTime,
	@dtEndServer DateTime,
	@StringName varchar(255)
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Формируем список сечений для копирования
create table #sections
(
	Section_ID int not null,
	IsCustomerCoordinated bit null
)

insert into #sections 
select distinct ID, IsCustomerCoordinated from @requestSectionParams where TypeHierarchy = 5
union 
select distinct Section_ID, IsCustomerCoordinated from @RequestSectionParams usf
join Info_Section_To_JuridicalContract sjc on usf.ID = sjc.JuridicalPersonContract_ID
where TypeHierarchy = 20

if (@closedPeriodType = 0) begin
	--Возвращаем обратно сечения которые уже закрыты, перезакрывать можно только с перерасчетом
	select Section_ID 
	into #alreaderClosedSections
	from Expl_ClosedPeriod_To_Section cts
	join Expl_ClosedPeriod_List l on l.ClosedPeriod_ID = cts.ClosedPeriod_ID
	where l.Year = Year(@startMonthYear) and l.Month = Month(@startMonthYear) and ClosedPeriodType = 0
	and cts.Section_ID in (select Section_ID from #sections)

	declare @closedSectionNames varchar(max);
	set @closedSectionNames = '';
	select @closedSectionNames += sl.SectionName + ', '
	from #alreaderClosedSections cs
	join Info_Section_List sl on cs.Section_ID = sl.Section_ID;

	if LEN(@closedSectionNames) > 0 select @closedSectionNames + ' - уже закрыты.\nНеобходимо делать перерасчет!';

	--Удаляем уже закрытые сечения из нашего списка 
	delete from #sections
	where Section_ID in (select Section_ID from #alreaderClosedSections)

	if not exists(select top 1 1 from #sections) return; --Нечего закрывать, досрочный выход

	drop table #alreaderClosedSections;
end;

--Набор ТП и ТИ для наших сечений
select distinct [Section_ID], sd.TP_ID, [StartDateTime], [FinishDateTime], [IsTransit], sd.[CUS_ID], tp.DirectConsumer_ID
into #tps
from [dbo].[Info_Section_Description2] sd
join Info_TP2 tp on sd.TP_ID = tp.TP_ID
where Section_ID in (select Section_ID from #sections) and StartDateTime < @dtEndServer and ISNULL(FinishDateTime, '21000101') >= @dtStartServer

--Список формул для копирования
select Formula_UN 
into #formulas
from [dbo].[Info_TP2_OurSide_Formula_List]
where TP_ID in (select distinct TP_ID from #tps) and StartDateTime < @dtEndServer and ISNULL(FinishDateTime, '21000101') > @dtStartServer

--Список ТИ для копирования архивов
select distinct ti.TI_ID, TIType 
into #tis
from [dbo].[Info_TP2_OurSide_Formula_Description] d
join Info_TI ti on ti.TI_ID = d.TI_ID
where Formula_UN in (select Formula_UN from #formulas)


--Все копирование необходимо делать в одной транзакции
BEGIN TRY  BEGIN TRANSACTION
declare @closedPeriod_ID uniqueidentifier, @tiType tinyint;
set @closedPeriod_ID = NEWID();
--Таблицы и порядок копирования
--Expl_ClosedPeriod_List

--Максимальные мощности
--ArchComm_Section_Power_Closed
--ArchComm_DirectConsumer_Power
--Info_TP2_Power_Closed

--Конфигурация ТП
--Info_Section_Description_Closed
--Info_TP2_OurSide_Formula_List_Closed
--Info_TP2_OurSide_Formula_Description_Closed

--Коэфф. трансф.
--Info_Transformators_Closed

-- Архив
--ArchCalcBit_30_Closed_XX

-- Архив интегралов
--ArchCalcBit_Integrals_Closed

--Уровни тарифных напряжений
--Info_TP_VoltageLevel_Closed

--Привязка закрытия к сечению
--Expl_ClosedPeriod_To_Section

INSERT INTO [dbo].[Expl_ClosedPeriod_List] ([ClosedPeriod_ID],[Year],[Month],[User_ID],[CUS_ID],[StringName],[DispatchDateTime])
VALUES (@closedPeriod_ID, Year(@startMonthYear), Month(@startMonthYear), @User_ID, @CUS_ID, @StringName, GETDATE())

INSERT INTO [dbo].[ArchComm_Section_Power_Closed] ([Section_ID],[StartDateTime],[PowerLimitType],[ClosedPeriod_ID],[FinishDateTime],[PowerLimit],[CUS_ID])
select [Section_ID],[StartDateTime],[PowerLimitType],@closedPeriod_ID,[FinishDateTime],[PowerLimit],[CUS_ID]
from [dbo].[ArchComm_Section_Power] where Section_ID in (select Section_ID from #sections) and StartDateTime < @dtEndServer and FinishDateTime >= @dtStartServer

INSERT INTO [dbo].[ArchComm_DirectConsumer_Power_Closed] (DirectConsumer_ID,[StartDateTime],[PowerLimitType],[ClosedPeriod_ID],[FinishDateTime],[PowerLimit],[CUS_ID])
select DirectConsumer_ID,[StartDateTime],[PowerLimitType],@closedPeriod_ID,[FinishDateTime],[PowerLimit],[CUS_ID]
from [dbo].[ArchComm_DirectConsumer_Power] where DirectConsumer_ID in (select distinct DirectConsumer_ID from #tps where DirectConsumer_ID is not null) and StartDateTime < @dtEndServer and FinishDateTime >= @dtStartServer

INSERT INTO [dbo].[Info_TP2_Power_Closed] (TP_ID,[StartDateTime],[ClosedPeriod_ID],[FinishDateTime],[AssertedPower],[ConnectedPower],[MaximumPower],[CUS_ID])
select [TP_ID],[StartDateTime],@ClosedPeriod_ID,[FinishDateTime],[AssertedPower],[ConnectedPower],[MaximumPower],[CUS_ID]
from [dbo].[Info_TP2_Power] where TP_ID in (select distinct TP_ID from #tps) and StartDateTime < @dtEndServer and FinishDateTime >= @dtStartServer

INSERT INTO Info_Section_Description_Closed ([Section_ID], TP_ID, [StartDateTime], [ClosedPeriod_ID], [FinishDateTime], [IsTransit], [CUS_ID])
SELECT [Section_ID], TP_ID, [StartDateTime], @ClosedPeriod_ID, [FinishDateTime], [IsTransit], [CUS_ID] from #tps

INSERT INTO [dbo].[Info_TP2_OurSide_Formula_List_Closed] ([Formula_UN]
      ,[ClosedPeriod_ID]
      ,[TP_ID]
      ,[ChannelType]
      ,[ForAutoUse]
      ,[FormulaName]
      ,[User_ID]
      ,[FormulaType_ID]
      ,[HighLimit]
      ,[LowerLimit]
      ,[StartDateTime]
      ,[FinishDateTime]
      ,[CUS_ID])
select [Formula_UN]
      ,@closedPeriod_ID
      ,[TP_ID]
      ,[ChannelType]
      ,[ForAutoUse]
      ,[FormulaName]
      ,[User_ID]
      ,[FormulaType_ID]
      ,[HighLimit]
      ,[LowerLimit]
      ,[StartDateTime]
      ,[FinishDateTime]
      ,[CUS_ID] from [dbo].[Info_TP2_OurSide_Formula_List] l
where l.Formula_UN in (select Formula_UN from #formulas)

INSERT INTO [dbo].[Info_TP2_OurSide_Formula_Description_Closed]
           ([Formula_UN]
           ,[StringNumber]
           ,[ClosedPeriod_ID]
           ,[OperBefore]
           ,[TP_ID]
           ,[UsedFormula_UN]
           ,[TI_ID]
           ,[Section_ID]
           ,[ContrTI_ID]
           ,[ChannelType]
           ,[ListPU_UN]
           ,[OperAfter]
           ,[User_ID]
		   ,[UANode_ID]
           ,[CUS_ID])
SELECT [Formula_UN]
           ,[StringNumber]
           ,@closedPeriod_ID
           ,[OperBefore]
           ,[TP_ID]
           ,[UsedFormula_UN]
           ,[TI_ID]
           ,[Section_ID]
           ,[ContrTI_ID]
           ,[ChannelType]
           ,[ListPU_UN]
           ,[OperAfter]
           ,[User_ID]
		   ,[UANode_ID]
           ,[CUS_ID]
from [dbo].[Info_TP2_OurSide_Formula_Description] d where d.Formula_UN in (select Formula_UN from #formulas)

DECLARE @linkedFormulas TABLE
(
    LinkedFormula_UN VARCHAR(22)
);

INSERT INTO [dbo].[Info_TP_LinkedFormulas_List_Closed]
           ([TP_ID]
           ,[ChannelType]
           ,[StartDateTime]
           ,[ClosedPeriod_ID]
           ,[FinishDateTime]
           ,[LinkedFormula_UN]
           ,[LinkType]
           ,[ApplyDateTime]
           ,[User_ID])
OUTPUT inserted.LinkedFormula_UN into @linkedFormulas
SELECT
          [TP_ID]
           ,[ChannelType]
           ,[StartDateTime]
           ,@closedPeriod_ID
           ,[FinishDateTime]
           ,[LinkedFormula_UN]
           ,[LinkType]
           ,[ApplyDateTime]
           ,[User_ID]
from Info_TP_LinkedFormulas_List where TP_ID in (select distinct TP_ID from #tps)
and StartDateTime < @dtEndServer and ISNULL(FinishDateTime, '21000101') >= @dtStartServer

INSERT INTO [dbo].[Info_TP_LinkedFormulas_OurSide_Description_Closed]
           ([LinkedFormula_UN]
           ,[Formula_UN]
		   ,[ClosedPeriod_ID])
SELECT [LinkedFormula_UN],[Formula_UN],@closedPeriod_ID
FROM Info_TP_LinkedFormulas_OurSide_Description
WHERE LinkedFormula_UN in (select LinkedFormula_UN from @linkedFormulas)

--Копирование коэфф. трансформации
INSERT INTO [dbo].[Info_Transformators_Closed]
           ([TI_ID]
           ,[StartDateTime]
           ,[ClosedPeriod_ID]
           ,[FinishDateTime]
           ,[COEFU]
           ,[COEFI])
select [TI_ID],[StartDateTime],@ClosedPeriod_ID,[FinishDateTime],[COEFU],[COEFI] from Info_Transformators
where ti_id in (select distinct ti_id from #tis) and StartDateTime <= @dtEndServer and ISNULL(FinishDateTime, '21000101') > @dtStartServer

--Приоритеты
INSERT INTO [dbo].[Expl_DataSource_PriorityList_Closed] ([ClosedPeriod_ID],[DataSource_ID],[Priority])
SELECT @ClosedPeriod_ID,[DataSource_ID],[Priority] from [dbo].[Expl_DataSource_PriorityList]
where [Year] = Year(@startMonthYear) and [Month]=Month(@startMonthYear)

INSERT INTO [dbo].[Expl_DataSource_To_TI_TP_Closed]  ([TI_ID],[TP_ID],[ClosedPeriod_ID],[DataSource_ID])
SELECT [TI_ID],[TP_ID],@ClosedPeriod_ID,[DataSource_ID] from Expl_DataSource_To_TI_TP
where TP_ID in (select distinct TP_ID from #tps) --Исходим из сечений поэтому фильтруем только по ТП
and [Year] = Year(@startMonthYear) and [Month]=Month(@startMonthYear)

declare @SQLString NVARCHAR(4000),@SQLExecutor NVARCHAR(4000), @ParmDefinition NVARCHAR(1000), @tableNumber NVARCHAR(3)
, @join NVARCHAR(1000), @whereAdditional NVARCHAR(1000), @mainColumns NVARCHAR(200), @SQLExecutorIntegrals NVARCHAR(4000), @SQLStringIntegrals NVARCHAR(4000)
, @additionalIntegralColumns NVARCHAR(300);
set @additionalIntegralColumns = ',[ContrReplaceStatus],[ManualEnterStatus],[MainProfileStatus]';
SET @ParmDefinition = N'@tiType tinyint, @closedPeriod_ID uniqueidentifier, @startMonthYear DateTime, @dtEnd DateTime';
SET @SQLString = N'INSERT INTO [dbo].[{tableTo}]
			   ([TI_ID],[ChannelType],[EventDate],[DataSource_ID],[ClosedPeriod_ID],
			   [VAL_01],[VAL_02],[VAL_03],[VAL_04],[VAL_05],[VAL_06],[VAL_07],[VAL_08],[VAL_09],[VAL_10]
			   ,[VAL_11],[VAL_12],[VAL_13],[VAL_14],[VAL_15],[VAL_16],[VAL_17],[VAL_18],[VAL_19],[VAL_20]
			   ,[VAL_21],[VAL_22],[VAL_23],[VAL_24],[VAL_25],[VAL_26],[VAL_27],[VAL_28],[VAL_29],[VAL_30]
			   ,[VAL_31],[VAL_32],[VAL_33],[VAL_34],[VAL_35],[VAL_36],[VAL_37],[VAL_38],[VAL_39],[VAL_40]
			   ,[VAL_41],[VAL_42],[VAL_43],[VAL_44],[VAL_45],[VAL_46],[VAL_47],[VAL_48]
			   ,[ValidStatus],[DispatchDateTime],[Status],[ContrReplaceStatus],[ManualEnterStatus],[MainProfileStatus],[CUS_ID]
			   ,[CAL_01],[CAL_02],[CAL_03],[CAL_04],[CAL_05],[CAL_06],[CAL_07],[CAL_08],[CAL_09],[CAL_10]
			   ,[CAL_11],[CAL_12],[CAL_13],[CAL_14],[CAL_15],[CAL_16],[CAL_17],[CAL_18],[CAL_19],[CAL_20]
			   ,[CAL_21],[CAL_22],[CAL_23],[CAL_24],[CAL_25],[CAL_26],[CAL_27],[CAL_28],[CAL_29],[CAL_30]
			   ,[CAL_31],[CAL_32],[CAL_33],[CAL_34],[CAL_35],[CAL_36],[CAL_37],[CAL_38],[CAL_39],[CAL_40]
			   ,[CAL_41],[CAL_42],[CAL_43],[CAL_44],[CAL_45],[CAL_46],[CAL_47],[CAL_48])
				SELECT distinct {mainColumns},ISNULL([DataSource_ID], 0),@closedPeriod_ID,
			   t1.[VAL_01],t1.[VAL_02],t1.[VAL_03],t1.[VAL_04],t1.[VAL_05],t1.[VAL_06],t1.[VAL_07],t1.[VAL_08],t1.[VAL_09],t1.[VAL_10]
			   ,t1.[VAL_11],t1.[VAL_12],t1.[VAL_13],t1.[VAL_14],t1.[VAL_15],t1.[VAL_16],t1.[VAL_17],t1.[VAL_18],t1.[VAL_19],t1.[VAL_20]
			   ,t1.[VAL_21],t1.[VAL_22],t1.[VAL_23],t1.[VAL_24],t1.[VAL_25],t1.[VAL_26],t1.[VAL_27],t1.[VAL_28],t1.[VAL_29],t1.[VAL_30]
			   ,t1.[VAL_31],t1.[VAL_32],t1.[VAL_33],t1.[VAL_34],t1.[VAL_35],t1.[VAL_36],t1.[VAL_37],t1.[VAL_38],t1.[VAL_39],t1.[VAL_40]
			   ,t1.[VAL_41],t1.[VAL_42],t1.[VAL_43],t1.[VAL_44],t1.[VAL_45],t1.[VAL_46],t1.[VAL_47],t1.[VAL_48]
			   ,ISNULL(t1.[ValidStatus], 0),ISNULL(t1.[DispatchDateTime], GetDate()),t1.[Status],ISNULL([ContrReplaceStatus], 0),ISNULL([ManualEnterStatus],0),case when t1.EventDate is null then null else 0 end as MainProfileStatus,t1.[CUS_ID]
			   ,[CAL_01],[CAL_02],[CAL_03],[CAL_04],[CAL_05],[CAL_06],[CAL_07],[CAL_08],[CAL_09],[CAL_10]
			   ,[CAL_11],[CAL_12],[CAL_13],[CAL_14],[CAL_15],[CAL_16],[CAL_17],[CAL_18],[CAL_19],[CAL_20]
			   ,[CAL_21],[CAL_22],[CAL_23],[CAL_24],[CAL_25],[CAL_26],[CAL_27],[CAL_28],[CAL_29],[CAL_30]
			   ,[CAL_31],[CAL_32],[CAL_33],[CAL_34],[CAL_35],[CAL_36],[CAL_37],[CAL_38],[CAL_39],[CAL_40]
			   ,[CAL_41],[CAL_42],[CAL_43],[CAL_44],[CAL_45],[CAL_46],[CAL_47],[CAL_48]
	FROM {tableFrom} where (t1.TI_ID in (select TI_ID from #tis where titype = @TIType) and t1.[EventDate] between floor(cast(@startMonthYear as float)) and @dtEnd)'

	set @SQLStringIntegrals = N'INSERT INTO [dbo].[{tableTo}] ([TI_ID],[EventDateTime],[ChannelType],[DataSource_ID],[ClosedPeriod_ID],[Data],
				[ManualEnterData],[IntegralType],[DispatchDateTime],[Status],[CUS_ID]{AdditionalColumns})
		select [TI_ID],[EventDateTime],[ChannelType],[DataSource_ID],@closedPeriod_ID,[Data],
				[ManualEnterData],[IntegralType],[DispatchDateTime],[Status],[CUS_ID]{AdditionalColumns}
		from [dbo].[{tableFrom}] where TI_ID in (select TI_ID from #tis where titype = @TIType) and EventDateTime between @startMonthYear and DateAdd(minute, 1, @dtEnd)' --Интегралы сохраняем с заходом в следующий месяц


declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct titype from #tis
open t;
FETCH NEXT FROM t into @TIType
WHILE @@FETCH_STATUS = 0
BEGIN
	
	--Закрывается только бытовые точки
	if (@TIType > 10) begin
		set @SQLExecutor = REPLACE(@SQLString, '{mainColumns}', 't1.[TI_ID],t1.[ChannelType],t1.[EventDate]');
	
		set @tableNumber = ltrim(str(@TIType - 10,2));
		set @SQLExecutor = REPLACE(@SQLExecutor, '{tableFrom}', 'ArchCalcBit_30_Virtual_' +  @tableNumber + ' t1');
		set @SQLExecutor = REPLACE(@SQLExecutor, '{tableTo}', 'ArchCalcBit_30_Closed_' + @tableNumber);

		--Копирование интегралов
		set @SQLExecutorIntegrals = REPLACE(@SQLStringIntegrals, '{tableFrom}', 'ArchCalcBit_Integrals_Virtual_' +  @tableNumber);
		set @SQLExecutorIntegrals = REPLACE(@SQLExecutorIntegrals, '{tableTo}', 'ArchCalcBit_Integrals_Closed_' + @tableNumber);
		set @SQLExecutorIntegrals = REPLACE(@SQLExecutorIntegrals, '{AdditionalColumns}', @additionalIntegralColumns);

	end else begin
		set @join = ' t1 full outer join ArchCalc_30_Virtual_Closed v on v.TI_ID=t1.TI_ID and v.EventDate=t1.EventDate and v.ChannelType=t1.ChannelType';
		set @whereAdditional = ' or (t1.TI_ID is null and v.TI_ID in (select TI_ID from #tis where titype = @TIType) and v.[EventDate] between floor(cast(@startMonthYear as float)) and @dtEnd)'
		if (@TIType = 1) begin
			set @SQLExecutor = REPLACE(@SQLString, '{tableFrom}', 'ArchComm_30_Import_From_XML' + @join);
		end else if (@TIType = 2) begin
			set @SQLExecutor = REPLACE(@SQLString, '{tableFrom}', 'ArchCalc_30_Month_Values' + @join);
		end else begin
			set @SQLExecutor = REPLACE(@SQLString, '{tableFrom}', 'ArchComm_30_Values' + @join);
		end

		set @SQLExecutor = REPLACE(@SQLExecutor, '{mainColumns}', 'ISNULL(t1.[TI_ID], v.[TI_ID]),ISNULL(t1.[ChannelType], v.[ChannelType]),ISNULL(t1.[EventDate], v.[EventDate])');
		set @SQLExecutor = REPLACE(@SQLExecutor, '{tableTo}', 'ArchCalc_30_Virtual_Closed') + @whereAdditional;

		--Копирование интегралов
		set @SQLExecutorIntegrals = REPLACE(@SQLStringIntegrals, '{tableFrom}', 'ArchCalc_Integrals_Virtual');
		set @SQLExecutorIntegrals = REPLACE(@SQLExecutorIntegrals, '{tableTo}', 'ArchCalc_Integrals_Closed');
		set @SQLExecutorIntegrals = REPLACE(@SQLExecutorIntegrals, '{AdditionalColumns}', '');
	end;

	--print @SQLExecutor;
	--Копирование получасовок
	EXEC sp_executesql @SQLExecutor, @ParmDefinition, @tiType, @closedPeriod_ID, @dtStartServer, @dtEndServer
	--Копирование интегралов
	--print @SQLExecutorIntegrals;
	EXEC sp_executesql @SQLExecutorIntegrals, @ParmDefinition, @tiType, @closedPeriod_ID, @dtStartServer, @dtEndServer

	FETCH NEXT FROM t into @TIType
END;
CLOSE t
DEALLOCATE t

--Уровни тарифных напряжений
insert into [dbo].[Info_TP_VoltageLevel_Closed] (TP_ID, StartDateTime, ClosedPeriod_ID, VoltageLevel, FinishDateTime)
select TP_ID, StartDateTime, @closedPeriod_ID, VoltageLevel, FinishDateTime from [dbo].[Info_TP_VoltageLevel]
where TP_ID in (select distinct TP_ID from #tps) and StartDateTime < @dtEndServer and ISNULL(FinishDateTime, '21000101') >= @dtStartServer

--Теперь можно сохранять в основных таблицах
INSERT INTO [dbo].[Expl_ClosedPeriod_To_Section] ([Section_ID],  [ClosedPeriod_ID], [ClosedPeriodType], IsCustomerCoordinated) 
select Section_ID, @closedPeriod_ID, @closedPeriodType, IsCustomerCoordinated from #sections

declare @eventString varchar(255), @objectID varchar(255), @commentString varchar(255);
set @commentString = ''; set @objectID = ''; set @eventString = '';
select @commentString = @commentString + SectionName + '; ', @objectID = @objectID + ltrim(str(Section_ID,5))  + ',' 
from Info_Section_List where Section_ID in (select section_id from #sections)
	
if (@closedPeriodType = 0) set @eventString = 'Закрытие';
else set @eventString = 'Перерасчет';

set @eventString += ' отчетного периода ' + DATENAME(MONTH, @startMonthYear) + ' ' + ltrim(str(Year(@startMonthYear), 4)) + 'г.';
--Запись в журнал о закрытии периода
insert into Expl_User_Journal (ApplicationType, CommentString, CUS_ID, EventDateTime, EventString, User_ID, ObjectID, ObjectName)
values (1, @commentString, @CUS_ID, GETDATE(), @eventString, @User_ID, @objectID, 'Expl_ClosedPeriod_To_Section');


COMMIT END TRY
BEGIN CATCH
	--Ошибка, откатываем все изменения
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 16 -- На верху нужен exception
	--SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH

--Помечаем временные таблицы к удалению (быстрее уходит потом память в SQL)
drop table #sections;
drop table #tps;
drop table #tis;

end
go
grant EXECUTE on usp2_Expl_ClosePeriod to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2010
--
-- Описание:
--
--		Выбираем параметры по списку ТП 
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetTPParams]
(	
	@RequestSectionParams RequestSectionParamsType READONLY, --Точки поставок
	@MoneyOurSideMode tinyint = 0,
	@dtStart DateTime,
	@dtEnd DateTime
)
AS
BEGIN 
			set nocount on
			set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
			set numeric_roundabort off
			set transaction isolation level read uncommitted

			select f.*, p.TypeHierarchy, p.ClosedPeriod_ID, p.SecondaryID
			into #params
			from @RequestSectionParams p
			cross apply usf2_Info_GetTPParams(p.ID, null, @dtStart, @dtEnd, case when p.TypeHierarchy = 9 then 1 else 0 end, @MoneyOurSideMode, p.ClosedPeriod_ID) f


			select f.*, d.Section_ID, 
			dbo.usf2_Info_GetRangeTpInSection(f.SecondaryID, f.TP_ID, f.ClosedPeriod_ID, @dtStart, @dtEnd, 1) as RangeInSection,
			z.MsTimeZoneId as TimeZoneId
			FROM #params f
			outer apply
				(
					select top (1) * from Info_Section_Description2 where TP_ID = f.TP_ID
					and StartDateTime <= @dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
					order by StartDateTime desc
				) d
			join Info_Section_List s on s.Section_ID = d.Section_ID
			left join Dict_TimeZone_Zones z on z.TimeZone_ID = s.TimeZone_ID
			where f.ClosedPeriod_ID is null 
			union 
			--Закрытый период
			select f.*, d.Section_ID,
			dbo.usf2_Info_GetRangeTpInSection(f.SecondaryID, f.TP_ID, f.ClosedPeriod_ID, @dtStart, @dtEnd, 1) as RangeInSection,
			z.MsTimeZoneId as TimeZoneId
			FROM #params f
			outer apply
				(
					select top (1) * from Info_Section_Description_Closed where TP_ID = f.TP_ID
					and StartDateTime <= @dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
					order by StartDateTime desc
				) d
			join Info_Section_List s on s.Section_ID = d.Section_ID
			left join Dict_TimeZone_Zones z on z.TimeZone_ID = s.TimeZone_ID
			where f.ClosedPeriod_ID is not null
end
go
grant EXECUTE on usp2_Info_GetTPParams to [UserCalcService]
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
--		ТП в объектах потребления со своими параметрами
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetPointsByDirectConsumerAndSection]
(	
	@RequestSectionParams RequestSectionParamsType READONLY, --Объекты потребления
	@dtStart DateTime,
	@dtEnd DateTime
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--1 Результат, параметры ТП
	--Открытые данные
	SELECT distinct tp.TP_ID, tp.DirectConsumer_ID, s.Section_ID, null as ClosedPeriod_ID 
	FROM [dbo].[Info_TP2] tp 
	join Info_Section_Description2 s on s.TP_ID = tp.TP_ID 
    where DirectConsumer_ID is not null and DirectConsumer_ID in (select ID from @RequestSectionParams where ClosedPeriod_ID is null) 
	and StartDateTime <= @dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart
	union 
	--Закрытые данные
	SELECT distinct tp.TP_ID, tp.DirectConsumer_ID, s.Section_ID, r.ClosedPeriod_ID
	FROM @RequestSectionParams r
	join [dbo].[Info_TP2] tp on tp.DirectConsumer_ID is not null and tp.DirectConsumer_ID = r.ID
	join Info_Section_Description_Closed s on s.TP_ID = tp.TP_ID and s.ClosedPeriod_ID = r.ClosedPeriod_ID
    where r.ClosedPeriod_ID is not null and StartDateTime <= @dtEnd and ISNULL(FinishDateTime, '21000101') >= @dtStart

	--2 результат, названия объектов потребления
	select DirectConsumer_ID as ID, StringName from Dict_DirectConsumer
	where DirectConsumer_ID in (select distinct ID from @RequestSectionParams)
			
end
go
grant EXECUTE on usp2_Info_GetPointsByDirectConsumerAndSection to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2013
--
-- Описание:
--
--		Формирование названий объектов по их типам
--	
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetDictionaryOfNames] (
		@IdArray RequestSectionParamsType READONLY
)	
	RETURNS TABLE 
	AS
	RETURN
	(
		select Id, TypeHierarchy, StringId,
		case 
			--Сечения
			when  TypeHierarchy = 5 then (select top 1 SectionName from Info_Section_List where Section_ID = id.ID) 
			--ТП
			when  TypeHierarchy = 8 then (select top 1 StringName from Info_TP2 where TP_ID = id.ID) 
			--Прямые потребители
			when  TypeHierarchy = 16 then (select top 1 StringName from Dict_DirectConsumer where DirectConsumer_ID = id.ID) 
			--Юр.лицо
			when  TypeHierarchy = 19 then (select top 1 StringName from Dict_JuridicalPersons where JuridicalPerson_ID = id.ID) 
			--Контракты юр.лиц
			when  TypeHierarchy = 20 then (select top 1 StringName from Dict_JuridicalPersons_Contracts where JuridicalPersonContract_ID = id.ID) 

			--Узел в дереве свободной иерархии
			when  TypeHierarchy = 28 then (select top 1 [StringName] from Dict_FreeHierarchyTree where [FreeHierItem_ID] = id.ID) 

			--Объект прогнозирования
			when  TypeHierarchy = 42 then (select top 1 [ForecastObjectName] from Forecast_Objects where [ForecastObject_UN] = id.StringId) 

			--Остальные объекты добавлять по мере необходимости
			

		end as StringName
		from @IdArray id
	)
go
grant select on usf2_Info_GetDictionaryOfNames to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2013
--
-- Описание:
--
--		Архивные данные в зависимости от таблицы, сортированно по приоритету источника
--
-- ======================================================================================

create  proc [dbo].[usp2_Info_GetDictionaryOfNames]

	@IdArray RequestSectionParamsType READONLY
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select * from dbo.usf2_Info_GetDictionaryOfNames(@IdArray)
end
go
grant EXECUTE on usp2_Info_GetDictionaryOfNames to [UserCalcService]
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
--		Читаем максимальные мощностя по списку объектов в диапазоне времени
--
-- ======================================================================================

create proc [dbo].[usp2_GetMaxPowerByObject]
(	
	@RequestObjectsParams RequestSectionParamsType READONLY, --Точки поставок с закрытими периодами
	@StartDateTime datetime,
	@FinishDateTime datetime
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--Сечения открытые данные
	select Section_ID as Id, cast(5 as tinyint) as TypeHierarchy, cast(null as uniqueidentifier) as ClosedPeriod_ID,  PowerLimit, StartDateTime, FinishDateTime from ArchComm_Section_Power
	where Section_ID in (select ID from @RequestObjectsParams where TypeHierarchy = 5 and ClosedPeriod_ID is null) --Сечение
	and StartDateTime <= @FinishDateTime and FinishDateTime >= @StartDateTime and PowerLimitType = 16384
	union all
	--Сечения закрытые данные
	select Section_ID as Id, cast(5 as tinyint) as TypeHierarchy, ClosedPeriod_ID, PowerLimit, StartDateTime, FinishDateTime from ArchComm_Section_Power_Closed
	where Section_ID in (select ID from @RequestObjectsParams where TypeHierarchy = 5 and ClosedPeriod_ID is not null) --Сечение
	and StartDateTime <= @FinishDateTime and FinishDateTime >= @StartDateTime and PowerLimitType = 16384
	union all
	--Объекты потребления открытые данные
	select DirectConsumer_ID as ID, cast(16 as tinyint) as TypeHierarchy, cast(null as uniqueidentifier) as ClosedPeriod_ID,  PowerLimit, StartDateTime, FinishDateTime from ArchComm_DirectConsumer_Power
	where DirectConsumer_ID in (select ID from @RequestObjectsParams where TypeHierarchy = 16 and ClosedPeriod_ID is null) --Сечение
	and StartDateTime <= @FinishDateTime and FinishDateTime >= @StartDateTime and PowerLimitType = 16384
	union all
	--Объекты потребления закрытые данные
	select DirectConsumer_ID as ID, cast(16 as tinyint) as TypeHierarchy, ClosedPeriod_ID, PowerLimit, StartDateTime, FinishDateTime from ArchComm_DirectConsumer_Power_Closed
	where DirectConsumer_ID in (select ID from @RequestObjectsParams where TypeHierarchy = 16 and ClosedPeriod_ID is not null) --Сечение
	and StartDateTime <= @FinishDateTime and FinishDateTime >= @StartDateTime and PowerLimitType = 16384
	union all
	--Точки поставки
	select TP_ID as Id, cast(8 as tinyint) as TypeHierarchy, cast(null as uniqueidentifier) as ClosedPeriod_ID, MaximumPower, StartDateTime, FinishDateTime from Info_TP2_Power
	where TP_ID in (select ID from @RequestObjectsParams where TypeHierarchy = 8 and ClosedPeriod_ID is null)
	and StartDateTime <= @FinishDateTime and FinishDateTime >= @StartDateTime
	union all
	select TP_ID as Id, cast(8 as tinyint) as TypeHierarchy, ClosedPeriod_ID, MaximumPower, StartDateTime, FinishDateTime from Info_TP2_Power_Closed
	where TP_ID in (select ID from @RequestObjectsParams where TypeHierarchy = 8 and ClosedPeriod_ID is not null)
	and StartDateTime <= @FinishDateTime and FinishDateTime >= @StartDateTime


END
go
grant EXECUTE on usp2_GetMaxPowerByObject to [UserCalcService]
go
grant EXECUTE on usp2_GetMaxPowerByObject to [UserSlave61968Service]
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
--		Читаем список объектов потребления для ТП в открытом и закрытом периоде
--
-- ======================================================================================

create proc [dbo].[usp2_Expl_DirectConsumerByTP]
(	
	@RequestSectionParams RequestSectionParamsType READONLY --Точки поставок с закрытими периодами
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select distinct DirectConsumer_ID as ID, cast(16  as tinyint) as TypeHierarchy, p.ClosedPeriod_ID from Info_TP2 tp
	join @RequestSectionParams p on p.ID = tp.TP_ID
	where tp.DirectConsumer_ID is not null
END
go
grant EXECUTE on usp2_Expl_DirectConsumerByTP to [UserCalcService]
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
--		Выборка дельты по ТП и закрытию
--
-- ======================================================================================
create proc [dbo].[usp2_GroupTP_ReadDeltaFromOpenDataTp]
	@ids RequestSectionParamsType READONLY, --Идентификаторы объектов
	@groupTPPowerReportMode tinyint = 2, --Режим формирования документа
	-- 2 Расчет по закрытым данным, с дорасчетами в последующие периоды
	-- 3 Расчет по закрытым данным, с дорасчетами за предыдущие периоды
	@Year int = null, --Отчетный период для режима 3 (год)
	@Month int = null, --Отчетный период для режима 3 (месяц)
	@IsReadInactive bit = 0 --Читать неактивные записи
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	declare 
	@monthYear DateTime;

	if (@Year is not null and @Month is not null) set @monthYear = dateadd(mm,(@Year-1900)* 12 + @Month - 1,0) --Дата на начало месяца
	else set @monthYear = GetDate();

	create table #tps
	(
		TP_ID int,
		ClosedPeriod_ID uniqueidentifier,
	)

	--Все ТП
	insert into #tps
	select ID, ClosedPeriod_ID 
	from @ids where TypeHierarchy = 8
	--Все сечения
	union all
	select sd.TP_ID, id.ClosedPeriod_ID 
	from @ids id join Info_Section_Description2 sd on sd.Section_ID = id.ID 
	where id.TypeHierarchy = 5 and @monthYear between sd.StartDateTime and ISNULL(sd.FinishDateTime, '21000101')
	--Объекты потребления
	union all
	select sd.TP_ID, id.ClosedPeriod_ID 
	from @ids id join Info_TP2 sd on sd.DirectConsumer_ID = id.ID 
	where id.TypeHierarchy = 16

	if (@groupTPPowerReportMode = 2) begin
	--Если пользователь выбирает этот режим формирования и отчетный месяц, 
	--отчет требуется собирать по тем данными, которые были выставлены и закрыты в отчетном месяце
	--плюсом дорасчеты в последующих месяцах за этот отчетный месяц
		select tp.TP_ID, tp.ClosedPeriod_ID, sd.Section_ID, c.ToYear, c.ToMonth, c.Delta, ISNULL(vl.VoltageLevel, 0) as VoltageLevel
		, inf.DirectConsumer_ID, c.IsActive, c.IsCustomerCoordinated, u.UserName, c.DispatchDateTime
		 from #tps tp
		join Expl_ClosedPeriod_DeltaTpFromOpen c on c.ClosedPeriod_ID = tp.ClosedPeriod_ID and c.TP_ID = tp.TP_ID
			and c.DispatchDateTime = (select MAX(DispatchDateTime) from Expl_ClosedPeriod_DeltaTpFromOpen where ClosedPeriod_ID = tp.ClosedPeriod_ID and TP_ID = tp.TP_ID and (@IsReadInactive = 1 OR IsActive = 1))
		join Info_Section_Description_Closed sd on sd.TP_ID = tp.TP_ID and sd.ClosedPeriod_ID = tp.ClosedPeriod_ID
		join Info_TP2 inf on inf.TP_ID = tp.TP_ID
		left join Info_TP_VoltageLevel vl on vl.TP_ID = tp.TP_ID 
				and vl.StartDateTime = (select Max(StartDateTime) from Info_TP_VoltageLevel where TP_ID = tp.TP_ID and StartDateTime <= @monthYear and ISNULL(FinishDateTime, '21000101')>=@monthYear)
		join Expl_Users u on u.[User_ID] = c.[User_ID]
		where c.IsActive = 1 OR @IsReadInactive = 1
		order by c.ToYear, c.ToMonth, sd.Section_ID, inf.DirectConsumer_ID, tp.TP_ID, tp.ClosedPeriod_ID
	end else if (@groupTPPowerReportMode = 3 and @Year is not null and @Month is not null) begin
	--Если пользователь выбирает этот режим формирования и отчетный месяц, 
	--отчет требуется собирать по тем данными, которые были выставлены и закрыты в отчетном месяце 
	--плюсом дорасчеты за предыдущие месяцы, которые были выставлены за этот отчетный месяц
		select tp.TP_ID, tp.ClosedPeriod_ID, sd.Section_ID, cpl.[Year] as ToYear, cpl.[Month] as ToMonth, c.Delta, ISNULL(vl.VoltageLevel, 0) as VoltageLevel
		, inf.DirectConsumer_ID, c.IsActive, c.IsCustomerCoordinated, u.UserName, c.DispatchDateTime
		 from #tps tp
		join Expl_ClosedPeriod_DeltaTpFromOpen c on c.ClosedPeriod_ID = tp.ClosedPeriod_ID and c.TP_ID = tp.TP_ID
		and c.ToYear = @Year and c.ToMonth = @Month
		and c.DispatchDateTime = (select MAX(DispatchDateTime) 
					from Expl_ClosedPeriod_DeltaTpFromOpen 
					where ClosedPeriod_ID = c.ClosedPeriod_ID and TP_ID = tp.TP_ID and (IsActive = 1 or @IsReadInactive = 1))
		join [dbo].[Expl_ClosedPeriod_List] cpl on cpl.ClosedPeriod_ID = c.ClosedPeriod_ID
		join Info_Section_Description_Closed sd on sd.TP_ID = tp.TP_ID and sd.ClosedPeriod_ID = c.ClosedPeriod_ID
		join Info_TP2 inf on inf.TP_ID = tp.TP_ID
		left join Info_TP_VoltageLevel_Closed vl on vl.TP_ID = tp.TP_ID and vl.ClosedPeriod_ID = c.ClosedPeriod_ID 
				and vl.StartDateTime = (select Max(StartDateTime) from Info_TP_VoltageLevel_Closed where TP_ID = tp.TP_ID and ClosedPeriod_ID = c.ClosedPeriod_ID and StartDateTime <= @monthYear and ISNULL(FinishDateTime, '21000101')>=@monthYear)
		join Expl_Users u on u.[User_ID] = c.[User_ID]
		where c.ToYear = @Year and c.ToMonth = @Month
		order by c.ToYear, c.ToMonth, sd.Section_ID, inf.DirectConsumer_ID, tp.TP_ID, tp.ClosedPeriod_ID
	end

	drop table #tps;

	--В остальных случаях дельта не выбирается
end
go
grant EXECUTE on usp2_GroupTP_ReadDeltaFromOpenDataTp to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2013
--
-- Описание:
--
--		Возврат связки объект потребления - сечение - закрытие
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetSectionForDirectConsumer]
(	
	@RequestSectionParams RequestSectionParamsType READONLY, --Объекты потребления
	@datestart datetime,
	@dateend datetime
	
)
AS
BEGIN 
			set nocount on
			set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
			set numeric_roundabort off
			set transaction isolation level read uncommitted

			--Открытые данные
			select r.ID
			, (select top 1 Section_ID from Info_Section_Description2 
				where TP_ID = (select top 1 TP_ID from Info_TP2 where DirectConsumer_ID = r.ID)) as SecondaryID
			, r.ClosedPeriod_ID, r.TypeHierarchy
			from @RequestSectionParams r
			where r.ClosedPeriod_ID is null
			--Закрытые данные
			union 
			select r.ID
			, (select top 1 Section_ID from Info_Section_Description_Closed 
				where TP_ID = (select top 1 TP_ID from Info_TP2 where DirectConsumer_ID = r.ID) 
				and ClosedPeriod_ID = r.ClosedPeriod_ID) as SecondaryID
			, r.ClosedPeriod_ID, r.TypeHierarchy
			from @RequestSectionParams r
			where r.ClosedPeriod_ID is not null
			order by SecondaryID
			
end
go
grant EXECUTE on usp2_Info_GetSectionForDirectConsumer to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2019
--
-- Описание:
--
--		Журнал событий ТИ
--
-- ======================================================================================
CREATE view [dbo].[vw_GlobalJournalTi] 
WITH SCHEMABINDING
AS
	--Больше 2 000 000 пока неполучается достаточно быстро подсчитать, зависает
	select arch.ID, arch.TypeHierarchy, arch.EventDateTime  
	,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime, CAST(2 as tinyint) as JournalFilter
	,arch.TiType 
	,ct.StringName as EventName
	,cte.StringName as ExtendetEventName
	,ti.PS_ID, ps.StringName as PsName, ms.MeterSerialNumber as SerialNumber, '' as IPMain, ms.METER_ID as Meter_ID
	,TIName as EventSourceString
	from
	(
		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,11 as TiType
		from 
		dbo.ArchBit_Events_Journal_1 arch with (nolock)
		--where arch.EventCode >= 0
	
		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,1 as TiType
		from 
		dbo.ArchComm_Events_Journal_TI arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,12 as TiType
		from 
		dbo.ArchBit_Events_Journal_2 arch with (nolock) 
	
		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,13 as TiType
		from 
		dbo.ArchBit_Events_Journal_3 arch with (nolock) 
	
		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,14 as TiType
		from 
		dbo.ArchBit_Events_Journal_4 arch with (nolock) 
	
		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,15 as TiType
		from 
		dbo.ArchBit_Events_Journal_5 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,16 as TiType
		from 
		dbo.ArchBit_Events_Journal_6 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,17 as TiType
		from 
		dbo.ArchBit_Events_Journal_7 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,18 as TiType
		from 
		dbo.ArchBit_Events_Journal_8 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,19 as TiType
		from 
		dbo.ArchBit_Events_Journal_9 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,20 as TiType
		from 
		dbo.ArchBit_Events_Journal_10 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,21 as TiType
		from 
		dbo.ArchBit_Events_Journal_11 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,22 as TiType
		from 
		dbo.ArchBit_Events_Journal_12 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,23 as TiType
		from 
		dbo.ArchBit_Events_Journal_13 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,24 as TiType
		from 
		dbo.ArchBit_Events_Journal_14 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,25 as TiType
		from 
		dbo.ArchBit_Events_Journal_15 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,26 as TiType
		from 
		dbo.ArchBit_Events_Journal_16 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,27 as TiType
		from 
		dbo.ArchBit_Events_Journal_17 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,28 as TiType
		from 
		dbo.ArchBit_Events_Journal_18 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,29 as TiType
		from 
		dbo.ArchBit_Events_Journal_19 arch with (nolock) 

		union all

		select arch.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy, arch.EventDateTime  
		,arch.EventCode, arch.ExtendedEventCode, arch.DispatchDateTime
		,30 as TiType
		from 
		dbo.ArchBit_Events_Journal_20 arch with (nolock) 
	) arch
	join dbo.Info_TI ti on ti.TI_ID = arch.ID
	join dbo.Dict_PS ps on ps.PS_ID = ti.PS_ID
	outer apply 
	(
		select top 1 hm.MeterSerialNumber, hm.Meter_ID from dbo.Info_Meters_TO_TI imtt with (nolock)
		join dbo.Hard_Meters hm with (nolock) on hm.Meter_ID = imtt.METER_ID
		where imtt.TI_ID = arch.ID and StartDateTime <= arch.EventDateTime and (FinishDateTime is null or FinishDateTime >= arch.EventDateTime)
		order by StartDateTime desc
	) ms
	left join [dbo].[Dict_TI_Journal_Event_Codes] ct with (nolock) on ct.EventCode = arch.EventCode
	left join [dbo].[Dict_TI_Journal_ExtendedEvent_Codes] cte with (nolock) on cte.ExtendedEventCode = arch.ExtendedEventCode and cte.MeterType_ID = ms.Meter_ID

GO
grant select on vw_GlobalJournalTi to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2019
--
-- Описание:
--
--		Журнал событий УСПД и Е422
--
-- ======================================================================================
CREATE view [dbo].[vw_GlobalJournalE422_USPD] 
WITH SCHEMABINDING
AS
	--События E422
	select e.E422_ID as ID, CAST(40 as tinyint) as TypeHierarchy
	, EventDateTime, e.EventCode
	, [EventData], [EventString],	[EventData2], [EventString2], [Status],
	[DispatchDateTime], CAST(1 as tinyint) as JournalFilter
	,PS_ID, E422SerialNumber as SerialNumber, E422IPMain as IPMain, ce.StringName as EventName
	from [dbo].[ArchComm_Events_Journal_E422] e with (nolock) 
	join [dbo].Hard_E422 he with (nolock) on he.E422_ID = e.E422_ID
	left join [dbo].[Hard_E422CommChannels_Links] cce with (nolock) on cce.E422_ID = e.E422_ID
	left join [dbo].[Hard_CommChannels] hc with (nolock) on hc.CommChannel_ID = cce.CommChannel_ID
	left join [dbo].[Dict_E422_JournalDataCollect_Codes] ce with (nolock) on ce.EventCode = e.EventCode

	union all

	--Коррекция времени E422
	select e.E422_ID as ID, CAST(40 as tinyint) as TypeHierarchy
	, EventDateTime, 7 as EventCode
	, [ClockDiff] as [EventData], NULL, NULL,NULL, NULL,
	DateTimeSetExecuted as [DispatchDateTime], CAST(1 as tinyint) as JournalFilter
	,PS_ID, E422SerialNumber as SerialNumber, E422IPMain as IPMain, 'Коррекция времени в контроллере' as EventName
	from
	[dbo].[ArchComm_ClockDiff_Center_E422] e  with (nolock)
	join [dbo].Hard_E422 he with (nolock) on he.E422_ID = e.E422_ID
	left join [dbo].[Hard_E422CommChannels_Links] cce with (nolock) on cce.E422_ID = e.E422_ID
	left join [dbo].[Hard_CommChannels] hc with (nolock) on hc.CommChannel_ID = cce.CommChannel_ID

	union all

	--События УСПД
	select e.USPD_ID as ID, CAST(32 as tinyint) as TypeHierarchy
	, EventDateTime, e.EventCode
	, [EventData], [EventString],	[EventData2], [EventString2], [Status],
	[DispatchDateTime], CAST(1 as tinyint) as JournalFilter
	,PS_ID, USPDSerialNumber as SerialNumber, USPDIPMain as IPMain, ce.StringName as EventName
	from
	[dbo].[ArchComm_Events_Journal_USPD] e with (nolock)
	join [dbo].Hard_USPD he with (nolock) on he.USPD_ID = e.USPD_ID
	left join [dbo].[Hard_USPDCommChannels_Links] cce with (nolock) on cce.USPD_ID = e.USPD_ID
	left join [dbo].[Hard_CommChannels] hc with (nolock) on hc.CommChannel_ID = cce.CommChannel_ID
	left join [dbo].[Dict_USPD_Event_Journal_Codes] ce with (nolock) on ce.EventCode = e.EventCode		
	union all

	--Коррекция времени УСПД
	select e.USPD_ID as ID, CAST(32 as tinyint) as TypeHierarchy
	, EventDateTime, 7 as EventCode 
	, [ClockDiff] as [EventData], NULL, NULL,NULL, NULL,
	DateTimeSetExecuted as [DispatchDateTime], CAST(1 as tinyint) as JournalFilter
	,PS_ID, USPDSerialNumber as SerialNumber, USPDIPMain as IPMain, 'Коррекция времени в контроллере' as EventName
	from
	[dbo].[ArchComm_ClockDiff_Center_USPD] e  with (nolock)
	join [dbo].Hard_USPD he with (nolock) on he.USPD_ID = e.USPD_ID
	left join [dbo].[Hard_USPDCommChannels_Links] cce with (nolock) on cce.USPD_ID = e.USPD_ID
	left join [dbo].[Hard_CommChannels] hc with (nolock) on hc.CommChannel_ID = cce.CommChannel_ID
		
GO
grant select on vw_GlobalJournalE422_USPD to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2017
--
-- Описание:
--
--		Расшифровываем события журнала обходного выключателя
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_JournalOvEventTypeInterpretation]
(
	@EventType smallint
)
RETURNS varchar(1024)
WITH SCHEMABINDING
AS
BEGIN
	if (@EventType is null) return null;

	return case @EventType 
		when 0 then 'Удаление обходного выключателя'
		when 1 then 'Добавление обходного выключателя'
		when 2 then 'Изменение обходного выключателя'
				
		else '<Не найдено в usf2_JournalOvEventTypeInterpretation> ' + str(@EventType)
	end
END
go
grant EXECUTE on usf2_JournalOvEventTypeInterpretation to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2017
--
-- Описание:
--
--		Расшифровываем коды журнала центра сбора
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_ServersGlobalJournalCodeInterpretation]
(
	@EventCode int
)
RETURNS varchar(1024)
WITH SCHEMABINDING
AS
BEGIN
	if (@EventCode is null) return null;

	return case @EventCode 
		when 0 then 'Коррекция времени'
	end
END
go
grant EXECUTE on usf2_ServersGlobalJournalCodeInterpretation to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2017
--
-- Описание:
--
--		Расшифровываем поставщиков журнала центра сбора
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_ServersGlobalJournalSourceInterpretation]
(
	@EventSource int
)
RETURNS varchar(1024)
WITH SCHEMABINDING
AS
BEGIN
	if (@EventSource is null) return null;

	return case @EventSource 
		when 0 then 'Сбор данных'
		when 1 then 'Сбор данных УСПД'
		when 2 then 'Мониторинг'
		when 3 then 'Сервер учетных показателей и ОВ'
		when 4 then 'Сервер AutoDetect'
	end
END
go
grant EXECUTE on usf2_ServersGlobalJournalSourceInterpretation to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2017
--
-- Описание:
--
--		Расшифровываем поставщиков журнала приложений
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_JournalApplicationTypeInterpretation]
(
	@ApplicationType smallint
)
RETURNS varchar(1024)
WITH SCHEMABINDING
AS
BEGIN
	if (@ApplicationType is null) return null;

	return case @ApplicationType 
		when 0 then 'Служба расчетов'
		when 1 then 'АРМ энергетика'
		when 2 then 'Описатель'
		when 3 then 'Обработка данных'
		when 4 then 'Служба рассылки'
		when 5 then 'Почтовая служба'
		when 6 then 'Мгновенные значения'
		when 7 then 'Подсистема мониторинга'
		when 8 then 'Репликация'
		when 9 then 'Сбор данных'
		when 10 then 'Служба уведомления пользователей'
		when 11 then 'Служба мастер 61968'
		when 12 then 'Служба подчиненная 61968'
		when 13 then 'Служба очистки таблиц'
		when 14 then 'Служба выгрузки данных в РДМ'

		when 20 then 'Служба репликации справочников'
		when 21 then 'Телескоп - Офис (надстройка Excel)'
		when 30 then '"АРМ энергетика Windows Store App")'
		when 31 then '"АРМ энергетика IOS'
		when 32 then '"АРМ энергетика Android'
		when 40 then 'Конвертер МЭК 60870-5-104 OPC UA'
		when 50 then 'Дизайнер форм'
		when 51 then 'Пульт диспетчера'
		when 52 then 'Пульт диспетчера мобильный'

		else '<Не найдено в usf2_JournalApplicationTypeInterpretation> ' + str(@ApplicationType)
	end
END
go
grant EXECUTE on usf2_JournalApplicationTypeInterpretation to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2017
--
-- Описание:
--
--		Расшифровываем события журнала приложений
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_JournalEventTypeInterpretation]
(
	@EventType smallint
)
RETURNS varchar(1024)
WITH SCHEMABINDING
AS
BEGIN
	if (@EventType is null) return null;

	return case @EventType 
		when 1 then 'Ошибка'
		when 2 then 'Предупреждение'
		when 4 then 'Информация'
		when 8 then 'Успешный вход'
		when 16 then 'Ошибка входа'
		
		else '<Не найдено в usf2_JournalEventTypeInterpretation> ' + str(@EventType)
	end
END
go
grant EXECUTE on usf2_JournalEventTypeInterpretation to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2019
--
-- Описание:
--
--		Журнал событий
--
-- ======================================================================================
create view [dbo].[vw_GlobalJournals] 
WITH SCHEMABINDING
AS
	select 
	arch.ID
	,arch.TypeHierarchy
	,arch.EventSource
	,arch.EventSourceString
	,arch.EventDateTime
	,arch.EventName  
	,arch.EventCode
	,arch.ExtendetEventName
	,arch.ExtendedEventCode 
	,arch.EventData
	,arch.EventString
	,arch.EventData2
	,arch.EventString2
	,arch.Status
	,DispatchDateTime
	,arch.JournalFilter
	,arch.SerialNumber  
	,arch.PS_ID
	,arch.PsName 
	,arch.StartDateTime
	,arch.FinishDateTime
	,arch.[User_ID]
	,u.UserFullName 
	from 
	(
		--1. Собираем события ТИ (ArchBit_Events_Journal_X) 
		select arch.ID, CAST(4 as tinyint) as TypeHierarchy, CAST(null as int) as EventSource
		, arch.EventDateTime, EventName, arch.EventCode, ExtendetEventName, arch.ExtendedEventCode 
		, CAST(null as float)as EventData, cast(null as nvarchar(max)) as EventString
		, CAST(null as float)as EventData2, cast(null as nvarchar(max)) as EventString2, cast(null as int) as Status, DispatchDateTime, CAST(2 as tinyint) as JournalFilter
		, CAST(null as datetime) as StartDateTime, CAST(null as datetime) as FinishDateTime, CAST(null as varchar(22)) as [User_ID]
		, PS_ID, PsName, SerialNumber, '' as IPMain, Meter_ID
		, EventSourceString
		from dbo.vw_GlobalJournalTi arch 
					
		union all

		--2. Добаляем события и коррекцию времени Е422 и УСПД
		select uspds.ID, TypeHierarchy, CAST(null as int) as EventSource
		, EventDateTime, EventName, EventCode, CAST(null as nvarchar(max)) as ExtendetEventName, CAST(null as bigint)  as ExtendedEventCode
		, EventData,EventString
		, EventData2, EventString2, Status, DispatchDateTime, JournalFilter
		, CAST(null as datetime) as StartDateTime, CAST(null as datetime) as FinishDateTime, CAST(null as varchar(22)) as [User_ID]
		, uspds.PS_ID, ps.StringName as PsName, SerialNumber, IPMain, -1 as Meter_ID
		, ISNULL(ps.StringName, '') + ', ' + uspds.IPMain as EventSourceString
		from dbo.vw_GlobalJournalE422_USPD uspds 
		join dbo.Dict_PS ps on ps.PS_ID = uspds.PS_ID
	
		union all

		--3. Ошибки приложений (JournalApplication_ErrorLog)
		select CAST(-1 as int) as ID, CAST(255 as tinyint) as TypeHierarchy, CAST(ApplicationType as int) as EventSource, EventDateTime, 
			dbo.usf2_JournalEventTypeInterpretation(cast([EventType] as tinyint)) as EventName,
			EventCode,  HostName as ExtendetEventName
			, CAST(null as bigint) as ExtendedEventCode, CAST(null as float) as [EventData], EventString, CAST(null as float) as EventData2, EventParams as EventString2, CAST(EventType as int) as [Status]
			, CAST(null as datetime) as DispatchDateTime, CAST(4 as tinyint) as JournalFilter
			, CAST(null as datetime) as StartDateTime, CAST(null as datetime) as FinishDateTime, CAST(null as varchar(22)) as [User_ID]
			, -1 as PS_ID, '' as PsName, '' as SerialNumber, '' as IPMain, -1 as Meter_ID
			, dbo.usf2_JournalApplicationTypeInterpretation(ApplicationType) as EventSourceString
		from dbo.JournalApplication_ErrorLog sgj with (nolock) where CUS_ID = 0 --and ApplicationType >= 0

	
		union all

		--4. Журнал системы сбора данных (JournalDataCollect_ServersGlobalJournal)
		select CAST(-1 as int) as ID, CAST(255 as tinyint) as TypeHierarchy, EventSource, EventDateTime
		, dbo.usf2_ServersGlobalJournalCodeInterpretation(cast([EventCode] as tinyint)) as EventName, EventCode, CAST(null as nvarchar(128)) as ExtendetEventName 
		, ClockDiff as ExtendedEventCode, ErrorCode as [EventData], EventString, CAST(null as float) as EventData2 
		, ErrorString as EventString2, EventCode as [Status], CAST(null as datetime) as DispatchDateTime, CAST(8 as tinyint) as JournalFilter
		, CAST(null as datetime) as StartDateTime, CAST(null as datetime) as FinishDateTime, CAST(null as varchar(22)) as [User_ID]
		, -1 as PS_ID, '' as PsName, '' as SerialNumber, '' as IPMain, -1 as Meter_ID
		, dbo.usf2_ServersGlobalJournalSourceInterpretation(EventSource) as EventSourceString
		from dbo.JournalDataCollect_ServersGlobalJournal sgj with (nolock)
	
		union all

		--5. Журнал действий пользователей над обходными выключателями (Expl_User_Journal_OV_Positions_Change)
		select position.TI_ID as ID, CAST(4 as tinyint) as TypeHierarchy 
		, CAST(null as int) as EventSource, p.EventDateTime 
		, dbo.usf2_JournalOvEventTypeInterpretation(cast([EventType] as tinyint)) as EventName, CAST(EventType as int) as EventCode, ol.OVName as ExtendetEventName 
		, position.TI_ID as ExtendedEventCode, CAST(null as float) as [EventData], p.Comment as EventString, CAST(null as float) as EventData2 
		, CAST(null as nvarchar(max)) as EventString2, CAST(EventType as int) as [Status], CAST(null as datetime) as DispatchDateTime, CAST(16 as tinyint) as JournalFilter
		, StartDateTime, FinishDateTime, p.User_ID
		, ti.PS_ID, ps.StringName as PsName, ms.MeterSerialNumber as SerialNumber, '' as IPMain, ms.Meter_ID
		, TIName as EventSourceString
		from dbo.Expl_User_Journal_OV_Positions_Change p with (nolock)
		join dbo.Hard_OV_List ol on ol.OV_ID = p.OV_ID
		join [dbo].[Hard_OV_Positions_List] position on p.OV_ID = p.OV_ID
		join dbo.Info_TI ti on ti.TI_ID = position.TI_ID
		join dbo.Dict_PS ps on ps.PS_ID = ti.PS_ID
		outer apply 
		(
			select top 1 hm.MeterSerialNumber, hm.Meter_ID from dbo.Info_Meters_TO_TI imtt with (nolock)
			join dbo.Hard_Meters hm with (nolock) on hm.Meter_ID = imtt.METER_ID
			where imtt.TI_ID = position.TI_ID and StartDateTime <= p.EventDateTime and (FinishDateTime is null or FinishDateTime >= p.EventDateTime)
			order by StartDateTime desc
		) ms

	) arch

	left join dbo.Expl_Users u with (nolock) on u.User_ID = arch.User_ID
GO
grant select on vw_GlobalJournals to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2017
--
-- Описание:
--
--		Глобальный журнал событий
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_Journals_Global]
	@requestedObjects RequestSectionParamsType READONLY, --Перечень объектов
	@DTStart DateTime,
	@DTEnd DateTime,
	@UseDictionary bit,
	@TopLimit int = 100000,
	@GlobalJournalFilter tinyint = 255
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Пока корректируем время на 4 месяца (если больше то тормоза)
if (DATEDIFF(month, @DTStart, @DTEnd) > 6 AND (@GlobalJournalFilter & 2) <> 0) set @DTStart = DATEADD(month, -6, @DTEnd)

--Запрос с фильтром объектов
if (exists (select top 1 1 from @requestedObjects)) begin

select top(@TopLimit) a.* from [dbo].[vw_GlobalJournals] a with (nolock) 
	join @requestedObjects r on r.ID = a.ID and r.TypeHierarchy = a.TypeHierarchy 
	--Объединяем с обобщенными журналами (без идентификаторов объектов)
	where EventDateTime between @DTStart and @DTEnd
		AND (@GlobalJournalFilter & JournalFilter) <> 0
		--AND ((a.JournalFilter = 4) or (a.JournalFilter = 8) OR r.ID is not null)
order by EventDateTime desc

end else begin 

--Запрос без фильтра объектов
	select top(@TopLimit) a.* from dbo.vw_GlobalJournals a with (nolock)
	--join @requestedObjectsLocal r on r.ID = a.ID and r.TypeHierarchy = a.TypeHierarchy
	--where PSName like '%ПНС%'
	where EventDateTime between @DTStart and @DTEnd AND (@GlobalJournalFilter & JournalFilter) <> 0
	--order by EventDateTime desc

end
end
go
   grant EXECUTE on usp2_ArchComm_Journals_Global to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
--		Привязываем объекты к аварии
--
-- ======================================================================================

create proc [dbo].[usp2_Alarm_LinkWithObjects]
(	
	@objectIds RequestSectionParamsType READONLY, --Идентификаторы объектов, которые требуется привязать
	@WorkflowActivity_ID int,
	@AlarmSetting_ID int,
	@User_ID varchar(22),
	@CreateDateTime DateTime,
	@CUS_ID tinyint
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	declare @insertedObjectIds RequestSectionParamsType;

	BEGIN TRY  BEGIN TRANSACTION

	declare @TypeHierarchy tinyint;
	declare t cursor local FAST_FORWARD for select distinct TypeHierarchy from @objectIds

	open t;
	FETCH NEXT FROM t into @TypeHierarchy
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF (@TypeHierarchy = 4) BEGIN
		--ТИ------------------------------------------

			MERGE [dbo].[Alarms_TI_To_Activity] as a USING
			(
				select ID from @objectIds where TypeHierarchy = @TypeHierarchy
			) n
			ON a.WorkflowActivity_ID = @WorkflowActivity_ID and a.TI_ID = n.ID and a.User_ID = @User_ID
			WHEN NOT MATCHED THEN 
			INSERT (WorkflowActivity_ID, TI_ID, User_ID, AlarmSetting_ID, CreateDateTime, CUS_ID)
			VALUES (@WorkflowActivity_ID, ID, @User_ID, @AlarmSetting_ID, @CreateDateTime, @CUS_ID)
			OUTPUT Inserted.TI_ID, @TypeHierarchy, NULL, NULL, 0, NULL, NULL INTO @insertedObjectIds;

		END ELSE IF (@TypeHierarchy = 3) BEGIN 
		--ПС------------------------------------------

			MERGE [dbo].[Alarms_PS_To_Activity] as a USING
			(
				select ID from @objectIds where TypeHierarchy = @TypeHierarchy
			) n
			ON a.WorkflowActivity_ID = @WorkflowActivity_ID and a.PS_ID = n.ID and a.User_ID = @User_ID
			WHEN NOT MATCHED THEN 
			INSERT (WorkflowActivity_ID, PS_ID, User_ID, AlarmSetting_ID, CreateDateTime, CUS_ID)
			VALUES (@WorkflowActivity_ID, ID, @User_ID, @AlarmSetting_ID, @CreateDateTime, @CUS_ID)
			OUTPUT Inserted.PS_ID, @TypeHierarchy, NULL, NULL, 0, NULL, NULL INTO @insertedObjectIds;

		END ELSE IF (@TypeHierarchy = 11) BEGIN 
		--Формула------------------------------------------

			MERGE [dbo].[Alarms_Formula_To_Activity] as a USING
			(
				select StringId from @objectIds where TypeHierarchy = @TypeHierarchy
			) n
			ON a.WorkflowActivity_ID = @WorkflowActivity_ID and a.Formula_UN = n.StringId and a.User_ID = @User_ID
			WHEN NOT MATCHED THEN 
			INSERT (WorkflowActivity_ID, Formula_UN, User_ID, AlarmSetting_ID, CreateDateTime, CUS_ID)
			VALUES (@WorkflowActivity_ID, StringId, @User_ID, @AlarmSetting_ID, @CreateDateTime, @CUS_ID)
			OUTPUT 0, @TypeHierarchy, NULL, NULL, 0, Inserted.Formula_UN, NULL INTO @insertedObjectIds;

		END ELSE IF (@TypeHierarchy = 44) BEGIN 
		--Баланс ПС------------------------------------------

			MERGE [dbo].[Alarms_Balance_PS_To_Activity] as a USING
			(
				select StringId from @objectIds where TypeHierarchy = @TypeHierarchy
			) n
			ON a.WorkflowActivity_ID = @WorkflowActivity_ID and a.BalancePS_UN = n.StringId and a.User_ID = @User_ID
			WHEN NOT MATCHED THEN 
			INSERT (WorkflowActivity_ID, BalancePS_UN, User_ID, AlarmSetting_ID, CreateDateTime, CUS_ID)
			VALUES (@WorkflowActivity_ID, StringId, @User_ID, @AlarmSetting_ID, @CreateDateTime, @CUS_ID)
			OUTPUT 0, @TypeHierarchy, NULL, NULL, 0, Inserted.BalancePS_UN, NULL INTO @insertedObjectIds;
		
		END ELSE IF (@TypeHierarchy = 45) BEGIN 
		--Баланс свободной ------------------------------------------

			MERGE [dbo].[Alarms_Balance_PS_To_Activity] as a USING
			(
				select StringId from @objectIds where TypeHierarchy = @TypeHierarchy
			) n
			ON a.WorkflowActivity_ID = @WorkflowActivity_ID and a.BalancePS_UN = n.StringId and a.User_ID = @User_ID
			WHEN NOT MATCHED THEN 
			INSERT (WorkflowActivity_ID, BalancePS_UN, User_ID, AlarmSetting_ID, CreateDateTime, CUS_ID)
			VALUES (@WorkflowActivity_ID, StringId, @User_ID, @AlarmSetting_ID, @CreateDateTime, @CUS_ID)
			OUTPUT 0, @TypeHierarchy, NULL, NULL, 0, Inserted.BalancePS_UN, NULL INTO @insertedObjectIds;

		END ELSE IF (@TypeHierarchy = 25) BEGIN --Площадка сбора по 61968

			MERGE [dbo].[Alarms_Master61968_SlaveSystems_To_Activity] as a USING
			(
				select ID from @objectIds where TypeHierarchy = @TypeHierarchy
			) n
			ON a.WorkflowActivity_ID = @WorkflowActivity_ID and a.Slave61968System_ID = n.ID and a.User_ID = @User_ID
			WHEN NOT MATCHED THEN 
			INSERT (WorkflowActivity_ID, Slave61968System_ID, User_ID, AlarmSetting_ID, CreateDateTime, CUS_ID)
			VALUES (@WorkflowActivity_ID, ID, @User_ID, @AlarmSetting_ID, @CreateDateTime, @CUS_ID)
			OUTPUT Inserted.Slave61968System_ID, @TypeHierarchy, NULL, NULL, 0, NULL, NULL INTO @insertedObjectIds;

		END

	
		FETCH NEXT FROM t into @TypeHierarchy
	END;
	CLOSE t
	DEALLOCATE t

	COMMIT	
	END TRY
	BEGIN CATCH
	--Ошибка, откатываем все изменения
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 10 
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH

	--Возвращаем объекты, которые уже есть в таблицах
	select case when ID < 0 then 0 else ID end as ID, TypeHierarchy, StringId from @objectIds 
	except select ID, TypeHierarchy, StringId from @insertedObjectIds 

END

go
   grant EXECUTE on usp2_Alarm_LinkWithObjects to [UserCalcService]
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
--		Стром путь к руту по списку объектов
--
-- ======================================================================================
CREATE proc [dbo].[usp2_FreeHierarchy_GetPath]

	@objectIds RequestSectionParamsType readonly, 
	@treeID int = null --Идентификатор дерева на котором ищем (если нужно)
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Строим кэш, если отсутствет
--exec usp2_FreeHierarchy_UpdateIncludedObjectChildren @treeID, 0

	--Путь по стандартным деревьям
	--Запрашиваем из кэша
	--По целочисленным идентификаторам
	select d.ToParentFreeHierPath as [Path] from [dbo].[Dict_FreeHierarchyIncludedObjectChildren] d
	join @objectIds o on o.TypeHierarchy = d.TypeHierarchy and o.ID is not null and o.ID = d.ID
	where FreeHierTree_ID = @treeID and d.ID is not null

	union all

	--По строковым
	select d.ToParentFreeHierPath as [Path] from [dbo].[Dict_FreeHierarchyIncludedObjectChildren] d
	join @objectIds o on o.TypeHierarchy = d.TypeHierarchy 
	and o.StringId is not null and (o.StringId = d.StringId or o.StringId = d.MeterSerialNumber)
	where FreeHierTree_ID = @treeID and (d.StringId is not null or d.MeterSerialNumber is not null)

end

go
   grant EXECUTE on usp2_FreeHierarchy_GetPath to [UserCalcService]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2019
--
-- Описание:
--
--		Обновляем информацию в таблице-кэш Dict_FreeHierarchyIncludedObjectChildren
--
-- ======================================================================================
CREATE proc [dbo].[usp2_FreeHierarchy_UpdateFreeHierCach]
(
@treeID int, --Идентификатор дерева
@objectIds RequestSectionParamsType readonly --Список объектов, по которым обновляем информацию
)
as
begin
	set nocount on
	
	if (@treeID < 0) begin 
		--Деревья свободной иерархии и подгружаемые InclideObject

	--end else begin

		--Стандартные деревья
		merge [dbo].[Dict_FreeHierarchyIncludedObjectChildren] as d
		using (
			select cast([dbo].[usf2_FreeHierarchy_GetPath](case when ids.ID is null or ids.ID <= 0 then ids.StringId else ltrim(str(ids.ID,15)) end, ids.TypeHierarchy, ids.FreeHierItemId, @treeID) as nvarchar(1000)) as ToParentFreeHierPath,
			ids.FreeHierItemId, '/' as ParentHierID, ids.TypeHierarchy,
			case when ids.ID > 0 then ids.ID else null end as ID, case when ids.StringId <> '' then ids.StringId else null end as StringId,  
			StringName, MeterSerialNumber, FreeHierItemType, 
			case when ParentID > 0 then ParentID else null end as ParentID, case when ParentTypeHierarchy > 0 then ParentTypeHierarchy else null end as ParentTypeHierarchy  
			, ParentName, ParentFreeHierItemType, @treeID as FreeHierTree_ID
			from @objectIds ids
			cross apply dbo.usf2_FreeHierarchyStandartObject(ids.TypeHierarchy, ids.ID, ids.StringId) h
		) h
		on d.[FreeHierTree_ID] = h.[FreeHierTree_ID]
		and d.[ToParentFreeHierPath] = h.[ToParentFreeHierPath]
		when matched then update set [StringName] = h.[StringName], [MeterSerialNumber] = h.[MeterSerialNumber] -- Пока обновляем эти поля, т.к. они используются для поиска
		when not matched then insert
		([ToParentFreeHierPath]
		,[FreeHierTree_ID]
		,[ParentHierID]
		,[TypeHierarchy]
		,[ID]
		,[StringId]
		,[StringName]
		,[MeterSerialNumber]
		,[FreeHierItemType]
		,[ParentID]
		,[ParentTypeHierarchy]
		,[ParentName]
		,[ParentFreeHierItemType])
		values (h.[ToParentFreeHierPath]
		,h.[FreeHierTree_ID]
		,h.[ParentHierID]
		,h.[TypeHierarchy]
		,h.[ID]
		,h.[StringId]
		,h.[StringName]
		,h.[MeterSerialNumber]
		,h.[FreeHierItemType]
		,h.[ParentID]
		,h.[ParentTypeHierarchy]
		,h.[ParentName]
		,h.[ParentFreeHierItemType]);

	end
end
go
   grant EXECUTE on usp2_FreeHierarchy_UpdateFreeHierCach to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchy_UpdateFreeHierCach to UserDeclarator
go