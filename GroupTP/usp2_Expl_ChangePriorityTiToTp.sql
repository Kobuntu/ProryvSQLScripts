if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_ChangePriorityTiToTp')
          and type in ('P','PC'))
   drop procedure usp2_Expl_ChangePriorityTiToTp
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Monit_GetMetersInfoForMonitoring')
          and type in ('P','PC'))
   drop procedure usp2_Monit_GetMetersInfoForMonitoring
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_ChangePriorityList')
          and type in ('P','PC'))
   drop procedure usp2_Expl_ChangePriorityList
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchTech_Select')
          and type in ('P','PC'))
   drop procedure usp2_ArchTech_Select
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_ArchTechVirtual')
          and type in ('V'))
   drop view vw_ArchTechVirtual
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'IntPair' AND ss.name = N'dbo')
DROP TYPE [dbo].[IntPair]
-- Пересоздаем заново
CREATE TYPE [dbo].[IntPair] AS TABLE 
(
	Item1 int NOT NULL,
	Item2 int NOT NULL
)
go

grant EXECUTE on TYPE::IntPair to [UserCalcService]
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
--		Изменение приоритетов источников для связки ТИ-ТП по списку
--
-- ======================================================================================
create proc [dbo].[usp2_Expl_ChangePriorityTiToTp]
@isPeriodNotLimited bit, --Распределение на весь период начиная от указанного 
@Month tinyint, --Месяц
@DataSourceType tinyint = null, -- Источник, если не задан то делаем удаление приоритетов
@Year int, -- Год
@EventDateTime DateTime, -- Текущая дата/время операции
@User_Id varchar(255), -- Пользователь
@DataSourceName varchar(255) = null, -- Полное название нового источника
@Section_ID int = null, -- Сечение, если действие производится в рамках всего сечения
@TiTpArray IntPair readonly --Набор пар ТИ-ТП
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare 
@ti_id int,
@tp_id int,
@commentString varchar(255),
@eventString varchar(255),
@DataSource_id int,
@cus_id tinyint;

BEGIN TRY  BEGIN TRANSACTION

	if (@DataSourceType is not null) begin
		--Изменение источника по списку точек
		set @DataSource_id = (select top 1 DataSource_id from Expl_DataSource_List where DataSourceType = @DataSourceType);

		if (@DataSource_id is null) RAISERROR('Указанный тип источника не определен в БД!', 10, 1)

		CREATE TABLE #tmp(
			[TI_ID] int NOT NULL,
			[TP_ID] int NOT NULL,
			[Year] [int] NOT NULL,
			[Month] [tinyint] NOT NULL,
			[DataSource_ID] int NOT NULL,
		PRIMARY KEY CLUSTERED 
		(
			[TI_ID] ASC,
			[TP_ID] ASC,
			[Year] ASC,
			[Month] ASC
		)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON))

		if (@isPeriodNotLimited = 1) begin --Новый источник действует от указанной даты

			declare @_month tinyint, @_year int;
			select @_month = @Month, @_year = @Year;
			while (@_year <= 2030) begin
				while (@_month <= 12) begin
					insert into #tmp
					select Item1, Item2, @_year, @_month, @DataSource_id as DataSource_id
					from @TiTpArray
					set @_month += 1
				end;
				set @_month = 1;
				set @_year += 1;
			end;

		end else begin
			--Обновление, добавление привязки
			insert into #tmp
			select Item1, Item2, @Year as [Year], @Month, @DataSource_id
			from @TiTpArray
		end

		--select * from #tmp

		Merge Expl_DataSource_To_TI_TP as a
		using #tmp t 
		on a.TI_ID = t.TI_ID and a.TP_ID = t.TP_ID and a.[Year] = t.[Year] and a.[Month] = t.[Month]
		WHEN MATCHED THEN 
		UPDATE set DataSource_ID = t.DataSource_ID
		WHEN NOT MATCHED THEN 
		INSERT (TI_ID,TP_ID,[Year],[Month],DataSource_ID) values (t.TI_ID,t.TP_ID,t.[Year],t.[Month],t.DataSource_ID);

		drop table #tmp;

		set @eventString = 'Выбран новый приоритетный источник ' + @DataSourceName

	end else begin
		--Удаление привязки по списку точек
		delete from Expl_DataSource_To_TI_TP
		from Expl_DataSource_To_TI_TP titp
		inner join @TiTpArray a on titp.TI_ID = a.Item1 and titp.TP_ID = a.Item2 
		WHERE (@isPeriodNotLimited = 0 and titp.Year = @Year and titp.Month = @Month) --Удаление для конкретного периода
		or (@isPeriodNotLimited = 1 and (titp.Year * 12 + titp.Month) >= (@Year * 12 + @Month)) --Удаление от указанной даты

		set @eventString = 'Удаление приоритетного источника'
		
	end

	if (@isPeriodNotLimited = 1) set @eventString +=', начиная с '
	else set @eventString += ', расчетный период '
	set @eventString += DateName( month , DateAdd( month , @Month , 0 ) - 1 ) + ' ' + str(@Year,4,4);

	set @cus_id = (select top 1 CUS_ID from Dict_CUS);

	set @commentString = '';

	if (@Section_ID is not null) set @commentString += 'ГТП - ' + ISNULL((select sectionName from Info_Section_List where Section_ID = @Section_ID), '')
	else select @commentString += 'ТП - ' + tp.StringName + ', ТИ - ' + ti.TIName + ', '
		from @TiTpArray a
		join Info_TI ti on ti.TI_ID = a.Item1
		join Info_TP2 tp on tp.TP_ID = a.Item2

	--Обновление журнала действий
	insert Expl_User_Journal (ApplicationType, CommentString, CUS_ID, EventDateTime, EventString, [User_ID], ObjectName)
	values (1, @commentString, @cus_id, @eventDateTime, @eventString, @User_ID, 'Expl_DataSource_To_TI_TP')

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

end
go
   grant EXECUTE on usp2_Expl_ChangePriorityTiToTp to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2012
--
-- Описание:
--
--		Выбирает информацию по счетчикам для анализа мониторинга
--
-- ======================================================================================
create proc [dbo].[usp2_Monit_GetMetersInfoForMonitoring]
	@parents Intpair readonly,
	@datestart DateTime,
	@dateend DateTime, 
	@isConcentratorsEnabled bit,
	@tiType tinyint = null -- Тип ТИ по которым отсеиваем 
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
--Набираем идентификаторы исходя из родителя

	create table #metersArray
	(
		Parrent_ID int not null,
		ParrentMonitoringHierarchy tinyint not null,
		Meter_ID int null,
	);

	--@Parrent_ID int, -- идентификатор родителя для которого собираем счетчики
	--@ParrentMonitoringHierarchy tinyint, -- тип родителя для которого выбираем счетчики (E422 = 1, Concentrator = 2, USPD = 3, Slave61968System = 4)
	
	--Это 61968
	insert into #metersArray(Parrent_ID, Meter_ID, ParrentMonitoringHierarchy)
	select p.Item1, m.Meter_ID, p.Item2 
	from @parents p
	left join dbo.Master61968_SlaveSystems_EndDeviceAssets a on a.Slave61968System_ID = p.Item1
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_To_Meters m on m.Slave61968EndDeviceAsset_ID = a.Slave61968EndDeviceAsset_ID
	where Item2 = 4
	--УСПД
	union all
	select p.Item1, m.Meter_ID, p.Item2 
	from @parents p
	left join dbo.Hard_MetersUSPD_Links m on p.Item1 = m.USPD_ID
	where Item2 = 3
	--Концентратор
	union all
	select p.Item1, m.Meter_ID, p.Item2 
	from @parents p
	left join dbo.Hard_MetersE422_Links m on m.Concentrator_ID = p.Item1
	where Item2 = 2
	--Е422
	union all
	select p.Item1, m.Meter_ID, p.Item2 
	from @parents p
	left join dbo.Hard_MetersE422_Links m on m.E422_ID = p.Item1
	where Item2 = 1 and (isnull(@isConcentratorsEnabled, 0)=0 or (isnull(@isConcentratorsEnabled, 0)= 1 and m.Concentrator_ID is null))
	
	--Количество ожидаемых команд и ручных запросов
	select distinct ManageRequest_ID
	into #waitingRequestTable
	from dbo.Expl_User_Journal_ManagePU_Request_List rl
	where ManageRequestStatus < 100

	select a.Meter_ID, isnull((select Count(ManageRequest_ID) as WaitingCommandsCount from 
		dbo.DeviceManage_Manual_ReadRequest  WITH(NOLOCK)
		where ManageRequest_ID in (select distinct ManageRequest_ID from #waitingRequestTable) and Meter_ID = a.Meter_ID),0) 
		+ isnull((select Count(ManageRequest_ID) as WaitingCommandsCount from 
		dbo.DeviceManage_DeviceTo_CommandRequest WITH(NOLOCK)
		where ManageRequest_ID in (select distinct ManageRequest_ID from #waitingRequestTable) and Meter_ID = a.Meter_ID), 0) as WaitingCommandsCount
	into #waitingMeters from #metersArray a
	
	--Информация
	select m.Parrent_ID as ParentDbId, m.ParrentMonitoringHierarchy as ParentMonitoringHierarchyDB, hm.Meter_ID, MeterType_ID, MeterExtendedType_ID
		,imtt.TI_ID, StartDateTime, FinishDateTime, LinkNumber, MeterSerialNumber
		,ISNULL(w.WaitingCommandsCount, 0) as WaitingCommandsCount
		, usf.*
		,cast(ti.Commercial as bit) as IsCommercial
		,dbo.usf2_Info_GetTariffChannelsForTI(imtt.TI_ID, ISNULL(ti.AbsentChannelsMask, 0), case when ti.AIATSCode=2 then 1 else 0 end) as TariffChannels,
		ti.PS_ID
		from #metersArray m 
		left join Hard_Meters hm on hm.Meter_ID = m.Meter_ID
		outer apply 
				(
					select top (1) *
					from dbo.Info_Meters_TO_TI
					where METER_ID = hm.Meter_ID
						and StartDateTime <= @dateend 
						and (FinishDateTime is null or FinishDateTime >= @datestart)
					order by StartDateTime desc
				) imtt
		left join dbo.Info_TI ti on ti.TI_ID = imtt.TI_ID --Здесь если написать left join то будут отображаться счетчики без ТИ
		left join #waitingMeters w on w.Meter_ID = hm.Meter_ID
		cross apply usf2_Monit_GetMeterParams(hm.Meter_ID, m.Parrent_ID, m.ParrentMonitoringHierarchy, hm.MeterType_ID) usf
		where ISNULL(ti.Deleted, 0) <> 1 and (@tiType is null or (@tiType is not null and ti.TIType = @tiType))
		order by m.ParrentMonitoringHierarchy, m.Parrent_ID
	
	drop table #waitingRequestTable
	drop table #waitingMeters
	
end
go
   grant EXECUTE on usp2_Monit_GetMetersInfoForMonitoring to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2016
--
-- Описание:
--
--		Изменение приоритетов источников
--
-- ======================================================================================
create proc [dbo].[usp2_Expl_ChangePriorityList]
@isPeriodNotLimited bit, --Распределение на весь период начиная от указанного 
@Month tinyint, --Месяц
@Year int, -- Год
@EventDateTime DateTime, -- Текущая дата/время операции
@User_Id varchar(255), -- Пользователь
@DataSourceTypeToPriority IntPair readonly --Набор пар ТИ-ТП
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare 
@commentString varchar(255),
@eventString varchar(255),
@cus_id int


BEGIN TRY  BEGIN TRANSACTION

	--Источники по приоритетам
	create table #dsp
	(
		[Year] int, 
		[Month] tinyint,
		[DataSource_ID] int, 
		[Priority] int, 
		PRIMARY KEY CLUSTERED 
		(
			[Year] asc, 
			[Month] asc,
			[DataSource_ID] asc 
		)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
		)
	
	declare
	@dtStart DateTime,
	@dtEnd DateTime

	set @dtStart = DATEADD(month, @Month - 1, DATEADD(Year, @Year-1900, 0))
	
	if (@isPeriodNotLimited = 1) begin --Новый источник действует от указанной даты
		set @dtEnd = DATEADD(year, 20, @dtStart)
	end else begin
		set @dtEnd = @dtStart
	end

	insert into #dsp
	select YEAR(MonthYear) as [Year],MONTH(MonthYear) as [Month], DataSource_ID,[Priority] from 
	(
		select DataSource_ID, Item2 as [Priority]
		from @DataSourceTypeToPriority p
		join Expl_DataSource_List d on d.DataSourceType = p.Item1
	) p, [dbo].[usf2_Utils_MonthByPeriod](@dtStart, @dtEnd)

	--select * from #dsp

	Merge Expl_DataSource_PriorityList as a
	using #dsp t 
	on a.[Year] = t.[Year] and a.[Month] = t.[Month] and a.[DataSource_ID] = t.[DataSource_ID] 
	WHEN MATCHED THEN 
	UPDATE set [Priority] = t.[Priority]
	WHEN NOT MATCHED THEN 
	INSERT ([Year],[Month],[DataSource_ID], [Priority]) values (t.[Year],t.[Month],t.[DataSource_ID], t.[Priority]);

	drop table #dsp

	--drop table #tmp;

	set @eventString = 'Смена общих приоритетов источников, расчетный период c ' + DATENAME(month, DATEADD(month, @Month-1, CAST('2008-01-01' AS datetime))) + ' ' + str(@Year,4)


	if (@isPeriodNotLimited = 1) set @eventString +=', по ' + DATENAME(month, DATEADD(month, DATEPART(month, @dtEnd)-1, CAST('2008-01-01' AS datetime))) + ' ' + str(DATEPART(year, @dtEnd),4)
	--else set @eventString += ', расчетный период '
	--set @eventString += DateName( month , DateAdd( month , @Month , 0 ) - 1 ) + ' ' + str(@Year,4,4);

	set @cus_id = (select top 1 CUS_ID from Dict_CUS);

	set @commentString = 'Источники: ';
	select @commentString += str(Item1,2) + ',' from @DataSourceTypeToPriority

	--if (@Section_ID is not null) set @commentString += 'ГТП - ' + ISNULL((select sectionName from Info_Section_List where Section_ID = @Section_ID), '')
	--else select @commentString += 'ТП - ' + tp.StringName + ', ТИ - ' + ti.TIName + ', '
	--	from @TiTpArray a
	--	join Info_TI ti on ti.TI_ID = a.Item1
	--	join Info_TP2 tp on tp.TP_ID = a.Item2

	--Обновление журнала действий
	insert Expl_User_Journal (ApplicationType, CommentString, CUS_ID, EventDateTime, EventString, [User_ID], ObjectName)
	values (1, @commentString, @cus_id, @eventDateTime, @eventString, @User_ID, 'Expl_DataSource_PriorityList')


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

end

go
   grant EXECUTE on usp2_Expl_ChangePriorityList to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2019
--
-- Описание:
--
--		Технический профиль ТИ (минутки, 2х, 3х, 5, 10, 15)
--
-- ======================================================================================
CREATE view [dbo].[vw_ArchTechVirtual] 
WITH SCHEMABINDING
AS
	select TI_ID, EventDate, ChannelType, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48,val_49,val_50, 
	val_51,val_52,val_53,val_54,val_55,val_56,val_57,val_58,val_59,val_60, 
	ValidStatus, DispatchDateTime, [Status],
	1 as TechProfilePeriod
	from dbo.ArchTech_1Min_Values a with (nolock) 

	union all

	select TI_ID, EventDate, ChannelType, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	NULL as val_31,NULL as val_32,NULL as val_33,NULL as val_34,NULL as val_35,NULL as val_36,NULL as val_37,NULL as val_38,NULL as val_39,NULL as val_40,
	NULL as val_41,NULL as val_42,NULL as val_43,NULL as val_44,NULL as val_45,NULL as val_46,NULL as val_47,NULL as val_48,NULL as val_49,NULL as val_50, 
	NULL as val_51,NULL as val_52,NULL as val_53,NULL as val_54,NULL as val_55,NULL as val_56,NULL as val_57,NULL as val_58,NULL as val_59,NULL as val_60,
	ValidStatus, DispatchDateTime, [Status],
	2 as TechProfilePeriod
	from dbo.ArchTech_2Min_Values a with (nolock) 

	union all

	select TI_ID, EventDate, ChannelType, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	NULL as val_21,NULL as val_22,NULL as val_23,NULL as val_24,NULL as val_25,NULL as val_26,NULL as val_27,NULL as val_28,NULL as val_29,NULL as val_30,
	NULL as val_31,NULL as val_32,NULL as val_33,NULL as val_34,NULL as val_35,NULL as val_36,NULL as val_37,NULL as val_38,NULL as val_39,NULL as val_40,
	NULL as val_41,NULL as val_42,NULL as val_43,NULL as val_44,NULL as val_45,NULL as val_46,NULL as val_47,NULL as val_48,NULL as val_49,NULL as val_50, 
	NULL as val_51,NULL as val_52,NULL as val_53,NULL as val_54,NULL as val_55,NULL as val_56,NULL as val_57,NULL as val_58,NULL as val_59,NULL as val_60,
	ValidStatus, DispatchDateTime, [Status],
	3 as TechProfilePeriod
	from dbo.ArchTech_3Min_Values a with (nolock) 

	union all

	select TI_ID, EventDate, ChannelType, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,NULL as val_13,NULL as val_14,NULL as val_15,NULL as val_16,NULL as val_17,NULL as val_18,NULL as val_19,NULL as val_20,
	NULL as val_21,NULL as val_22,NULL as val_23,NULL as val_24,NULL as val_25,NULL as val_26,NULL as val_27,NULL as val_28,NULL as val_29,NULL as val_30,
	NULL as val_31,NULL as val_32,NULL as val_33,NULL as val_34,NULL as val_35,NULL as val_36,NULL as val_37,NULL as val_38,NULL as val_39,NULL as val_40,
	NULL as val_41,NULL as val_42,NULL as val_43,NULL as val_44,NULL as val_45,NULL as val_46,NULL as val_47,NULL as val_48,NULL as val_49,NULL as val_50, 
	NULL as val_51,NULL as val_52,NULL as val_53,NULL as val_54,NULL as val_55,NULL as val_56,NULL as val_57,NULL as val_58,NULL as val_59,NULL as val_60,
	ValidStatus, DispatchDateTime, [Status],
	5 as TechProfilePeriod
	from dbo.ArchTech_5Min_Values a with (nolock) 

	union all

	select TI_ID, EventDate, ChannelType, 
	val_01,val_02,val_03,val_04,val_05,val_06,NULL as val_07,NULL as val_08,NULL as val_09,NULL as val_10,
	NULL as val_11,NULL as val_12,NULL as val_13,NULL as val_14,NULL as val_15,NULL as val_16,NULL as val_17,NULL as val_18,NULL as val_19,NULL as val_20,
	NULL as val_21,NULL as val_22,NULL as val_23,NULL as val_24,NULL as val_25,NULL as val_26,NULL as val_27,NULL as val_28,NULL as val_29,NULL as val_30,
	NULL as val_31,NULL as val_32,NULL as val_33,NULL as val_34,NULL as val_35,NULL as val_36,NULL as val_37,NULL as val_38,NULL as val_39,NULL as val_40,
	NULL as val_41,NULL as val_42,NULL as val_43,NULL as val_44,NULL as val_45,NULL as val_46,NULL as val_47,NULL as val_48,NULL as val_49,NULL as val_50, 
	NULL as val_51,NULL as val_52,NULL as val_53,NULL as val_54,NULL as val_55,NULL as val_56,NULL as val_57,NULL as val_58,NULL as val_59,NULL as val_60,
	ValidStatus, DispatchDateTime, [Status],
	10 as TechProfilePeriod
	from dbo.ArchTech_10Min_Values a with (nolock) 

	union all

	select TI_ID, EventDate, ChannelType, 
	val_01,val_02,val_03,val_04,NULL as val_05,NULL as val_06,NULL as val_07,NULL as val_08,NULL as val_09,NULL as val_10,
	NULL as val_11,NULL as val_12,NULL as val_13,NULL as val_14,NULL as val_15,NULL as val_16,NULL as val_17,NULL as val_18,NULL as val_19,NULL as val_20,
	NULL as val_21,NULL as val_22,NULL as val_23,NULL as val_24,NULL as val_25,NULL as val_26,NULL as val_27,NULL as val_28,NULL as val_29,NULL as val_30,
	NULL as val_31,NULL as val_32,NULL as val_33,NULL as val_34,NULL as val_35,NULL as val_36,NULL as val_37,NULL as val_38,NULL as val_39,NULL as val_40,
	NULL as val_41,NULL as val_42,NULL as val_43,NULL as val_44,NULL as val_45,NULL as val_46,NULL as val_47,NULL as val_48,NULL as val_49,NULL as val_50, 
	NULL as val_51,NULL as val_52,NULL as val_53,NULL as val_54,NULL as val_55,NULL as val_56,NULL as val_57,NULL as val_58,NULL as val_59,NULL as val_60,
	ValidStatus, DispatchDateTime, [Status],
	15 as TechProfilePeriod
	from dbo.ArchTech_15Min_Values a with (nolock) 
	
GO

   grant select on vw_ArchTechVirtual to [UserCalcService]
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
--		Выбирает тех. архивы
--
-- ======================================================================================
create proc [dbo].[usp2_ArchTech_Select]

	@TIArray IntPair readonly, --ТИ, канал, признак стороны, идентификатор закрытого периода, если необходимо читать из закрытого периода
	@DTStart DateTime,
	@DTEnd DateTime,
	@UseCoeffTransformation bit = 1, --Использовать коэфф. трансформации
	@UseLossesCoefficient bit = 0, --Использовать ли коэфф. потерь для ТИ
	@techProfilePeriod tinyint = null -- Профиль, по которому необходимо запросить данные
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
	
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--1 результат Таблица с коэфф. потерь для ТИ
if (@UseLossesCoefficient = 1) begin
	SELECT ti.Item1 as TI_ID, cast(ti.Item2 as tinyint) as ChannelType, c.[StartDateTime],c.[FinishDateTime],c.[LossesCoefficient]
	FROM @TIArray ti
	cross apply 
	(
		select top 1 * from Info_TI_LossesCoefficients c where c.TI_ID = ti.Item1
			and c.StartDateTime <= @DTEnd and (c.FinishDateTime is null or (c.FinishDateTime is not null and c.FinishDateTime>= @DTStart))
		order by c.StartDateTime desc
	) c
	order by Item1, Item2, StartDateTime
end

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 2 результат (Данные по времени действия коэффициента трансформации)
if (@UseCoeffTransformation = 1) begin
	select ti.Item1 as TI_ID, cast(ti.Item2 as tinyint) as ChannelType, COEFU*COEFI as Coeff, 
	it.StartDateTime, it.FinishDateTime
	from @TIArray ti
	cross apply 
	(
		select top 1 * from dbo.Info_Transformators it where it.TI_ID = ti.Item1
			and it.StartDateTime <= @DTEnd and (it.FinishDateTime is null or ( it.FinishDateTime is not null and it.FinishDateTime >= @DTStart))
		order by it.StartDateTime desc
	) it
	order by Item1, Item2, StartDateTime
end

---- 3 результат минутные архивы
select * from @TIArray ti
join [dbo].[vw_ArchTechVirtual] a 
on (@techProfilePeriod is null or a.TechProfilePeriod = @techProfilePeriod)
and a.TI_ID = ti.Item1 and a.ChannelType = ti.Item2
and EventDate between @DTStart and @DTEnd
order by TI_ID, ChannelType, EventDate

end
go
   grant EXECUTE on usp2_ArchTech_Select to [UserCalcService]
go