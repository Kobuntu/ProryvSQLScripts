if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_WriteArchive30')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_WriteArchive30
go

--Создаем тип, если его нет
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ForecastWriterArchive30ValuesType' AND ss.name = N'dbo') 
DROP TYPE [dbo].[ForecastWriterArchive30ValuesType]

CREATE TYPE [dbo].[ForecastWriterArchive30ValuesType] AS TABLE(
	[ForecastObject_UN] varchar(22) NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[Priority] [tinyint] NULL,
	--[ForecastArchiveJournal_UN] uniqueidentifier NOT NULL,
	[ForecastCalculateModel_ID] int NOT NULL,
	[User_ID] varchar(22) NOT NULL,
	[DispatchDateTime] DateTime NOT NULL,
	[Comment] nvarchar(1000) NULL,
	[AUTOBITS] [bigint],
	[MANUALBITS] [bigint],
	[FACTBITS] [bigint],
	[AUTO_01] [float] NULL,
	[AUTO_02] [float] NULL,
	[AUTO_03] [float] NULL,
	[AUTO_04] [float] NULL,
	[AUTO_05] [float] NULL,
	[AUTO_06] [float] NULL,
	[AUTO_07] [float] NULL,
	[AUTO_08] [float] NULL,
	[AUTO_09] [float] NULL,
	[AUTO_10] [float] NULL,
	[AUTO_11] [float] NULL,
	[AUTO_12] [float] NULL,
	[AUTO_13] [float] NULL,
	[AUTO_14] [float] NULL,
	[AUTO_15] [float] NULL,
	[AUTO_16] [float] NULL,
	[AUTO_17] [float] NULL,
	[AUTO_18] [float] NULL,
	[AUTO_19] [float] NULL,
	[AUTO_20] [float] NULL,
	[AUTO_21] [float] NULL,
	[AUTO_22] [float] NULL,
	[AUTO_23] [float] NULL,
	[AUTO_24] [float] NULL,
	[AUTO_25] [float] NULL,
	[AUTO_26] [float] NULL,
	[AUTO_27] [float] NULL,
	[AUTO_28] [float] NULL,
	[AUTO_29] [float] NULL,
	[AUTO_30] [float] NULL,
	[AUTO_31] [float] NULL,
	[AUTO_32] [float] NULL,
	[AUTO_33] [float] NULL,
	[AUTO_34] [float] NULL,
	[AUTO_35] [float] NULL,
	[AUTO_36] [float] NULL,
	[AUTO_37] [float] NULL,
	[AUTO_38] [float] NULL,
	[AUTO_39] [float] NULL,
	[AUTO_40] [float] NULL,
	[AUTO_41] [float] NULL,
	[AUTO_42] [float] NULL,
	[AUTO_43] [float] NULL,
	[AUTO_44] [float] NULL,
	[AUTO_45] [float] NULL,
	[AUTO_46] [float] NULL,
	[AUTO_47] [float] NULL,
	[AUTO_48] [float] NULL,
	[MANUAL_01] [float] NULL,
	[MANUAL_02] [float] NULL,
	[MANUAL_03] [float] NULL,
	[MANUAL_04] [float] NULL,
	[MANUAL_05] [float] NULL,
	[MANUAL_06] [float] NULL,
	[MANUAL_07] [float] NULL,
	[MANUAL_08] [float] NULL,
	[MANUAL_09] [float] NULL,
	[MANUAL_10] [float] NULL,
	[MANUAL_11] [float] NULL,
	[MANUAL_12] [float] NULL,
	[MANUAL_13] [float] NULL,
	[MANUAL_14] [float] NULL,
	[MANUAL_15] [float] NULL,
	[MANUAL_16] [float] NULL,
	[MANUAL_17] [float] NULL,
	[MANUAL_18] [float] NULL,
	[MANUAL_19] [float] NULL,
	[MANUAL_20] [float] NULL,
	[MANUAL_21] [float] NULL,
	[MANUAL_22] [float] NULL,
	[MANUAL_23] [float] NULL,
	[MANUAL_24] [float] NULL,
	[MANUAL_25] [float] NULL,
	[MANUAL_26] [float] NULL,
	[MANUAL_27] [float] NULL,
	[MANUAL_28] [float] NULL,
	[MANUAL_29] [float] NULL,
	[MANUAL_30] [float] NULL,
	[MANUAL_31] [float] NULL,
	[MANUAL_32] [float] NULL,
	[MANUAL_33] [float] NULL,
	[MANUAL_34] [float] NULL,
	[MANUAL_35] [float] NULL,
	[MANUAL_36] [float] NULL,
	[MANUAL_37] [float] NULL,
	[MANUAL_38] [float] NULL,
	[MANUAL_39] [float] NULL,
	[MANUAL_40] [float] NULL,
	[MANUAL_41] [float] NULL,
	[MANUAL_42] [float] NULL,
	[MANUAL_43] [float] NULL,
	[MANUAL_44] [float] NULL,
	[MANUAL_45] [float] NULL,
	[MANUAL_46] [float] NULL,
	[MANUAL_47] [float] NULL,
	[MANUAL_48] [float] NULL,
	[FACT_01] [float] NULL,
	[FACT_02] [float] NULL,
	[FACT_03] [float] NULL,
	[FACT_04] [float] NULL,
	[FACT_05] [float] NULL,
	[FACT_06] [float] NULL,
	[FACT_07] [float] NULL,
	[FACT_08] [float] NULL,
	[FACT_09] [float] NULL,
	[FACT_10] [float] NULL,
	[FACT_11] [float] NULL,
	[FACT_12] [float] NULL,
	[FACT_13] [float] NULL,
	[FACT_14] [float] NULL,
	[FACT_15] [float] NULL,
	[FACT_16] [float] NULL,
	[FACT_17] [float] NULL,
	[FACT_18] [float] NULL,
	[FACT_19] [float] NULL,
	[FACT_20] [float] NULL,
	[FACT_21] [float] NULL,
	[FACT_22] [float] NULL,
	[FACT_23] [float] NULL,
	[FACT_24] [float] NULL,
	[FACT_25] [float] NULL,
	[FACT_26] [float] NULL,
	[FACT_27] [float] NULL,
	[FACT_28] [float] NULL,
	[FACT_29] [float] NULL,
	[FACT_30] [float] NULL,
	[FACT_31] [float] NULL,
	[FACT_32] [float] NULL,
	[FACT_33] [float] NULL,
	[FACT_34] [float] NULL,
	[FACT_35] [float] NULL,
	[FACT_36] [float] NULL,
	[FACT_37] [float] NULL,
	[FACT_38] [float] NULL,
	[FACT_39] [float] NULL,
	[FACT_40] [float] NULL,
	[FACT_41] [float] NULL,
	[FACT_42] [float] NULL,
	[FACT_43] [float] NULL,
	[FACT_44] [float] NULL,
	[FACT_45] [float] NULL,
	[FACT_46] [float] NULL,
	[FACT_47] [float] NULL,
	[FACT_48] [float] NULL
)
go

grant EXECUTE on TYPE::ForecastWriterArchive30ValuesType to [UserCalcService]
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
--		Январь, 2017
--
-- Описание:
--
--		Пишем таблицу 30 минуток прогнозируемого значения в БД
--
-- ======================================================================================
create proc [dbo].[usp2_Forecast_WriteArchive30]
	
	@forecastWriterArchive30Table ForecastWriterArchive30ValuesType READONLY, --Таблицу которую пишем в базу данных
	@isForceFix bit = 0,
	@treeID int = null
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

--Проверка всех разрешений
declare @forecastWriteTable ForecastWriterType;
insert into @forecastWriteTable
select distinct ForecastObject_UN, EventDate, User_ID, DispatchDateTime, AUTOBITS, MANUALBITS, FACTBITS  from @forecastWriterArchive30Table

declare @objectUnRight nvarchar(max);
set @objectUnRight = (select dbo.usf2_Forecast_UserHasRight_ForecastFix(@forecastWriteTable,'CBBB7316-9875-44B8-9178-E7B6603B6C73', @treeID))

if (len(@objectUnRight) > 1) begin
	set @objectUnRight = 'Недостаточно прав <Сохранение/фиксирование> на объекты: ' + @objectUnRight;
	RAISERROR(@objectUnRight, 16, 1)
return;
end

--Проверяем допустимое время
declare @dntWriteManualList nvarchar(max), @dntWriteFactList nvarchar(max)
exec usp2_Forecast_UserHasRight_PlanTimeRules @forecastWriteTable, @dntWriteManualList OUTPUT, @dntWriteFactList OUTPUT

if (len(@dntWriteManualList) > 0) begin
	set @dntWriteManualList = 'Запрет на сохранение плановых часов: ' + @dntWriteManualList;
	RAISERROR( @dntWriteManualList, 16, 1)
	return;
end

--if (len(@dntWriteFactList) > 0) begin
--	set @dntWriteFactList = 'Запрет на сохранение фактических часов: ' + @dntWriteFactList;
--	RAISERROR(@dntWriteFactList, 16, 1)
--	return;
--end

create table #inserted
(
	[ForecastObject_UN] varchar(22) NOT NULL,
	[EventDate] DateTime NOT NULL,
	[Priority] tinyint NOT NULL,
	[ForecastArchiveJournal_UN] [uniqueidentifier] NOT NULL,
	[AUTOBITS] [bigint], [MANUALBITS] [bigint], [FACTBITS] [bigint], 
	[AUTO_01] [float] NULL,	[AUTO_02] [float] NULL,	[AUTO_03] [float] NULL,	[AUTO_04] [float] NULL,	[AUTO_05] [float] NULL,	[AUTO_06] [float] NULL,	[AUTO_07] [float] NULL,	[AUTO_08] [float] NULL,	[AUTO_09] [float] NULL,	[AUTO_10] [float] NULL,
	[AUTO_11] [float] NULL,	[AUTO_12] [float] NULL,	[AUTO_13] [float] NULL,	[AUTO_14] [float] NULL,	[AUTO_15] [float] NULL,	[AUTO_16] [float] NULL,	[AUTO_17] [float] NULL,	[AUTO_18] [float] NULL,	[AUTO_19] [float] NULL,	[AUTO_20] [float] NULL,
	[AUTO_21] [float] NULL,	[AUTO_22] [float] NULL,	[AUTO_23] [float] NULL,	[AUTO_24] [float] NULL,	[AUTO_25] [float] NULL,	[AUTO_26] [float] NULL,	[AUTO_27] [float] NULL,	[AUTO_28] [float] NULL,	[AUTO_29] [float] NULL,	[AUTO_30] [float] NULL,
	[AUTO_31] [float] NULL,	[AUTO_32] [float] NULL,	[AUTO_33] [float] NULL,	[AUTO_34] [float] NULL,	[AUTO_35] [float] NULL,	[AUTO_36] [float] NULL,	[AUTO_37] [float] NULL,	[AUTO_38] [float] NULL,	[AUTO_39] [float] NULL,	[AUTO_40] [float] NULL,
	[AUTO_41] [float] NULL,	[AUTO_42] [float] NULL,	[AUTO_43] [float] NULL,	[AUTO_44] [float] NULL,	[AUTO_45] [float] NULL,	[AUTO_46] [float] NULL,	[AUTO_47] [float] NULL,	[AUTO_48] [float] NULL,
	[MANUAL_01] [float]  NULL,	[MANUAL_02] [float]  NULL,	[MANUAL_03] [float]  NULL,	[MANUAL_04] [float]  NULL,	[MANUAL_05] [float]  NULL,	[MANUAL_06] [float]  NULL,	[MANUAL_07] [float]  NULL,	[MANUAL_08] [float]  NULL,	[MANUAL_09] [float]  NULL,	[MANUAL_10] [float]  NULL,
	[MANUAL_11] [float]  NULL,	[MANUAL_12] [float]  NULL,	[MANUAL_13] [float]  NULL,	[MANUAL_14] [float]  NULL,	[MANUAL_15] [float]  NULL,	[MANUAL_16] [float]  NULL,	[MANUAL_17] [float]  NULL,	[MANUAL_18] [float]  NULL,	[MANUAL_19] [float]  NULL,	[MANUAL_20] [float]  NULL,
	[MANUAL_21] [float]  NULL,	[MANUAL_22] [float]  NULL,	[MANUAL_23] [float]  NULL,	[MANUAL_24] [float]  NULL,	[MANUAL_25] [float]  NULL,	[MANUAL_26] [float]  NULL,	[MANUAL_27] [float]  NULL,	[MANUAL_28] [float]  NULL,	[MANUAL_29] [float]  NULL,	[MANUAL_30] [float]  NULL,
	[MANUAL_31] [float]  NULL,	[MANUAL_32] [float]  NULL,	[MANUAL_33] [float]  NULL,	[MANUAL_34] [float]  NULL,	[MANUAL_35] [float]  NULL,	[MANUAL_36] [float]  NULL,	[MANUAL_37] [float]  NULL,	[MANUAL_38] [float]  NULL,	[MANUAL_39] [float]  NULL,	[MANUAL_40] [float]  NULL,
	[MANUAL_41] [float]  NULL,	[MANUAL_42] [float]  NULL,	[MANUAL_43] [float]  NULL,	[MANUAL_44] [float]  NULL,	[MANUAL_45] [float]  NULL,	[MANUAL_46] [float]  NULL,	[MANUAL_47] [float]  NULL,	[MANUAL_48] [float]  NULL,
	[FACT_01] [float]  NULL,	[FACT_02] [float]  NULL,	[FACT_03] [float]  NULL,	[FACT_04] [float]  NULL,	[FACT_05] [float]  NULL,	[FACT_06] [float]  NULL,	[FACT_07] [float]  NULL,	[FACT_08] [float]  NULL,	[FACT_09] [float]  NULL,	[FACT_10] [float]  NULL,
	[FACT_11] [float]  NULL,	[FACT_12] [float]  NULL,	[FACT_13] [float]  NULL,	[FACT_14] [float]  NULL,	[FACT_15] [float]  NULL,	[FACT_16] [float]  NULL,	[FACT_17] [float]  NULL,	[FACT_18] [float]  NULL,	[FACT_19] [float]  NULL,	[FACT_20] [float]  NULL,
	[FACT_21] [float]  NULL,	[FACT_22] [float]  NULL,	[FACT_23] [float]  NULL,	[FACT_24] [float]  NULL,	[FACT_25] [float]  NULL,	[FACT_26] [float]  NULL,	[FACT_27] [float]  NULL,	[FACT_28] [float]  NULL,	[FACT_29] [float]  NULL,	[FACT_30] [float]  NULL,
	[FACT_31] [float]  NULL,	[FACT_32] [float]  NULL,	[FACT_33] [float]  NULL,	[FACT_34] [float]  NULL,	[FACT_35] [float]  NULL,	[FACT_36] [float]  NULL,	[FACT_37] [float]  NULL,	[FACT_38] [float]  NULL,	[FACT_39] [float]  NULL,	[FACT_40] [float]  NULL,
	[FACT_41] [float]  NULL,	[FACT_42] [float]  NULL,	[FACT_43] [float]  NULL,	[FACT_44] [float]  NULL,	[FACT_45] [float]  NULL,	[FACT_46] [float]  NULL,	[FACT_47] [float]  NULL,	[FACT_48] [float]  NULL,

	deletedEventDate DateTime NULL, deletedPriority tinyint NULL,

	PRIMARY KEY CLUSTERED 
(
[ForecastArchiveJournal_UN] ASC
)
)

BEGIN TRY  BEGIN TRANSACTION

	MERGE Forecast_Archive_Journal as aj
	USING @forecastWriterArchive30Table s
	on aj.ForecastObject_UN = s.ForecastObject_UN and aj.EventDate = s.EventDate and (s.[Priority] is not null and aj.[Priority] = s.[Priority]) -- Если NULL это вставка нового приоритета
	WHEN MATCHED THEN 
	UPDATE 
	SET ForecastCalculateModel_ID = s.ForecastCalculateModel_ID,
	DispatchDateTime = s.DispatchDateTime, --Обновляем только дату, время и коментарий (это простая правка старого)
		 Comment = s.Comment,
		 [User_ID] = s.[User_ID]
	WHEN NOT MATCHED THEN 
	--В остальных случаях вставляем новую запись
	INSERT ([ForecastObject_UN],[EventDate],[ForecastArchiveJournal_UN],[ForecastCalculateModel_ID],[User_ID]
			   ,[DispatchDateTime],[Comment],[Priority])
	VALUES (s.[ForecastObject_UN],s.[EventDate],NEWID(),s.[ForecastCalculateModel_ID],s.[User_ID]
			   ,s.[DispatchDateTime],s.[Comment],
			   case when s.[Priority] is null 
			   ---Приоритет уменьшаем от 255 до 0, 0 - это зафиксированные данные, чем меньше, тем выше приоритет
				then ISNULL((select Min([Priority]) from Forecast_Archive_Journal where ForecastObject_UN = s.ForecastObject_UN and EventDate = s.EventDate and [Priority] > 0), 255) - 1 
				else s.[Priority] end)
	output inserted.[ForecastObject_UN], inserted.[EventDate], inserted.[Priority], inserted.ForecastArchiveJournal_UN, 
	s.[AUTOBITS], s.[MANUALBITS], s.[FACTBITS],
	s.[AUTO_01],s.[AUTO_02],s.[AUTO_03],s.[AUTO_04],s.[AUTO_05],s.[AUTO_06],s.[AUTO_07],s.[AUTO_08],s.[AUTO_09],s.[AUTO_10],
	s.[AUTO_11],s.[AUTO_12],s.[AUTO_13],s.[AUTO_14],s.[AUTO_15],s.[AUTO_16],s.[AUTO_17],s.[AUTO_18],s.[AUTO_19],s.[AUTO_20],
	s.[AUTO_21],s.[AUTO_22],s.[AUTO_23],s.[AUTO_24],s.[AUTO_25],s.[AUTO_26],s.[AUTO_27],s.[AUTO_28],s.[AUTO_29],s.[AUTO_30],
	s.[AUTO_31],s.[AUTO_32],s.[AUTO_33],s.[AUTO_34],s.[AUTO_35],s.[AUTO_36],s.[AUTO_37],s.[AUTO_38],s.[AUTO_39],s.[AUTO_40],
	s.[AUTO_41],s.[AUTO_42],s.[AUTO_43],s.[AUTO_44],s.[AUTO_45],s.[AUTO_46],s.[AUTO_47],s.[AUTO_48],

	s.[MANUAL_01],s.[MANUAL_02],s.[MANUAL_03],s.[MANUAL_04],s.[MANUAL_05],s.[MANUAL_06],s.[MANUAL_07],s.[MANUAL_08],s.[MANUAL_09],s.[MANUAL_10],
	s.[MANUAL_11],s.[MANUAL_12],s.[MANUAL_13],s.[MANUAL_14],s.[MANUAL_15],s.[MANUAL_16],s.[MANUAL_17],s.[MANUAL_18],s.[MANUAL_19],s.[MANUAL_20],
	s.[MANUAL_21],s.[MANUAL_22],s.[MANUAL_23],s.[MANUAL_24],s.[MANUAL_25],s.[MANUAL_26],s.[MANUAL_27],s.[MANUAL_28],s.[MANUAL_29],s.[MANUAL_30],
	s.[MANUAL_31],s.[MANUAL_32],s.[MANUAL_33],s.[MANUAL_34],s.[MANUAL_35],s.[MANUAL_36],s.[MANUAL_37],s.[MANUAL_38],s.[MANUAL_39],s.[MANUAL_40],
	s.[MANUAL_41],s.[MANUAL_42],s.[MANUAL_43],s.[MANUAL_44],s.[MANUAL_45],s.[MANUAL_46],s.[MANUAL_47],s.[MANUAL_48],

	s.[FACT_01],s.[FACT_02],s.[FACT_03],s.[FACT_04],s.[FACT_05],s.[FACT_06],s.[FACT_07],s.[FACT_08],s.[FACT_09],s.[FACT_10],
	s.[FACT_11],s.[FACT_12],s.[FACT_13],s.[FACT_14],s.[FACT_15],s.[FACT_16],s.[FACT_17],s.[FACT_18],s.[FACT_19],s.[FACT_20],
	s.[FACT_21],s.[FACT_22],s.[FACT_23],s.[FACT_24],s.[FACT_25],s.[FACT_26],s.[FACT_27],s.[FACT_28],s.[FACT_29],s.[FACT_30],
	s.[FACT_31],s.[FACT_32],s.[FACT_33],s.[FACT_34],s.[FACT_35],s.[FACT_36],s.[FACT_37],s.[FACT_38],s.[FACT_39],s.[FACT_40],
	s.[FACT_41],s.[FACT_42],s.[FACT_43],s.[FACT_44],s.[FACT_45],s.[FACT_46],s.[FACT_47],s.[FACT_48],
	deleted.[EventDate], deleted.[Priority]

	into #inserted;

	---Проверяем что нет зафиксированных
	if (@isForceFix = 0 and exists(select top 1 1 from #inserted where deletedPriority=0 and (AUTOBITS<>0 or MANUALBITS<>0))) begin
		declare @fixedObject nvarchar(max)
		set @fixedObject = '';

		select distinct @fixedObject = @fixedObject + '<' + o.ForecastObjectName + '> на ' +
		substring(
			(select ',' + convert(varchar, deletedEventDate, 104) as [text()] from #inserted 
			where deletedPriority=0 and ForecastObject_UN = i.ForecastObject_UN
			order by deletedEventDate
			For XML PATH ('')) ,2,1000
		) + ';' + CHAR(13)
		from #inserted i
		join Forecast_Objects o on o.ForecastObject_UN = i.ForecastObject_UN
		where deletedPriority is not null

		set @fixedObject = 'Обнаружены уже зафиксированные записи для: ' + CHAR(13) + @fixedObject + 'Сначала расфиксируйте записи, или измените даты'

		RAISERROR(@fixedObject, 16, 1)
		return
	end

	--Теперь обновляем таблицу Forecast_Archive_Data_30
	MERGE Forecast_Archive_Data_30 as a
	USING #inserted i
	ON a.ForecastArchiveJournal_UN = i.ForecastArchiveJournal_UN
	WHEN MATCHED THEN 
	UPDATE SET 
	a.[AUTO_01] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 0) = 1 then i.[AUTO_01] else a.[AUTO_01] end,
	a.[AUTO_02] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 1) = 1 then i.[AUTO_02] else a.[AUTO_02] end,
	a.[AUTO_03] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 2) = 1 then i.[AUTO_03] else a.[AUTO_03] end,
	a.[AUTO_04] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 3) = 1 then i.[AUTO_04] else a.[AUTO_04] end,
	a.[AUTO_05] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 4) = 1 then i.[AUTO_05] else a.[AUTO_05] end,
	a.[AUTO_06] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 5) = 1 then i.[AUTO_06] else a.[AUTO_06] end,
	a.[AUTO_07] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 6) = 1 then i.[AUTO_07] else a.[AUTO_07] end,
	a.[AUTO_08] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 7) = 1 then i.[AUTO_08] else a.[AUTO_08] end,
	a.[AUTO_09] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 8) = 1 then i.[AUTO_09] else a.[AUTO_09] end,
	a.[AUTO_10] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 9) = 1 then i.[AUTO_10] else a.[AUTO_10] end,
	a.[AUTO_11] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 10) = 1 then i.[AUTO_11] else a.[AUTO_11] end,
	a.[AUTO_12] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 11) = 1 then i.[AUTO_12] else a.[AUTO_12] end,
	a.[AUTO_13] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 12) = 1 then i.[AUTO_13] else a.[AUTO_13] end,
	a.[AUTO_14] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 13) = 1 then i.[AUTO_14] else a.[AUTO_14] end,
	a.[AUTO_15] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 14) = 1 then i.[AUTO_15] else a.[AUTO_15] end,
	a.[AUTO_16] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 15) = 1 then i.[AUTO_16] else a.[AUTO_16] end,
	a.[AUTO_17] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 16) = 1 then i.[AUTO_17] else a.[AUTO_17] end,
	a.[AUTO_18] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 17) = 1 then i.[AUTO_18] else a.[AUTO_18] end,
	a.[AUTO_19] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 18) = 1 then i.[AUTO_19] else a.[AUTO_19] end,
	a.[AUTO_20] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 19) = 1 then i.[AUTO_20] else a.[AUTO_20] end,
	a.[AUTO_21] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 20) = 1 then i.[AUTO_21] else a.[AUTO_21] end,
	a.[AUTO_22] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 21) = 1 then i.[AUTO_22] else a.[AUTO_22] end,
	a.[AUTO_23] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 22) = 1 then i.[AUTO_23] else a.[AUTO_23] end,
	a.[AUTO_24] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 23) = 1 then i.[AUTO_24] else a.[AUTO_24] end,
	a.[AUTO_25] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 24) = 1 then i.[AUTO_25] else a.[AUTO_25] end,
	a.[AUTO_26] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 25) = 1 then i.[AUTO_26] else a.[AUTO_26] end,
	a.[AUTO_27] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 26) = 1 then i.[AUTO_27] else a.[AUTO_27] end,
	a.[AUTO_28] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 27) = 1 then i.[AUTO_28] else a.[AUTO_28] end,
	a.[AUTO_29] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 28) = 1 then i.[AUTO_29] else a.[AUTO_29] end,
	a.[AUTO_30] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 29) = 1 then i.[AUTO_30] else a.[AUTO_30] end,
	a.[AUTO_31] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 30) = 1 then i.[AUTO_31] else a.[AUTO_31] end,
	a.[AUTO_32] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 31) = 1 then i.[AUTO_32] else a.[AUTO_32] end,
	a.[AUTO_33] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 32) = 1 then i.[AUTO_33] else a.[AUTO_33] end,
	a.[AUTO_34] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 33) = 1 then i.[AUTO_34] else a.[AUTO_34] end,
	a.[AUTO_35] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 34) = 1 then i.[AUTO_35] else a.[AUTO_35] end,
	a.[AUTO_36] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 35) = 1 then i.[AUTO_36] else a.[AUTO_36] end,
	a.[AUTO_37] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 36) = 1 then i.[AUTO_37] else a.[AUTO_37] end,
	a.[AUTO_38] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 37) = 1 then i.[AUTO_38] else a.[AUTO_38] end,
	a.[AUTO_39] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 38) = 1 then i.[AUTO_39] else a.[AUTO_39] end,
	a.[AUTO_40] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 39) = 1 then i.[AUTO_40] else a.[AUTO_40] end,
	a.[AUTO_41] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 40) = 1 then i.[AUTO_41] else a.[AUTO_41] end,
	a.[AUTO_42] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 41) = 1 then i.[AUTO_42] else a.[AUTO_42] end,
	a.[AUTO_43] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 42) = 1 then i.[AUTO_43] else a.[AUTO_43] end,
	a.[AUTO_44] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 43) = 1 then i.[AUTO_44] else a.[AUTO_44] end,
	a.[AUTO_45] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 44) = 1 then i.[AUTO_45] else a.[AUTO_45] end,
	a.[AUTO_46] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 45) = 1 then i.[AUTO_46] else a.[AUTO_46] end,
	a.[AUTO_47] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 46) = 1 then i.[AUTO_47] else a.[AUTO_47] end,
	a.[AUTO_48] = case when [dbo].[sfclr_Utils_BitOperations2](AUTOBITS, 47) = 1 then i.[AUTO_48] else a.[AUTO_48] end,

	a.[MANUAL_01] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 0) = 1 then i.[MANUAL_01] else a.[MANUAL_01] end,
	a.[MANUAL_02] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 1) = 1 then i.[MANUAL_02] else a.[MANUAL_02] end,
	a.[MANUAL_03] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 2) = 1 then i.[MANUAL_03] else a.[MANUAL_03] end,
	a.[MANUAL_04] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 3) = 1 then i.[MANUAL_04] else a.[MANUAL_04] end,
	a.[MANUAL_05] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 4) = 1 then i.[MANUAL_05] else a.[MANUAL_05] end,
	a.[MANUAL_06] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 5) = 1 then i.[MANUAL_06] else a.[MANUAL_06] end,
	a.[MANUAL_07] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 6) = 1 then i.[MANUAL_07] else a.[MANUAL_07] end,
	a.[MANUAL_08] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 7) = 1 then i.[MANUAL_08] else a.[MANUAL_08] end,
	a.[MANUAL_09] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 8) = 1 then i.[MANUAL_09] else a.[MANUAL_09] end,
	a.[MANUAL_10] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 9) = 1 then i.[MANUAL_10] else a.[MANUAL_10] end,
	a.[MANUAL_11] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 10) = 1 then i.[MANUAL_11] else a.[MANUAL_11] end,
	a.[MANUAL_12] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 11) = 1 then i.[MANUAL_12] else a.[MANUAL_12] end,
	a.[MANUAL_13] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 12) = 1 then i.[MANUAL_13] else a.[MANUAL_13] end,
	a.[MANUAL_14] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 13) = 1 then i.[MANUAL_14] else a.[MANUAL_14] end,
	a.[MANUAL_15] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 14) = 1 then i.[MANUAL_15] else a.[MANUAL_15] end,
	a.[MANUAL_16] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 15) = 1 then i.[MANUAL_16] else a.[MANUAL_16] end,
	a.[MANUAL_17] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 16) = 1 then i.[MANUAL_17] else a.[MANUAL_17] end,
	a.[MANUAL_18] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 17) = 1 then i.[MANUAL_18] else a.[MANUAL_18] end,
	a.[MANUAL_19] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 18) = 1 then i.[MANUAL_19] else a.[MANUAL_19] end,
	a.[MANUAL_20] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 19) = 1 then i.[MANUAL_20] else a.[MANUAL_20] end,
	a.[MANUAL_21] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 20) = 1 then i.[MANUAL_21] else a.[MANUAL_21] end,
	a.[MANUAL_22] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 21) = 1 then i.[MANUAL_22] else a.[MANUAL_22] end,
	a.[MANUAL_23] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 22) = 1 then i.[MANUAL_23] else a.[MANUAL_23] end,
	a.[MANUAL_24] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 23) = 1 then i.[MANUAL_24] else a.[MANUAL_24] end,
	a.[MANUAL_25] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 24) = 1 then i.[MANUAL_25] else a.[MANUAL_25] end,
	a.[MANUAL_26] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 25) = 1 then i.[MANUAL_26] else a.[MANUAL_26] end,
	a.[MANUAL_27] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 26) = 1 then i.[MANUAL_27] else a.[MANUAL_27] end,
	a.[MANUAL_28] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 27) = 1 then i.[MANUAL_28] else a.[MANUAL_28] end,
	a.[MANUAL_29] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 28) = 1 then i.[MANUAL_29] else a.[MANUAL_29] end,
	a.[MANUAL_30] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 29) = 1 then i.[MANUAL_30] else a.[MANUAL_30] end,
	a.[MANUAL_31] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 30) = 1 then i.[MANUAL_31] else a.[MANUAL_31] end,
	a.[MANUAL_32] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 31) = 1 then i.[MANUAL_32] else a.[MANUAL_32] end,
	a.[MANUAL_33] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 32) = 1 then i.[MANUAL_33] else a.[MANUAL_33] end,
	a.[MANUAL_34] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 33) = 1 then i.[MANUAL_34] else a.[MANUAL_34] end,
	a.[MANUAL_35] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 34) = 1 then i.[MANUAL_35] else a.[MANUAL_35] end,
	a.[MANUAL_36] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 35) = 1 then i.[MANUAL_36] else a.[MANUAL_36] end,
	a.[MANUAL_37] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 36) = 1 then i.[MANUAL_37] else a.[MANUAL_37] end,
	a.[MANUAL_38] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 37) = 1 then i.[MANUAL_38] else a.[MANUAL_38] end,
	a.[MANUAL_39] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 38) = 1 then i.[MANUAL_39] else a.[MANUAL_39] end,
	a.[MANUAL_40] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 39) = 1 then i.[MANUAL_40] else a.[MANUAL_40] end,
	a.[MANUAL_41] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 40) = 1 then i.[MANUAL_41] else a.[MANUAL_41] end,
	a.[MANUAL_42] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 41) = 1 then i.[MANUAL_42] else a.[MANUAL_42] end,
	a.[MANUAL_43] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 42) = 1 then i.[MANUAL_43] else a.[MANUAL_43] end,
	a.[MANUAL_44] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 43) = 1 then i.[MANUAL_44] else a.[MANUAL_44] end,
	a.[MANUAL_45] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 44) = 1 then i.[MANUAL_45] else a.[MANUAL_45] end,
	a.[MANUAL_46] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 45) = 1 then i.[MANUAL_46] else a.[MANUAL_46] end,
	a.[MANUAL_47] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 46) = 1 then i.[MANUAL_47] else a.[MANUAL_47] end,
	a.[MANUAL_48] = case when [dbo].[sfclr_Utils_BitOperations2](MANUALBITS, 47) = 1 then i.[MANUAL_48] else a.[MANUAL_48] end,

	a.[FACT_01] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 0) = 1 then i.[FACT_01] else a.[FACT_01] end,
	a.[FACT_02] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 1) = 1 then i.[FACT_02] else a.[FACT_02] end,
	a.[FACT_03] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 2) = 1 then i.[FACT_03] else a.[FACT_03] end,
	a.[FACT_04] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 3) = 1 then i.[FACT_04] else a.[FACT_04] end,
	a.[FACT_05] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 4) = 1 then i.[FACT_05] else a.[FACT_05] end,
	a.[FACT_06] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 5) = 1 then i.[FACT_06] else a.[FACT_06] end,
	a.[FACT_07] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 6) = 1 then i.[FACT_07] else a.[FACT_07] end,
	a.[FACT_08] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 7) = 1 then i.[FACT_08] else a.[FACT_08] end,
	a.[FACT_09] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 8) = 1 then i.[FACT_09] else a.[FACT_09] end,
	a.[FACT_10] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 9) = 1 then i.[FACT_10] else a.[FACT_10] end,
	a.[FACT_11] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 10) = 1 then i.[FACT_11] else a.[FACT_11] end,
	a.[FACT_12] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 11) = 1 then i.[FACT_12] else a.[FACT_12] end,
	a.[FACT_13] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 12) = 1 then i.[FACT_13] else a.[FACT_13] end,
	a.[FACT_14] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 13) = 1 then i.[FACT_14] else a.[FACT_14] end,
	a.[FACT_15] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 14) = 1 then i.[FACT_15] else a.[FACT_15] end,
	a.[FACT_16] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 15) = 1 then i.[FACT_16] else a.[FACT_16] end,
	a.[FACT_17] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 16) = 1 then i.[FACT_17] else a.[FACT_17] end,
	a.[FACT_18] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 17) = 1 then i.[FACT_18] else a.[FACT_18] end,
	a.[FACT_19] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 18) = 1 then i.[FACT_19] else a.[FACT_19] end,
	a.[FACT_20] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 19) = 1 then i.[FACT_20] else a.[FACT_20] end,
	a.[FACT_21] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 20) = 1 then i.[FACT_21] else a.[FACT_21] end,
	a.[FACT_22] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 21) = 1 then i.[FACT_22] else a.[FACT_22] end,
	a.[FACT_23] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 22) = 1 then i.[FACT_23] else a.[FACT_23] end,
	a.[FACT_24] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 23) = 1 then i.[FACT_24] else a.[FACT_24] end,
	a.[FACT_25] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 24) = 1 then i.[FACT_25] else a.[FACT_25] end,
	a.[FACT_26] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 25) = 1 then i.[FACT_26] else a.[FACT_26] end,
	a.[FACT_27] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 26) = 1 then i.[FACT_27] else a.[FACT_27] end,
	a.[FACT_28] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 27) = 1 then i.[FACT_28] else a.[FACT_28] end,
	a.[FACT_29] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 28) = 1 then i.[FACT_29] else a.[FACT_29] end,
	a.[FACT_30] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 29) = 1 then i.[FACT_30] else a.[FACT_30] end,
	a.[FACT_31] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 30) = 1 then i.[FACT_31] else a.[FACT_31] end,
	a.[FACT_32] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 31) = 1 then i.[FACT_32] else a.[FACT_32] end,
	a.[FACT_33] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 32) = 1 then i.[FACT_33] else a.[FACT_33] end,
	a.[FACT_34] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 33) = 1 then i.[FACT_34] else a.[FACT_34] end,
	a.[FACT_35] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 34) = 1 then i.[FACT_35] else a.[FACT_35] end,
	a.[FACT_36] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 35) = 1 then i.[FACT_36] else a.[FACT_36] end,
	a.[FACT_37] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 36) = 1 then i.[FACT_37] else a.[FACT_37] end,
	a.[FACT_38] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 37) = 1 then i.[FACT_38] else a.[FACT_38] end,
	a.[FACT_39] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 38) = 1 then i.[FACT_39] else a.[FACT_39] end,
	a.[FACT_40] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 39) = 1 then i.[FACT_40] else a.[FACT_40] end,
	a.[FACT_41] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 40) = 1 then i.[FACT_41] else a.[FACT_41] end,
	a.[FACT_42] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 41) = 1 then i.[FACT_42] else a.[FACT_42] end,
	a.[FACT_43] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 42) = 1 then i.[FACT_43] else a.[FACT_43] end,
	a.[FACT_44] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 43) = 1 then i.[FACT_44] else a.[FACT_44] end,
	a.[FACT_45] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 44) = 1 then i.[FACT_45] else a.[FACT_45] end,
	a.[FACT_46] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 45) = 1 then i.[FACT_46] else a.[FACT_46] end,
	a.[FACT_47] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 46) = 1 then i.[FACT_47] else a.[FACT_47] end,
	a.[FACT_48] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 47) = 1 then i.[FACT_48] else a.[FACT_48] end

	WHEN NOT MATCHED THEN 
	insert ([ForecastArchiveJournal_UN],[AUTO_01],[AUTO_02],[AUTO_03],[AUTO_04],[AUTO_05],[AUTO_06],[AUTO_07],[AUTO_08],[AUTO_09],[AUTO_10],
	[AUTO_11],[AUTO_12],[AUTO_13],[AUTO_14],[AUTO_15],[AUTO_16],[AUTO_17],[AUTO_18],[AUTO_19],[AUTO_20],
	[AUTO_21],[AUTO_22],[AUTO_23],[AUTO_24],[AUTO_25],[AUTO_26],[AUTO_27],[AUTO_28],[AUTO_29],[AUTO_30],
	[AUTO_31],[AUTO_32],[AUTO_33],[AUTO_34],[AUTO_35],[AUTO_36],[AUTO_37],[AUTO_38],[AUTO_39],[AUTO_40],
	[AUTO_41],[AUTO_42],[AUTO_43],[AUTO_44],[AUTO_45],[AUTO_46],[AUTO_47],[AUTO_48],
	[MANUAL_01],[MANUAL_02],[MANUAL_03],[MANUAL_04],[MANUAL_05],[MANUAL_06],[MANUAL_07],[MANUAL_08],[MANUAL_09],[MANUAL_10],
	[MANUAL_11],[MANUAL_12],[MANUAL_13],[MANUAL_14],[MANUAL_15],[MANUAL_16],[MANUAL_17],[MANUAL_18],[MANUAL_19],[MANUAL_20],
	[MANUAL_21],[MANUAL_22],[MANUAL_23],[MANUAL_24],[MANUAL_25],[MANUAL_26],[MANUAL_27],[MANUAL_28],[MANUAL_29],[MANUAL_30],
	[MANUAL_31],[MANUAL_32],[MANUAL_33],[MANUAL_34],[MANUAL_35],[MANUAL_36],[MANUAL_37],[MANUAL_38],[MANUAL_39],[MANUAL_40],
	[MANUAL_41],[MANUAL_42],[MANUAL_43],[MANUAL_44],[MANUAL_45],[MANUAL_46],[MANUAL_47],[MANUAL_48],
	[FACT_01],[FACT_02],[FACT_03],[FACT_04],[FACT_05],[FACT_06],[FACT_07],[FACT_08],[FACT_09],[FACT_10],
	[FACT_11],[FACT_12],[FACT_13],[FACT_14],[FACT_15],[FACT_16],[FACT_17],[FACT_18],[FACT_19],[FACT_20],
	[FACT_21],[FACT_22],[FACT_23],[FACT_24],[FACT_25],[FACT_26],[FACT_27],[FACT_28],[FACT_29],[FACT_30],
	[FACT_31],[FACT_32],[FACT_33],[FACT_34],[FACT_35],[FACT_36],[FACT_37],[FACT_38],[FACT_39],[FACT_40],
	[FACT_41],[FACT_42],[FACT_43],[FACT_44],[FACT_45],[FACT_46],[FACT_47],[FACT_48])
	values(i.[ForecastArchiveJournal_UN],i.[AUTO_01],i.[AUTO_02],i.[AUTO_03],i.[AUTO_04],i.[AUTO_05],i.[AUTO_06],i.[AUTO_07],i.[AUTO_08],i.[AUTO_09],i.[AUTO_10],
	i.[AUTO_11],i.[AUTO_12],i.[AUTO_13],i.[AUTO_14],i.[AUTO_15],i.[AUTO_16],i.[AUTO_17],i.[AUTO_18],i.[AUTO_19],i.[AUTO_20],
	i.[AUTO_21],i.[AUTO_22],i.[AUTO_23],i.[AUTO_24],i.[AUTO_25],i.[AUTO_26],i.[AUTO_27],i.[AUTO_28],i.[AUTO_29],i.[AUTO_30],
	i.[AUTO_31],i.[AUTO_32],i.[AUTO_33],i.[AUTO_34],i.[AUTO_35],i.[AUTO_36],i.[AUTO_37],i.[AUTO_38],i.[AUTO_39],i.[AUTO_40],
	i.[AUTO_41],i.[AUTO_42],i.[AUTO_43],i.[AUTO_44],i.[AUTO_45],i.[AUTO_46],i.[AUTO_47],i.[AUTO_48],
	i.[MANUAL_01],i.[MANUAL_02],i.[MANUAL_03],i.[MANUAL_04],i.[MANUAL_05],i.[MANUAL_06],i.[MANUAL_07],i.[MANUAL_08],i.[MANUAL_09],i.[MANUAL_10],
	i.[MANUAL_11],i.[MANUAL_12],i.[MANUAL_13],i.[MANUAL_14],i.[MANUAL_15],i.[MANUAL_16],i.[MANUAL_17],i.[MANUAL_18],i.[MANUAL_19],i.[MANUAL_20],
	i.[MANUAL_21],i.[MANUAL_22],i.[MANUAL_23],i.[MANUAL_24],i.[MANUAL_25],i.[MANUAL_26],i.[MANUAL_27],i.[MANUAL_28],i.[MANUAL_29],i.[MANUAL_30],
	i.[MANUAL_31],i.[MANUAL_32],i.[MANUAL_33],i.[MANUAL_34],i.[MANUAL_35],i.[MANUAL_36],i.[MANUAL_37],i.[MANUAL_38],i.[MANUAL_39],i.[MANUAL_40],
	i.[MANUAL_41],i.[MANUAL_42],i.[MANUAL_43],i.[MANUAL_44],i.[MANUAL_45],i.[MANUAL_46],i.[MANUAL_47],i.[MANUAL_48],
	i.[FACT_01],i.[FACT_02],i.[FACT_03],i.[FACT_04],i.[FACT_05],i.[FACT_06],i.[FACT_07],i.[FACT_08],i.[FACT_09],i.[FACT_10],
	i.[FACT_11],i.[FACT_12],i.[FACT_13],i.[FACT_14],i.[FACT_15],i.[FACT_16],i.[FACT_17],i.[FACT_18],i.[FACT_19],i.[FACT_20],
	i.[FACT_21],i.[FACT_22],i.[FACT_23],i.[FACT_24],i.[FACT_25],i.[FACT_26],i.[FACT_27],i.[FACT_28],i.[FACT_29],i.[FACT_30],
	i.[FACT_31],i.[FACT_32],i.[FACT_33],i.[FACT_34],i.[FACT_35],i.[FACT_36],i.[FACT_37],i.[FACT_38],i.[FACT_39],i.[FACT_40],
	i.[FACT_41],i.[FACT_42],i.[FACT_43],i.[FACT_44],i.[FACT_45],i.[FACT_46],i.[FACT_47],i.[FACT_48]);

	COMMIT	

	--Возвращаем то что записали
	select [ForecastObject_UN],[EventDate],[Priority] from #inserted 
	return 

END TRY
BEGIN CATCH
	--Ошибка, откатываем все изменения
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 16 --Нужен exception
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
--select * from #inserted;

drop table #inserted

--Запись в журнал
--insert into Expl_User_Journal (ApplicationType, CommentString, CUS_ID, EventDateTime, EventString, User_ID, ObjectID, ObjectName)
--select distinct 1, 'Прогнозирование', 0, DispatchDateTime,
--case when Priority = 0 then 'Фиксация' else 'Сохранение' end + ' ручной ввод энергии за ' + Convert(nvarchar, s.EventDate, 104), s.User_ID, s.ForecastObject_UN, o.ForecastObjectName
--from @forecastWriterArchive30Table s
--join Forecast_Objects o on o.ForecastObject_UN = s.ForecastObject_UN


end
go
   grant EXECUTE on usp2_Forecast_WriteArchive30 to [UserCalcService]
go