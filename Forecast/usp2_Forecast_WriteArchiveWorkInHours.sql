if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_WriteArchiveWorkInHours')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_WriteArchiveWorkInHours
go

--Создаем тип, если его нет
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ForecastWriterArchiveWorkInHoursValuesType' AND ss.name = N'dbo')
DROP TYPE [dbo].[ForecastWriterArchiveWorkInHoursValuesType]

CREATE TYPE [dbo].[ForecastWriterArchiveWorkInHoursValuesType] AS TABLE(
	[ForecastObject_UN] [varchar](22) NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[Priority] [tinyint] NULL,
	[ForecastCalculateModel_ID] [int] NOT NULL,
	[ForecastObjectTypeMode_ID] [int] NULL,
	[User_ID] [varchar](22) NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[Comment] [nvarchar](1000) NULL,
	[MANUALBITS] [bigint],
	[FACTBITS] [bigint],
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
	[FACT_24] [float] NULL
)
go

grant EXECUTE on TYPE::ForecastWriterArchiveWorkInHoursValuesType to [UserCalcService]
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
--		Пишем таблицу часов работы насосов в БД
--
-- ======================================================================================
create proc [dbo].[usp2_Forecast_WriteArchiveWorkInHours]
	
	@forecastWriterArchiveWorkInHoursTable ForecastWriterArchiveWorkInHoursValuesType READONLY, --Таблицу которую пишем в базу данных
	@treeID int = null
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare @forecastWriteTable ForecastWriterType;
insert into @forecastWriteTable
select distinct ForecastObject_UN, EventDate, User_ID, DispatchDateTime, 0, MANUALBITS, FACTBITS  from @forecastWriterArchiveWorkInHoursTable

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

if (len(@dntWriteFactList) > 0) begin
	set @dntWriteFactList = 'Запрет на сохранение фактических часов: ' + @dntWriteFactList;
	RAISERROR(@dntWriteFactList, 16, 1)
	return;
end


create table #inserted
(
	[ForecastObject_UN] varchar(22) NOT NULL,
	[EventDate] DateTime NOT NULL,
	[Priority] tinyint NOT NULL,
	[ForecastArchiveJournal_UN] [uniqueidentifier] NOT NULL
)

BEGIN TRY  BEGIN TRANSACTION

	MERGE Forecast_Archive_Journal as aj
	USING (select distinct [ForecastObject_UN],[EventDate],[Priority], DispatchDateTime,ForecastCalculateModel_ID,[User_ID], Comment from @forecastWriterArchiveWorkInHoursTable) s
	on aj.ForecastObject_UN = s.ForecastObject_UN and aj.EventDate = s.EventDate and (s.[Priority] is not null and aj.[Priority] = s.[Priority]) -- Если NULL это вставка нового приоритета
	WHEN MATCHED THEN 
	UPDATE 
	SET ForecastCalculateModel_ID = s.ForecastCalculateModel_ID,
	DispatchDateTime = s.DispatchDateTime, --Обновляем только дату, время и коментарий (это простая правка старого)
		 Comment = s.Comment
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
	output inserted.[ForecastObject_UN], inserted.[EventDate], inserted.[Priority], inserted.ForecastArchiveJournal_UN
	into #inserted;
		
	--Теперь обновляем таблицу Forecast_Archive_Data_WorkInHours
	MERGE Forecast_Archive_Data_WorkInHours as a
	USING (
			select i.*, ISNULL([ForecastObjectTypeMode_ID],ISNULL((
					select top 1 [ForecastObjectTypeMode_ID] from [dbo].[Forecast_Objects_To_ObjectTypes] oo
					join [dbo].[Forecast_ObjectTypeMode_To_ObjectType] ot on ot.ForecastObjectType_ID = oo.ForecastObjectType_ID
					where oo.ForecastObject_UN = [ForecastObject_UN]
					), 0)) as [ForecastObjectTypeMode_ID],
			f.[MANUALBITS], f.[FACTBITS],
			f.[MANUAL_01],f.[MANUAL_02],f.[MANUAL_03],f.[MANUAL_04],f.[MANUAL_05],f.[MANUAL_06],f.[MANUAL_07],f.[MANUAL_08],f.[MANUAL_09],f.[MANUAL_10],
			f.[MANUAL_11],f.[MANUAL_12],f.[MANUAL_13],f.[MANUAL_14],f.[MANUAL_15],f.[MANUAL_16],f.[MANUAL_17],f.[MANUAL_18],f.[MANUAL_19],f.[MANUAL_20],
			f.[MANUAL_21],f.[MANUAL_22],f.[MANUAL_23],f.[MANUAL_24],
			f.[FACT_01],f.[FACT_02],f.[FACT_03],f.[FACT_04],f.[FACT_05],f.[FACT_06],f.[FACT_07],f.[FACT_08],f.[FACT_09],f.[FACT_10],
			f.[FACT_11],f.[FACT_12],f.[FACT_13],f.[FACT_14],f.[FACT_15],f.[FACT_16],f.[FACT_17],f.[FACT_18],f.[FACT_19],f.[FACT_20],
			f.[FACT_21],f.[FACT_22],f.[FACT_23],f.[FACT_24]
			from #inserted i
			join @forecastWriterArchiveWorkInHoursTable f on f.ForecastObject_UN = i.ForecastObject_UN and f.EventDate = i.EventDate and (f.Priority is null or f.Priority = i.Priority)
			) i
	ON a.ForecastArchiveJournal_UN = i.ForecastArchiveJournal_UN and a.[ForecastObjectTypeMode_ID] = i.[ForecastObjectTypeMode_ID]
	WHEN MATCHED THEN 
	UPDATE SET 
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
	a.[FACT_24] = case when [dbo].[sfclr_Utils_BitOperations2](FACTBITS, 23) = 1 then i.[FACT_24] else a.[FACT_24] end
	WHEN NOT MATCHED THEN 
	insert ([ForecastArchiveJournal_UN], [ForecastObjectTypeMode_ID],
	[MANUAL_01],[MANUAL_02],[MANUAL_03],[MANUAL_04],[MANUAL_05],[MANUAL_06],[MANUAL_07],[MANUAL_08],[MANUAL_09],[MANUAL_10],
	[MANUAL_11],[MANUAL_12],[MANUAL_13],[MANUAL_14],[MANUAL_15],[MANUAL_16],[MANUAL_17],[MANUAL_18],[MANUAL_19],[MANUAL_20],
	[MANUAL_21],[MANUAL_22],[MANUAL_23],[MANUAL_24],
	[FACT_01],[FACT_02],[FACT_03],[FACT_04],[FACT_05],[FACT_06],[FACT_07],[FACT_08],[FACT_09],[FACT_10],
	[FACT_11],[FACT_12],[FACT_13],[FACT_14],[FACT_15],[FACT_16],[FACT_17],[FACT_18],[FACT_19],[FACT_20],
	[FACT_21],[FACT_22],[FACT_23],[FACT_24])
	values(i.[ForecastArchiveJournal_UN],
	--Если режим явно не задан, то выбираем первый попашийся
	i.[ForecastObjectTypeMode_ID],
	i.[MANUAL_01],i.[MANUAL_02],i.[MANUAL_03],i.[MANUAL_04],i.[MANUAL_05],i.[MANUAL_06],i.[MANUAL_07],i.[MANUAL_08],i.[MANUAL_09],i.[MANUAL_10],
	i.[MANUAL_11],i.[MANUAL_12],i.[MANUAL_13],i.[MANUAL_14],i.[MANUAL_15],i.[MANUAL_16],i.[MANUAL_17],i.[MANUAL_18],i.[MANUAL_19],i.[MANUAL_20],
	i.[MANUAL_21],i.[MANUAL_22],i.[MANUAL_23],i.[MANUAL_24],
	i.[FACT_01],i.[FACT_02],i.[FACT_03],i.[FACT_04],i.[FACT_05],i.[FACT_06],i.[FACT_07],i.[FACT_08],i.[FACT_09],i.[FACT_10],
	i.[FACT_11],i.[FACT_12],i.[FACT_13],i.[FACT_14],i.[FACT_15],i.[FACT_16],i.[FACT_17],i.[FACT_18],i.[FACT_19],i.[FACT_20],
	i.[FACT_21],i.[FACT_22],i.[FACT_23],i.[FACT_24])
	output i.[ForecastObject_UN], inserted.[ForecastObjectTypeMode_ID], i.[EventDate], i.[Priority]; --Возвращаем то что записали
	
	COMMIT	

END TRY
BEGIN CATCH
	--Ошибка, откатываем все изменения
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000)
	set @ErrMsg = ERROR_MESSAGE()
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, 16, 1)
END CATCH

drop table #inserted

--Запись в журнал
--insert into Expl_User_Journal (User_ID, EventDateTime, ApplicationType, CommentString, CUS_ID, EventString, ObjectID, ObjectName)
--select f.User_ID, DispatchDateTime, 1,'Прогнозирование',0,  
--case when c.Priority = 0 then 'Фиксация' else 'Сохранение' end + ' часов работы за ' + Convert(nvarchar, c.EventDate, 104), c.ForecastObject_UN, o.ForecastObjectName

--from
--(
--	select distinct User_ID, DispatchDateTime from @forecastWriterArchiveWorkInHoursTable
--) f
--cross apply
--(
--	select top 1 Priority, EventDate,ForecastObject_UN from @forecastWriterArchiveWorkInHoursTable where User_ID = f.User_ID and DispatchDateTime = f.DispatchDateTime
--) c
--join Forecast_Objects o on o.ForecastObject_UN = c.ForecastObject_UN

end
go
   grant EXECUTE on usp2_Forecast_WriteArchiveWorkInHours to [UserCalcService]
go