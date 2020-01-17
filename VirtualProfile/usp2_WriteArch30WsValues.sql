if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteArch30WsValues')
          and type in ('P','PC'))
 drop procedure usp2_WriteArch30WsValues
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'Arch30WsValuesType' AND ss.name = N'dbo')
DROP TYPE [dbo].[Arch30WsValuesType]
-- Пересоздаем заново
CREATE TYPE [dbo].[Arch30WsValuesType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
	[DataSourceType] tinyint NOT NULL,
	[CAL_01] [float] NULL,
	[CAL_02] [float] NULL,
	[CAL_03] [float] NULL,
	[CAL_04] [float] NULL,
	[ValidStatus] [bigint] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[Status] [int] NULL,
	[CUS_ID] [dbo].[CUS_ID_TYPE] NOT NULL,
	
	PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDate] ASC,
	[ChannelType] ASC,
	[DataSourceType]
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::Arch30WsValuesType to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2014
--
-- Описание:
--
--		Пишем таблицу 30 минуток расчетного профиля в промежуточную таблицу замещения
--
-- ======================================================================================

create proc [dbo].[usp2_WriteArch30WsValues]
	@Arch30WsValuesTable Arch30WsValuesType READONLY, --Таблицу которую пишем в базу данных
	@IsCalc bit = 1
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

if (@IsCalc = 1) begin
	MERGE dbo.ArchBit_30_Values_WS as a
	USING 
		(
			select a.*, d.DataSource_Id, dbo.usf2_ReverseTariffChannel(0, a.[ChannelType], ti.AIATSCode,ti.AOATSCode,ti.RIATSCode,ti.ROATSCode, a.TI_ID, a.[EventDate], a.[EventDate]) as Channel
			from @Arch30WsValuesTable a
			join Expl_DataSource_List d on a.DataSourceType = d.DataSourceType
			join Info_TI ti on ti.TI_ID = a.TI_ID
		) n 
	ON a.TI_ID = n.TI_ID and a.EventDate = n.EventDate and a.ChannelType = n.Channel and a.DataSource_ID = n.DataSource_ID
	WHEN MATCHED THEN UPDATE SET a.CAL_03 = case when n.CAL_03 is null then a.CAL_03 else n.CAL_03 end, a.CAL_04 = case when n.CAL_04 is null then a.CAL_04 else n.CAL_04 end
	WHEN NOT MATCHED THEN 
	INSERT ([TI_ID]
			   ,[EventDate]
			   ,[ChannelType]
			   ,[DataSource_ID]
			   ,[VAL_01]
			   ,[VAL_02]
			   ,[VAL_03]
			   ,[VAL_04]
			   ,[ValidStatus]
			   ,[DispatchDateTime]
			   ,[CUS_ID]
			   ,[Status]
			   ,[CAL_01]
			   ,[CAL_02]
			   ,[CAL_03]
			   ,[CAL_04]) values 
			   ([TI_ID]
			   ,[EventDate]
			   ,[Channel]
			   ,[DataSource_ID]
			   ,NULL
			   ,NULL
			   ,NULL
			   ,NULL
			   ,ValidStatus
			   ,[DispatchDateTime]
			   ,[CUS_ID]
			   ,[Status]
			   ,[CAL_01]
			   ,[CAL_02]
			   ,[CAL_03]
			   ,[CAL_04]);
end else begin
	MERGE dbo.ArchBit_30_Values_WS as a
	USING 
		(
			select a.*, d.DataSource_Id, dbo.usf2_ReverseTariffChannel(0, a.[ChannelType], ti.AIATSCode,ti.AOATSCode,ti.RIATSCode,ti.ROATSCode, a.TI_ID, a.[EventDate], a.[EventDate]) as Channel
			from @Arch30WsValuesTable a
			join Expl_DataSource_List d on a.DataSourceType = d.DataSourceType
			join Info_TI ti on ti.TI_ID = a.TI_ID
		) n 
	ON a.TI_ID = n.TI_ID and a.EventDate = n.EventDate and a.ChannelType = n.Channel and a.DataSource_ID = n.DataSource_ID
	WHEN MATCHED THEN UPDATE SET a.VAL_03 = case when n.CAL_03 is null then a.CAL_03 else n.CAL_03 end, a.VAL_04 = case when n.CAL_04 is null then a.CAL_04 else n.CAL_04 end
	WHEN NOT MATCHED THEN 
	INSERT ([TI_ID]
			   ,[EventDate]
			   ,[ChannelType]
			   ,[DataSource_ID]
			   ,[CAL_01]
			   ,[CAL_02]
			   ,[CAL_03]
			   ,[CAL_04]
			   ,[ValidStatus]
			   ,[DispatchDateTime]
			   ,[CUS_ID]
			   ,[Status]
			   ,[VAL_01]
			   ,[VAL_02]
			   ,[VAL_03]
			   ,[VAL_04]) values 
			   ([TI_ID]
			   ,[EventDate]
			   ,[Channel]
			   ,[DataSource_ID]
			   ,NULL
			   ,NULL
			   ,NULL
			   ,NULL
			   ,ValidStatus
			   ,[DispatchDateTime]
			   ,[CUS_ID]
			   ,[Status]
			   ,[CAL_01]
			   ,[CAL_02]
			   ,[CAL_03]
			   ,[CAL_04]);
end
end
go
   grant EXECUTE on usp2_WriteArch30WsValues to [UserCalcService]
go