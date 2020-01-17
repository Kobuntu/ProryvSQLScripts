if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteIntegralValues')
          and type in ('P','PC'))
 drop procedure usp2_WriteIntegralValues
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ItegralValuesTableType' AND ss.name = N'dbo')
DROP TYPE [dbo].[ItegralValuesTableType]
-- Пересоздаем заново
CREATE TYPE [dbo].[ItegralValuesTableType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[TariffZone_ID] [int] NOT NULL,
	[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
	[Data] [float] NOT NULL,
	[IntegralType] [tinyint] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[Status] [int] NULL,
	[CUS_ID] [dbo].[CUS_ID_TYPE] NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDateTime] ASC,
	[ChannelType] ASC,
	[TariffZone_ID] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::ItegralValuesTableType to [UserMaster61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2011
--
-- Описание:
--
--		Пишем таблицу со значениями барабанов в базу
--
-- ======================================================================================

create proc [dbo].[usp2_WriteIntegralValues]
	@TIType tinyint, --Тип точки 
	@IntegralValuesTable ItegralValuesTableType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@sqlcommand nvarchar(4000),
@tableName nvarchar(100);

if (@TIType < 10) begin
	set @tableName = 'ArchComm_Integrals'
end else begin
	set @tableName = 'ArchBit_Integrals_' + ltrim(str(@TIType - 10,2));
end;

set @sqlcommand = N'MERGE ' + @tableName + ' AS a
USING (SELECT [TI_ID],[EventDateTime], dbo.usf2_Tariff_GetChannelByZoneID([ti_id], [eventDateTime], [ChannelType], [tariffZone_ID]) as Channel, [Data]
      ,[IntegralType],[DispatchDateTime],[Status],[CUS_ID]
 FROM @IntegralValuesTable where dbo.usf2_Tariff_GetChannelByZoneID([ti_id], [eventDateTime], [ChannelType], [tariffZone_ID]) > 0) AS n
ON a.TI_ID = n.TI_ID and a.EventDateTime = n.EventDateTime and a.ChannelType = n.Channel
WHEN MATCHED THEN 
	UPDATE SET [Status] = n.[Status], CUS_ID = n.CUS_ID, DispatchDateTime = n.[DispatchDateTime], [Data] = n.[Data]
WHEN NOT MATCHED THEN 
    INSERT ([TI_ID],[EventDateTime],[ChannelType],[Data]
      ,[IntegralType],[DispatchDateTime],[Status],[CUS_ID])
    VALUES (n.[TI_ID],n.[EventDateTime],n.[Channel],n.[Data]
      ,n.[IntegralType],n.[DispatchDateTime],n.[Status],n.[CUS_ID]);'

EXEC sp_executesql @sqlcommand, 
N'@IntegralValuesTable ItegralValuesTableType READONLY', @IntegralValuesTable

end
go
   grant EXECUTE on usp2_WriteIntegralValues to [UserCalcService]
go

grant EXECUTE on usp2_WriteIntegralValues to [UserMaster61968Service]
go