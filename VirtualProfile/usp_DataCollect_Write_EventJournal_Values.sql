if exists (select 1
          from sysobjects
          where  id = object_id('usp_DataCollect_Write_EventJournal_Values')
          and type in ('P','PC'))
   drop procedure usp_DataCollect_Write_EventJournal_Values
go

--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'DataCollectJournalEventValuesType' AND ss.name = N'dbo')
DROP TYPE [dbo].[DataCollectJournalEventValuesType]

-- Пересоздаем заново
CREATE TYPE DataCollectJournalEventValuesType AS TABLE 
(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[EventCode] [int] NOT NULL,

	[Event61968Domain_ID] [dbo].[EVENT61968_DOMAIN_ID_TYPE] NULL,
	[Event61968DomainPart_ID] [dbo].[EVENT61968_DOMAINPART_ID_TYPE] NULL,
	[Event61968Type_ID] [dbo].[EVENT61968_TYPE_ID_TYPE] NULL,
	[Event61968Index_ID] [dbo].[EVENT61968_INDEX_ID_TYPE] NULL,
	[Event61968Param] [varchar](255) NULL, 
	[DispatchDateTime] [datetime] NOT NULL,
	[ExtendedEventCode] [bigint] NULL,
	[CUS_ID] [dbo].[CUS_ID_TYPE] NOT NULL,
    PRIMARY KEY ([TI_ID], [EventDateTime], [EventCode])
)
GO

grant EXECUTE on TYPE::DataCollectJournalEventValuesType to [UserDataCollectorService]
go

create procedure usp_DataCollect_Write_EventJournal_Values 
@TIType tinyint, --Тип точки 
	@JournalEventTable DataCollectJournalEventValuesType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare
@sqlcommand nvarchar(4000),
@tableName nvarchar(100);

if (@TIType < 10) begin
	set @tableName = 'ArchComm_Events_Journal_TI'
end else begin
	set @tableName = 'ArchBit_Events_Journal_' + ltrim(str(@TIType - 10,2));
end;

set @sqlcommand = N'MERGE ' + @tableName + ' AS a
USING (SELECT TI_ID,EventDateTime,EventCode,DispatchDateTime,ExtendedEventCode,CUS_ID
		   ,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID,Event61968Param	
	FROM @JournalEventTable ) AS n
ON a.TI_ID = n.TI_ID and a.EventDateTime = n.EventDateTime and a.EventCode = n.EventCode
WHEN MATCHED THEN 
	UPDATE SET [Event61968Domain_ID] = n.[Event61968Domain_ID]
	, Event61968DomainPart_ID = n.Event61968DomainPart_ID
	, Event61968Type_ID = n.[Event61968Type_ID]
	, [Event61968Index_ID] = n.[Event61968Index_ID]
	, [Event61968Param] = n.[Event61968Param]
    , [ExtendedEventCode] = n.[ExtendedEventCode]
    , [CUS_ID] = n.[CUS_ID]
WHEN NOT MATCHED THEN 
    INSERT (TI_ID,EventDateTime,EventCode,DispatchDateTime,ExtendedEventCode,CUS_ID,Event61968Domain_ID
           ,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID,Event61968Param)
    VALUES (n.TI_ID,n.EventDateTime,n.EventCode,n.DispatchDateTime,n.ExtendedEventCode,n.CUS_ID
			,n.Event61968Domain_ID
			,n.Event61968DomainPart_ID
			,n.Event61968Type_ID
			,n.Event61968Index_ID
           ,n.Event61968Param);'

EXEC sp_executesql @sqlcommand, 
N' @JournalEventTable dbo.DataCollectJournalEventValuesType READONLY', @JournalEventTable
end
go

grant EXECUTE on usp_DataCollect_Write_EventJournal_Values to UserDataCollectorService
go