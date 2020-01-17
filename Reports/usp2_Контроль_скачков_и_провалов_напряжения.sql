if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Контроль_скачков_и_провалов_напряжения')
          and type in ('P','PC'))
   drop procedure usp2_Контроль_скачков_и_провалов_напряжения
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
--		Июль, 2012
--
-- Описание:
--
--		Контроль скачков и провалов напряжения на объектах потребителей
--
-- ======================================================================================

create proc [dbo].[usp2_Контроль_скачков_и_провалов_напряжения]
	@ID int, --Идентификатор
	@TypeHierarchy int, --Уровень родителя (enumTypeHierarchy)
	@DTStart DateTime,
	@DTEnd DateTime
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Информация по ТИ
create table #tableTI
(
 TI_ID int,
 TIType tinyint,
 PS_ID int,
 HierLev3_ID int,
 HierLev2_ID int,
 HierLev1_ID int,
 TIName varchar(1024),
 
 PSName nvarchar(255),
 H3Name nvarchar(255),
 H2Name nvarchar(255),
 H1Name nvarchar(255),
 
 MeterTypeName nvarchar(128),
 MeterSerialNumber nvarchar(255),
 PRIMARY KEY CLUSTERED ([TIType], [ti_id])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)
	insert into #tableTI
	select ti.TI_ID, ti.TIType, ti.PS_ID, h3.HierLev3_ID, h2.HierLev2_ID, h1.HierLev1_ID, TIName, 
	ps.StringName,
	h3.StringName,
	h2.StringName,
	h1.StringName,
	mt.MeterTypeName, hm.MeterSerialNumber from Info_TI ti
	join Dict_PS ps on ti.PS_ID = ps.PS_ID
	join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID
	join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID
	join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID
	left join Info_Meters_TO_TI mti on mti.TI_ID = ti.TI_ID and StartDateTime = (
	select MAX(StartDateTime) from Info_Meters_TO_TI where TI_ID = ti.TI_ID)
	left join Hard_Meters hm on hm.Meter_ID = mti.METER_ID
	left join dbo.Dict_Meters_Types mt on mt.MeterType_ID = hm.MeterType_ID
	where 
	(@TypeHierarchy = 3 and ti.PS_ID = @ID) --Дом
	OR (@TypeHierarchy = 0 and h1.HierLev1_ID = @ID)-- HierLev1
	OR (@TypeHierarchy = 1 and h2.HierLev2_ID = @ID)-- HierLev2
	OR (@TypeHierarchy = 2 and h3.HierLev3_ID = @ID)-- HierLev3

declare
@TIType tinyint;

--Таблица в котору ложим результат
CREATE TABLE #tableArchCommEvents(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[EventCode] [int] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[ExtendedEventCode] [bigint] NULL,
	[CUS_ID] tinyint NOT NULL,
	[Event61968Domain_ID] tinyint NULL,
	[Event61968DomainPart_ID] tinyint NULL,
	[Event61968Type_ID] tinyint NULL,
	[Event61968Index_ID] int NULL,
	[Event61968Param] nvarchar(255) NULL
 PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDateTime] ASC,
	[EventCode] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]


DECLARE @ParmDefinition NVARCHAR(1000);
	SET @ParmDefinition = N'@TIType tinyint, @DTStart DateTime, @DTEnd DateTime';
	
DECLARE @SQLString NVARCHAR(4000);

declare @tableName NVARCHAR(100); 

declare @tablePrefix NVARCHAR(1000)
declare @tableSufix NVARCHAR(1000); 

SET @tablePrefix =N'insert into #tableArchCommEvents (TI_ID,EventDateTime,EventCode,DispatchDateTime,ExtendedEventCode,CUS_ID,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID,Event61968Param)
select TI_ID,EventDateTime,EventCode,DispatchDateTime,ExtendedEventCode,CUS_ID,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID,Event61968Param from '
      
set @tableSufix = ' where TI_ID in (select distinct TI_ID from #tableTI where titype = @TIType) and EventDateTime between @DTStart and @DTEnd
and (dbo.usf2_Event61968_ToLowLevelCode(EventCode,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID) between 0 and 5 
or dbo.usf2_Event61968_ToLowLevelCode(EventCode,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID) between 33 and 56 
or dbo.usf2_Event61968_ToLowLevelCode(EventCode,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID) between 86 and 94)' --выбираем все события связанные с напряжени

--0 and 5 Общее сообщение об отсутствии(восстановлении)
--33 and 56 Выход(возврат) за минимальное (максимально) по определенной фазе
--86 and 94 Отсутствие на определенной фазе

--select COUNT(*) from #tableTI;

declare t cursor FAST_FORWARD for select distinct TIType from #tableTI
  open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
	if (@TIType < 11) BEGIN
		set @tableName = 'ArchComm_Events_Journal_TI';
	END ELSE BEGIN
		set @tableName = 'ArchBit_Events_Journal_' + ltrim(str(@TIType - 10,2));
	END;
	
	--select * from #tableTI where TIType = @TIType
--Теперь берем информацию из архива

	SET @SQLString =  @tablePrefix + @tableName + @tableSufix;
	EXEC sp_executesql @SQLString, @ParmDefinition, @TIType, @DTStart, @DTEnd ;
	
	--select @SQLString;

FETCH NEXT FROM t into @TIType
	end;
	CLOSE t
	DEALLOCATE t


--Выбираем таблицу с именами
select * from #tableTI;

--Таблица с архивными значениями
select * from #tableArchCommEvents;

	--select 
	--TIName as Наименование_ТИ,PSName as Дом,H3Name as Улица,H2Name as Район,H3Name as Город,MeterTypeName, 
	--MeterTypeName as Прибор_учета_тип,
	--MeterSerialNumber as Прибор_учета_номер
	----Здесь поля из архива
	--from #tableTI ti
	--join #tableArchCommEvents arch on ti.TI_ID = arch.TI_ID
	--order by  ti.TI_ID;

drop table #tableArchCommEvents;
drop table #tableTI;
end
go
   grant EXECUTE on usp2_Контроль_скачков_и_провалов_напряжения to [UserCalcService]
go



