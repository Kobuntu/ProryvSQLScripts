if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_MetersLink')
          and type in ('P','PC'))
   drop procedure usp2_Rep_MetersLink
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
--		Сентябрь, 2012
--
-- Описание:
--
--		Перечень приборов учета и концентраторов, не выходивших на связь 
--
-- ======================================================================================
create proc [dbo].[usp2_Rep_MetersLink]
@PSArray varchar(4000),
@DTStart DateTime, 
@DTEnd DateTime
as
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@TIType tinyint;

--Таблица в котору ложим время последнего интегрального (получасового) значения
CREATE TABLE #tableLast(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NULL, -- Дата, время последнего опроса
	[SvazDateTime] [datetime] NULL, --Дата и время последнего выхода на связь
 PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]


DECLARE @ParmDefinition NVARCHAR(1000);
	SET @ParmDefinition = N'@TIType tinyint,@DTStart DateTime,@DTEnd DateTime';
	
DECLARE @SQLString NVARCHAR(4000), @tableLastIntegralName NVARCHAR(100), @tableLastHalfHourName NVARCHAR(100); 


select usf.*, mti.METER_ID
into #tableTI
from usf2_Rep_Info_TI(@PSArray, null) usf
left join Info_Meters_TO_TI mti on usf.TI_ID = mti.TI_ID and mti.StartDateTime = (select MAX(StartDateTime) from Info_Meters_TO_TI where TI_ID = usf.TI_ID)

--select * from #tableTI;


	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tableTI
	open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
	if (@TIType < 11) BEGIN
		insert into #tableLast (TI_ID, EventDateTime, SvazDateTime) 
		select distinct TI_ID, max(EventDateTime) OVER (PARTITION BY TI_ID), (select max(EventDateTime) from dbo.JournalDataCollect_Concentrators_Meters_Tree where Meter_ID = ah.METER_ID) as SvazDateTime
		from (
				select #tableTI.TI_ID, Max(ai.EventDateTime) OVER (PARTITION BY #tableTI.TI_ID)  as EventDateTime, METER_ID  from #tableTI 
				join ArchComm_Integrals ai on ai.TI_ID = #tableTI.TI_ID
				where titype = @TIType and ai.EventDateTime between @DTStart and @DTEnd and ai.IntegralType = 0
				union all
				select #tableTI.TI_ID, Max(ai.EventDate)  OVER (PARTITION BY #tableTI.TI_ID)  as EventDateTime, METER_ID  from #tableTI 
				join ArchComm_30_Values ai on ai.TI_ID = #tableTI.TI_ID
				where titype = @TIType and ai.EventDate between @DTStart and @DTEnd
			) ah;
			
	END ELSE BEGIN
		--Берем время последнего интегрального
		SET @SQLString =  'insert into #tableLast (TI_ID, EventDateTime, SvazDateTime) 
		select distinct TI_ID, max(EventDateTime) OVER (PARTITION BY TI_ID), (select max(EventDateTime) from dbo.JournalDataCollect_Concentrators_Meters_Tree where Meter_ID = ah.METER_ID) as SvazDateTime
		from (
				select #tableTI.TI_ID, Max(ai.EventDateTime) OVER (PARTITION BY #tableTI.TI_ID)  as EventDateTime, METER_ID  from #tableTI 
				join ' + 'ArchBit_Integrals_' + ltrim(str(@TIType - 10,2)) + ' ai on ai.TI_ID = #tableTI.TI_ID
				where titype = @TIType and ai.EventDateTime between @DTStart and @DTEnd
			) ah;';
	-- and ai.IntegralType = 0	
	--print @TIType;
	--print @SQLString;
	
	EXEC sp_executesql @SQLString, @ParmDefinition, @TIType, @DTStart,@DTEnd;
	END;
	
	FETCH NEXT FROM t into @TIType
	end;
	CLOSE t
	DEALLOCATE t	


select t.*, l.EventDateTime, l.SvazDateTime from #tableLast l
join #tableTI t on l.TI_ID = t.TI_ID
order by t.TI_ID;

drop table #tableTI;
drop table #tableLast;

go
  grant EXECUTE on usp2_Rep_MetersLink to [UserCalcService]
go