if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_OverflowControl')
          and type in ('P','PC'))
   drop procedure usp2_Rep_OverflowControl
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
--		август, 2012
--
-- Описание:
--
--		Данные для отчетов по Перми, по превышению чего либо (задается параметром)
--
-- ======================================================================================
create proc [dbo].[usp2_Rep_OverflowControl]

	@PSArray varchar(4000),
	@DTStart DateTime,
	@DTEnd DateTime,
	--Тип отчета
	-- 0 - Контроль скачков и провалов напряжения
	-- 2 - Контроль частоты сети и перегрузок по току
	-- 3 - Перечень отключений/ограничений
	@ReportType tinyint 
as

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@TIType tinyint;

select * 
into #tableTI
from usf2_Rep_Info_TI(@PSArray, null)


--Таблица в котору ложим результат аварийных сообщений
CREATE TABLE #tableArchCommEvents(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[EventCode] [int] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[ExtendedEventCode] [bigint] NULL,
	[CUS_ID] tinyint NOT NULL
 PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDateTime] ASC,
	[EventCode] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]


--Таблица в котору ложим время последнего интегрального значения
CREATE TABLE #tableLastIntegral(
	[TI_ID] int NOT NULL,
	[EventDateTime] [datetime] NOT NULL
 PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]


declare @codesList varchar(100);

--Определяемся с кодами которые будем искать
set @codesList = case @ReportType 
when 0 then '(33,39,41,47,49,55)' --Контроль скачков и провалов напряжения
--33 - Выход за минимальное предельно допустимое значение напряжения в фазе 1
--39 - Выход за максимальное предельно допустимое значение напряжения в фазе 1
--41 - Выход за минимальное предельно допустимое значение напряжения в фазе 2
--47 - Выход за максимальное предельно допустимое значение напряжения в фазе 2
--49 - Выход за минимальное предельно допустимое значение напряжения в фазе 3
--55 - Выход за максимальное предельно допустимое значение напряжения в фазе 3

when 2 then '(109,115)' --Контроль частоты сети и перегрузок по току
--97	Выход за лимит частоты сети
--109	Выход за минимальное предельно допустимое значение частоты сети
--110	Возврат за минимальное предельно допустимое значение частоты сети
--115	Выход за максимальное предельно допустимое значение частоты сети
--116	Возврат в максимальное предельно допустимое значение частоты сети

when 3 then '(18,19)' --Перечень отключений/ограничений
--18	Исчезновение питания
--19	Восстановление питания	

else '(-1)'
end;


DECLARE @ParmDefinition NVARCHAR(1000);
	SET @ParmDefinition = N'@TIType tinyint, @DTStart DateTime, @DTEnd DateTime';
	
DECLARE @SQLString NVARCHAR(4000);

declare @tableName NVARCHAR(100), @tableLastName NVARCHAR(100); 

declare @tablePrefix NVARCHAR(2000)
declare @tableSufix NVARCHAR(1000); 

SET @tablePrefix =N'insert into #tableArchCommEvents (TI_ID,EventDateTime,EventCode,DispatchDateTime,ExtendedEventCode,CUS_ID)
select TI_ID,EventDateTime,dbo.usf2_Event61968_ToLowLevelCode(EventCode,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID) as EventCode,DispatchDateTime,ExtendedEventCode,CUS_ID from '
      
set @tableSufix = ' where TI_ID in (select distinct TI_ID from #tableTI where titype = @TIType) and EventDateTime between @DTStart and @DTEnd
and (dbo.usf2_Event61968_ToLowLevelCode(EventCode,Event61968Domain_ID,Event61968DomainPart_ID,Event61968Type_ID,Event61968Index_ID) in ' + @codesList + ')' --выбираем все события определенные типом отчета

--Время последнего интегрального значения
declare @tableLastPrefix NVARCHAR(1000)
declare @tableLastSufix NVARCHAR(1000);

set @tableLastPrefix =N'insert into #tableLastIntegral (TI_ID, EventDateTime) select TI_ID, max(EventDateTime) from ';
set @tableLastSufix = N' where TI_ID in (select distinct TI_ID from #tableTI where titype = @TIType) group by TI_ID';

declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tableTI
  open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
	if (@TIType < 11) BEGIN
		set @tableName = 'ArchComm_Events_Journal_TI';
		set @tableLastName = 'ArchComm_Integrals';
	END ELSE BEGIN
		set @tableName = 'ArchBit_Events_Journal_' + ltrim(str(@TIType - 10,2));
		set @tableLastName = 'ArchBit_Integrals_' + ltrim(str(@TIType - 10,2));
	END;
	
	--Теперь берем информацию из архива
	SET @SQLString =  @tablePrefix + @tableName + @tableSufix;
	EXEC sp_executesql @SQLString, @ParmDefinition, @TIType, @DTStart, @DTEnd ;
	
	--print @SQLString;
	
	--Берем время последнего интегрального
	SET @SQLString =  @tableLastPrefix + @tableLastName + @tableLastSufix;
	EXEC sp_executesql @SQLString, @ParmDefinition, @TIType, @DTStart, @DTEnd ;

FETCH NEXT FROM t into @TIType
	end;
	CLOSE t
	DEALLOCATE t

--Выборка результата в виде 2х таблиц

--Выбираем таблицу с именами
select * from #tableTI 
where TI_ID in (select distinct TI_ID from #tableArchCommEvents) --Выбераем только ТИ имеющие архив превышений
order by TI_ID;

--Таблица со временем последнего интегрального значения
select * from #tableLastIntegral 
where TI_ID in (select distinct TI_ID from #tableArchCommEvents) --Выбераем только ТИ имеющие архив превышений
order by TI_ID;

--Таблица с архивными значениями
select * from #tableArchCommEvents order by TI_ID,EventDateTime, EventCode;

drop table #tableLastIntegral;
drop table #tableArchCommEvents;
drop table #tableTI;
go
   grant EXECUTE on usp2_Rep_OverflowControl to [UserCalcService]
go



