if exists (select 1
          from sysobjects
          where  id = object_id('vw_ArchCalcBitEventJournals')
          and type in ('V'))
   drop view vw_ArchCalcBitEventJournals
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
--		Получасовые значения ТИ
--
-- ======================================================================================
CREATE view [dbo].[vw_ArchCalcBitEventJournals] 
WITH SCHEMABINDING
as
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
0 as TiType
FROM dbo.ArchComm_Events_Journal_TI
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
11 as TiType
FROM dbo.ArchBit_Events_Journal_1
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
12 as TiType
FROM dbo.ArchBit_Events_Journal_2
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
13 as TiType
FROM dbo.ArchBit_Events_Journal_3
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
14 as TiType
FROM dbo.ArchBit_Events_Journal_4
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
15 as TiType
FROM dbo.ArchBit_Events_Journal_5
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
16 as TiType
FROM dbo.ArchBit_Events_Journal_6
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
17 as TiType
FROM dbo.ArchBit_Events_Journal_7
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
18 as TiType
FROM dbo.ArchBit_Events_Journal_8
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
19 as TiType
FROM dbo.ArchBit_Events_Journal_9
UNION ALL
SELECT ti_id, EventDateTime, EventCode, Event61968Param, [ExtendedEventCode],
20 as TiType
FROM dbo.ArchBit_Events_Journal_10
GO

   grant select on vw_ArchCalcBitEventJournals to [UserCalcService]
go