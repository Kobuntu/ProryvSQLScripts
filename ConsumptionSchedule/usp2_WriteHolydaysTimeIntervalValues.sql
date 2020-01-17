if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteHolydaysTimeIntervalValues')
          and type in ('P','PC'))
 drop procedure usp2_WriteHolydaysTimeIntervalValues
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'HolydaysTimeIntervalTableType' AND ss.name = N'dbo')
DROP TYPE [dbo].[HolydaysTimeIntervalTableType]
-- Пересоздаем заново
CREATE TYPE [dbo].[HolydaysTimeIntervalTableType] AS TABLE(
	[Year] [int] NOT NULL,
	[Month] [tinyint] NOT NULL,
	[DayMask] [bigint] NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[Year] ASC,
	[Month] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::HolydaysTimeIntervalTableType to [UserCalcService]
go

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
--		Пишем таблицу с масками праздничных или выходных дней в базу
--
-- ======================================================================================

create proc [dbo].[usp2_WriteHolydaysTimeIntervalValues]
	@HolydaysTimeIntervalTable HolydaysTimeIntervalTableType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off


MERGE InfoCalc_Holydays_Time_Intervals AS a
USING @HolydaysTimeIntervalTable  AS n
ON a.[Year] = n.[Year] and a.[Month] = n.[Month] 
WHEN MATCHED THEN 
	UPDATE SET DayMask = n.DayMask
WHEN NOT MATCHED THEN 
    INSERT ([Year], [Month], [DayMask])
    VALUES (n.[Year], n.[Month], n.[DayMask]);

end
go
   grant EXECUTE on usp2_WriteHolydaysTimeIntervalValues to [UserCalcService]
go
