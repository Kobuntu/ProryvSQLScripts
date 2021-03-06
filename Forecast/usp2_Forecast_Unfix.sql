--Создаем тип, если его нет
IF  NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ForecastWriterType' AND ss.name = N'dbo') BEGIN
	CREATE TYPE [dbo].[ForecastWriterType] AS TABLE(
	[ForecastObject_UN] varchar(22) NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[User_ID] varchar(22) NOT NULL,
	[DispatchDateTime] DateTime NOT NULL,
	[AUTOBITS] [bigint],
	[MANUALBITS] [bigint],
	[FACTBITS] [bigint]
)
END
go

grant EXECUTE on TYPE::ForecastWriterType to [UserCalcService]
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_Unfix')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_Unfix
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

---- ======================================================================================
---- Автор:
----
----		Малышев Игорь
----
---- Дата создания:
----
----		Январь, 2017
----
---- Описание:
----
----		Пишем таблицу 30 минуток прогнозируемого значения в БД
----
---- ======================================================================================
create proc [dbo].[usp2_Forecast_Unfix]
	@forecastWriteTable ForecastWriterType  READONLY, --Объекты, которые расфиксируем
	@treeID int = null

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Проверка всех разрешений
declare @objectUnRight nvarchar(max);
set @objectUnRight = (select dbo.usf2_Forecast_UserHasRight_ForecastFix(@forecastWriteTable, 'CBBB7316-9875-44B8-9178-E7B6603B6C74', @treeID)) --ForecastUnfix

if (len(@objectUnRight) > 1) begin
	set @objectUnRight = 'Недостаточно прав <Сохранение/фиксирование> на объекты: ' + @objectUnRight;
	RAISERROR(@objectUnRight, 16, 1)
return;
end

create table #inserted
(
	[ForecastObject_UN] varchar(22) NOT NULL,
	[EventDate] DateTime NOT NULL,
	[Priority] tinyint NOT NULL
)

update [dbo].[Forecast_Archive_Journal]
set Priority = newPriority
output inserted.ForecastObject_UN, inserted.EventDate, inserted.[Priority] --Возвращаем идентификаторы объектов, которые обновили и даты
into #inserted
from [dbo].[Forecast_Archive_Journal] u
inner join 
(
select f.*, ISNULL((select Min(Priority) - 1 from [dbo].[Forecast_Archive_Journal] where ForecastObject_UN = f.ForecastObject_UN and EventDate = f.EventDate and Priority >0), 254) as newPriority
from @forecastWriteTable f
join [dbo].[Forecast_Archive_Journal] a on a.ForecastObject_UN = f.ForecastObject_UN and a.EventDate = f.EventDate and a.Priority = 0
) j on u.ForecastObject_UN = j.ForecastObject_UN and u.EventDate = j.EventDate and u.Priority = 0

select * from #inserted

end
go
   grant EXECUTE on usp2_Forecast_Unfix to [UserCalcService]
go