if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetMeterReadingArray')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetMeterReadingArray
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2012
--
-- Описание:
--
--		Выбираем данные по нескольким точкам за промежуток времени
--
-- ======================================================================================
create proc [dbo].[usp2_SM61968_GetMeterReadingArray]

@MeterArray varchar(4000),
@DTStart dateTime,
@DTEnd dateTime

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select distinct Items 
into #metersId
from usf2_Utils_Split(@MeterArray, ',');

select COUNT(Items) as [Count] from #metersId;

declare @Meter_ID int

declare itmCursor cursor FAST_FORWARD for select Items from #metersId
	  open itmCursor;
	FETCH NEXT FROM itmCursor into @Meter_ID
	WHILE @@FETCH_STATUS = 0
	BEGIN
		exec usp2_SM61968_GetMeterReading @Meter_ID, @DTStart, @DTEnd
	FETCH NEXT FROM itmCursor into @Meter_ID
	END
	CLOSE itmCursor
	DEALLOCATE itmCursor
	
drop table #metersId;
end
go
   grant EXECUTE on usp2_SM61968_GetMeterReadingArray to [UserCalcService]
go
grant EXECUTE on usp2_SM61968_GetMeterReadingArray to [UserSlave61968Service]
go
grant EXECUTE on usp2_SM61968_GetMeterReadingArray to [UserMaster61968Service]
go
