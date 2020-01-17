if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Поиск_счетчиков_на_одном_УСПД_более_1_раза')
          and type in ('P','PC'))
   drop procedure usp2_Поиск_счетчиков_на_одном_УСПД_более_1_раза
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Барченко Николай
--
-- Дата создания:
--
--		Декабрь, 2016
--
-- Описание:
--
--		Поиск счетчиков на одном УСПД более 1 раза за последнюю неделю
--
-- ======================================================================================
create proc [dbo].[usp2_Поиск_счетчиков_на_одном_УСПД_более_1_раза]
	@USPDType tinyint
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	declare @dtNow DateTime;
	set @dtNow = DATEADD(day, -7, GetDate());

	select distinct jdc.USPD_ID, jdc.MeterSerialNumber, Count(jdc.Meter_ID) as КоличествоДубликатов 
	from JournalDataCollect_USPD_Discovered_Meters jdc
	join Hard_USPD hu on hu.USPD_ID=jdc.USPD_ID
	join Dict_USPD_Extended_Types dut on dut.USPDType=hu.USPDType
	where dut.USPDType=@USPDType and jdc.EventDateTime in
	(
	select top 1 EventDateTime from JournalDataCollect_USPD_Discovered_Meters
	where Meter_ID=jdc.Meter_ID and USPD_ID=jdc.USPD_ID and EventDateTime>@dtNow
	order by EventDateTime desc
	)
	group by jdc.USPD_ID,jdc.MeterSerialNumber
	having Count(jdc.Meter_ID) > 1
	
end

go
  grant EXECUTE on usp2_Поиск_счетчиков_на_одном_УСПД_более_1_раза to [UserCalcService]
go