if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetMeterReadingParams')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetMeterReadingParams
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2011
--
-- Описание:
--
--		На вход подаем список идентификаторов счетчиков, на выходы параметры для ф-ии GetMeterReading
--
-- ======================================================================================

create proc [dbo].[usp2_SM61968_GetMeterReadingParams]

	@MeterIDArray varchar(4000),
	@IncludeReadings bit, --Собирем информацию тарифам и тарифным каналам
	@DTStart DateTime,
	@DTEnd DateTime

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

DECLARE @ParmDefinition NVARCHAR(1000);
SET @ParmDefinition = N'@MeterIDArray varchar(4000),@DTStart DateTime,@DTEnd DateTime'
DECLARE @SQLString NVARCHAR(4000);

SET @SQLString = N'select usf.Items as Meter_ID, ti.TI_ID';

if @IncludeReadings = 1 begin
	SET @SQLString = @SQLString + ', tz.TariffZone_ID,  tz.ChannelType1, tz.ChannelType2, tz.ChannelType3, tz.ChannelType4';
end;
	
SET @SQLString = @SQLString + ' from usf2_Utils_Split(@MeterIDArray, '','') usf
	left join Info_Meters_TO_TI ti on usf.Items = ti.METER_ID and ti.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_Meters_TO_TI
				where Info_Meters_TO_TI.METER_ID = usf.Items
				and Info_Meters_TO_TI.StartDateTime <= @DTEnd and (FinishDateTime is null OR @DTStart <= Info_Meters_TO_TI.FinishDateTime)
			)';
			
if @IncludeReadings = 1 begin
	SET @SQLString = @SQLString + 'left join dbo.DictTariffs_ToTI ttt on ttt.TI_ID = ti.TI_ID and ttt.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.DictTariffs_ToTI
				where DictTariffs_ToTI.TI_ID = ti.TI_ID
				and DictTariffs_ToTI.StartDateTime <= @DTEnd and (FinishDateTime is null OR @DTStart <= DictTariffs_ToTI.FinishDateTime)
			)
	left join dbo.DictTariffs_Zones tz on tz.Tariff_ID = ttt.Tariff_ID';
end;	

	SET @SQLString = @SQLString + ' Order by ti.TI_ID;'
	
	--select @SQLString;
	
	EXEC sp_executesql @SQLString, @ParmDefinition, @MeterIDArray,@DTStart,@DTEnd
	
	
end
go
   grant EXECUTE on usp2_SM61968_GetMeterReadingParams to [UserCalcService]
go
grant EXECUTE on usp2_SM61968_GetMeterReadingParams to [UserMaster61968Service]
go
grant EXECUTE on usp2_SM61968_GetMeterReadingParams to [UserSlave61968Service]
go