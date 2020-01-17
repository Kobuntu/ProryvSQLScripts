if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetManageRequestedMeters')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetManageRequestedMeters
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
--		Выбираем данные по одной точке за промежуток времени
--
-- ======================================================================================
create proc [dbo].[usp2_SM61968_GetManageRequestedMeters]

@ManageRequest_ID uniqueidentifier,
@DTStart dateTime,
@DTEnd dateTime

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@Meter_ID int

set @Meter_ID = (select METER_ID from dbo.DeviceManage_Manual_ReadRequest where ManageRequest_ID = @ManageRequest_ID);

exec usp2_SM61968_GetMeterReading @Meter_ID, @DTStart, @DTEnd

end
go
   grant EXECUTE on usp2_SM61968_GetManageRequestedMeters to [UserCalcService]
go
grant EXECUTE on usp2_SM61968_GetManageRequestedMeters to [UserSlave61968Service]
go
grant EXECUTE on usp2_SM61968_GetManageRequestedMeters to [UserMaster61968Service]
go
