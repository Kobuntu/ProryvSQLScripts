if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetRequestListForMeterReading')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetRequestListForMeterReading
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2012
--
-- Описание:
--
--		Выбираем точки по которым принудительно вычитываем архивные значения
--
-- ======================================================================================

create proc [dbo].[usp2_SM61968_GetRequestListForMeterReading]

as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	select * into #tmp from Expl_User_Journal_ManagePU_Request_List
	where ManageRequest_ID in 
	(
	select ManageRequest_ID from Expl_User_Journal_ManagePU_Request_List
	where ManageRequestStatus <= 1 and ManageRequestType = 10
	except 
	select ManageRequest_ID from Master61968_SendedEndDeviceControl
	) and ManageRequestType = 10


	--Выбираем все ПС
	select s.Slave61968System_ID, d.MRID, rt.StartDateTime, rt.FinishDateTime,rt.IncludeEndDeviceEvents, rt.IncludeIntervalBlocks,
	rt.IncludePowerSuppyObjects, rt.IncludeReadings, #tmp.* from #tmp
	join dbo.Master61968_GetMeterReadings_Tasks rt on rt.ManageRequest_ID = #tmp.ManageRequest_ID
	join dbo.Info_TI ti on (rt.PS_ID is not null and rt.PS_ID = ti.PS_ID) or (rt.TI_ID is not null and rt.TI_ID = ti.TI_ID)
	join dbo.Info_Meters_To_TI mti on mti.TI_ID=ti.TI_ID and mti.StartDateTime = 
	(
		select Max(StartDateTime) from dbo.Info_Meters_To_TI 
		where  TI_ID = ti.TI_ID and StartDateTime < Current_TimeStamp and (FinishDateTime is NULL or FinishDateTime>Current_TimeStamp)
	) 
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_To_Meters mt on mt.Meter_ID = mti.Meter_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets s on s.Slave61968EndDeviceAsset_ID = mt.Slave61968EndDeviceAsset_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_Description d on d.Slave61968EndDeviceAsset_ID = mt.Slave61968EndDeviceAsset_ID
	order by s.Slave61968System_ID

	drop table #tmp;
end
go
   grant EXECUTE on usp2_SM61968_GetRequestListForMeterReading to [UserCalcService]
go

grant EXECUTE on usp2_SM61968_GetRequestListForMeterReading to [UserMaster61968Service]
go
