if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetSendedEndDeviceControl')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetSendedEndDeviceControl
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetSendedEndDeviceControlPowerManage')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetSendedEndDeviceControlPowerManage
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2011
--
-- Описание:
--
--		Выбираем список SendedEndDeviceControl
--
-- ======================================================================================

create proc [dbo].[usp2_SM61968_GetSendedEndDeviceControl]

as
begin
	select * into #tmp from Expl_User_Journal_ManagePU_Request_List
	where ManageRequest_ID in 
	(
	select ManageRequest_ID from Expl_User_Journal_ManagePU_Request_List
	where ManageRequestStatus <= 1 and ManageRequestType = 6
	except 
	select ManageRequest_ID from Master61968_SendedEndDeviceControl
	) and ManageRequestType = 6


	select s.Slave61968System_ID, d.MRID, deviceRequest.ManualReadType, #tmp.* from #tmp
	join dbo.DeviceManage_Manual_ReadRequest deviceRequest on deviceRequest.ManageRequest_ID = #tmp.ManageRequest_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_To_Meters mt on mt.Meter_ID = deviceRequest.Meter_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets s on s.Slave61968EndDeviceAsset_ID = mt.Slave61968EndDeviceAsset_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_Description d on d.Slave61968EndDeviceAsset_ID = mt.Slave61968EndDeviceAsset_ID
	order by s.Slave61968System_ID

	drop table #tmp;
end
go
   grant EXECUTE on usp2_SM61968_GetSendedEndDeviceControl to [UserCalcService]
go

grant EXECUTE on usp2_SM61968_GetSendedEndDeviceControl to [UserMaster61968Service]
go

create proc [dbo].[usp2_SM61968_GetSendedEndDeviceControlPowerManage]

as
begin
	select * into #tmp from Expl_User_Journal_ManagePU_Request_List
	where ManageRequest_ID in 
	(
	select ManageRequest_ID from Expl_User_Journal_ManagePU_Request_List
	where ManageRequestStatus <= 1 and (ManageRequestType = 3 or ManageRequestType = 0)
	except 
	select ManageRequest_ID from Master61968_SendedEndDeviceControl
	) and (ManageRequestType = 3 or ManageRequestType = 0)


	select s.Slave61968System_ID, d.MRID, deviceRequest.DoSwitchOn,deviceRequest.DoSwitchOff,deviceRequest.PowerLimitDoSwitchOff,deviceRequest.PowerLimit,#tmp.* from #tmp
	join dbo.PowerManage_TI_Requested_Limit_State deviceRequest on deviceRequest.ManageRequest_ID = #tmp.ManageRequest_ID
	join dbo.Info_Meters_To_TI on Info_Meters_To_TI.TI_ID=deviceRequest.TI_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_To_Meters mt on mt.Meter_ID = Info_Meters_To_TI.Meter_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets s on s.Slave61968EndDeviceAsset_ID = mt.Slave61968EndDeviceAsset_ID
	join dbo.Master61968_SlaveSystems_EndDeviceAssets_Description d on d.Slave61968EndDeviceAsset_ID = mt.Slave61968EndDeviceAsset_ID
	where (Info_Meters_To_TI.FinishDateTime is NULL) or (Info_Meters_To_TI.FinishDateTime>Current_TimeStamp)
	order by s.Slave61968System_ID

	drop table #tmp;
end
go
   grant EXECUTE on usp2_SM61968_GetSendedEndDeviceControlPowerManage to [UserCalcService]
go

grant EXECUTE on usp2_SM61968_GetSendedEndDeviceControlPowerManage to [UserMaster61968Service]
go
