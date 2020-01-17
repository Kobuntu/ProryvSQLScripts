if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetEndDeviceAssets')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetEndDeviceAssets
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
--		Выбираем список счетчиков для SM61968 GetEndDeviceAssets
--
-- ======================================================================================

create proc [dbo].[usp2_SM61968_GetEndDeviceAssets]

	@Meter_ID int,
	@LimitCount int

as
begin
	select top (@LimitCount)  hm.*, 
	cast(case when ls.DoSwitchOff = 1 then 0 else 1 end as bit) as IsConnected, --ConnectDisconnectFunction.isConnected 
	extt.MeterExtendedTypeName, -- EndDeviceModel.modelNumber
	
	suspd.EventDateTime as EventDateTimeUSPD, suspd.LastModifyEventDateTime as LastModifyEventDateTimeUSPD, suspd.EventCode as EventCodeUSPD, -- Status 
	sm.EventDateTime, sm.LastModifyEventDateTime, sm.EventCode, -- Из этих строк в дальнейшем выбираем самое последнее значение
	
	extt.PhaseCount,
	extt.ReverseFlowHandling
	
	from Hard_Meters hm
	join Info_Meters_TO_TI ti on hm.Meter_ID = ti.METER_ID and ti.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_Meters_TO_TI
				where Info_Meters_TO_TI.METER_ID = hm.Meter_ID
				and StartDateTime <= GETDATE() and (FinishDateTime is null or FinishDateTime >= GETDATE())
			)
	left join dbo.Dict_Meters_Extended_Types extt on extt.MeterExtendedType_ID =  hm.MeterExtendedType_ID and extt.MeterType_ID = hm.MeterType_ID
	left join PowerManage_TI_Current_Limit_State ls on ls.TI_ID = ti.TI_ID
	left join Monit_Current_State_Meters sm on sm.Meter_ID = hm.Meter_ID and sm.EventDateTime = 
	(
				select max(EventDateTime)
				from dbo.Monit_Current_State_Meters
				where METER_ID = hm.Meter_ID
	)
	left join Monit_Current_State_Meters_USPD suspd  on suspd.Meter_ID = hm.Meter_ID and suspd.EventDateTime = 
	(
				select max(EventDateTime)
				from dbo.Monit_Current_State_Meters_USPD
				where METER_ID = hm.Meter_ID
	)
	where hm.Meter_ID > @Meter_ID
	order by hm.Meter_ID;
end

go
   grant EXECUTE on usp2_SM61968_GetEndDeviceAssets to [UserCalcService]
go
grant EXECUTE on usp2_SM61968_GetEndDeviceAssets to [UserSlave61968Service]
go