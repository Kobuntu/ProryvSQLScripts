if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Alarms_GetTopRegisteredDevices')
          and type in ('P','PC'))
   drop procedure usp2_Alarms_GetTopRegisteredDevices
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август, 2013
--
-- Описание:
--
--		Список идентификаторов устройств для рассылки уведомлений 
--
-- ======================================================================================
create proc [dbo].[usp2_Alarms_GetTopRegisteredDevices]

	@topNumbers int,
	@sortedNumberFrom int,
	@isPushNow bit
	
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select top(@topNumbers) [User_ID], DeviceType, Registration_ID, SortedNumber, case when @isPushNow = 1 then CAST(null as DateTime) else LastEventDateTime end as LastEventDateTime
	into #topDevices
	from  Alarms_RegisteredDevices where SortedNumber > @sortedNumberFrom and IsActive = 1

    select ISNULL(Numbers, 0) as [AlarmsCount], DeviceType, d.Registration_ID, SortedNumber, MaxEventDateTime, d.[User_ID]
	into #result
	from  #topDevices d
    left join (
				 SELECT Count(c.Alarm_ID) as [Numbers], a.[User_ID], d.Registration_ID, MAX(a.EventDateTime) as MaxEventDateTime
				 FROM dbo.Alarms_Current c 
				 join Alarms_Archive a on c.Alarm_ID = a.Alarm_ID 
				 join 
				 (
					select distinct [USER_ID], Registration_ID from #topDevices 
				 ) d on a.[User_ID] = d.[User_ID]
				 where Confirmed = 0
				group by a.[User_ID], d.Registration_ID
				--Если есть аварии пришедшие после последнего чтения, возвращаем общее количество аварий
				having MAX(a.EventDateTime)  > ISNULL((select top 1 LastEventDateTime from #topDevices where Registration_ID = d.Registration_ID and [User_ID] = a.[User_ID]), '20000101')
			) a on d.[User_ID] = a.[User_ID] and a.Registration_ID = d.Registration_ID

	--Обновление даты последней прочитанной аварии, для того чтобы по ним повторно не отправить push
	update [dbo].[Alarms_RegisteredDevices]
	SET
		LastEventDateTime = r.MaxEventDateTime
	FROM
		[dbo].[Alarms_RegisteredDevices] t
	INNER JOIN
		#result r
	ON
		t.[User_ID] = r.[User_ID] and t.Registration_ID = r.Registration_ID
	where r.MaxEventDateTime is not null and r.AlarmsCount > 0


	select AlarmsCount, DeviceType, Registration_ID, SortedNumber from #result

end
go
   grant EXECUTE on usp2_Alarms_GetTopRegisteredDevices to [UserCalcService]
go