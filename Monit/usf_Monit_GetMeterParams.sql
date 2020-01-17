if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Monit_GetMeterParams')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Monit_GetMeterParams
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2012
--
-- Описание:
--
--		Возвращаем параметры счетчика для форма анализа мониторинга
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Monit_GetMeterParams](@Meter_ID int, @Parrent_ID int, @ParrentMonitoringHierarchy tinyint, @MeterType_ID int) 
RETURNS @Results TABLE 
(
Meter_ID int,
ClockDiff bigint,
EventCode int
) 
AS BEGIN 
	DECLARE 
	@Concentrator_ID int,
	@ClockDiff bigint,
	@EventCode int,
	@tariff_id int;
	
	if @ParrentMonitoringHierarchy = 1 begin
		--Если счетчик привязан к Е422
		--последний разбег  времени Е422
		set @ClockDiff = (select top 1 ClockDiff from dbo.ArchComm_ClockDiff_Meter_E422 where Meter_ID=@Meter_ID and E422_ID = @Parrent_ID order by EventDateTime desc);
		--Текущее состояние (показываем лампочкой красный зеленый) Е422
		set @EventCode = (select EventCode from dbo.Monit_Current_State_Meters where Meter_ID = @Meter_ID and E422_ID = @Parrent_ID);
	end else if @ParrentMonitoringHierarchy = 3 begin 
		--Если счетчик привязан к УСПД

		--Определяемся с типом счетчика
		if (@MeterType_ID = 2004) begin
			--Если это ЭКОМ 3000
			set @ClockDiff = cast(
				(select top 1 [Param] * 1000 from JournalDataCollect_USPD_From_Meters 
				where USPD_ID = @Parrent_ID and Meter_ID = @Meter_ID  order by EventDateTime desc) as bigint)

		end else begin
			declare @isNotConverter bit;
			set @isNotConverter = 
				case ISNULL((select top 1 USPDType from Hard_USPD where USPD_ID = @Parrent_ID), 1)
					when 13 then 0 
					when 14 then 0 
					when 15 then 0 
					when 16 then 0 
					when 18 then 0 
					when 20 then 0 
					when 26 then 0 
				else 1 end
		
			--Убираем УСПД с конвертерами
			if (@isNotConverter = 1)set @ClockDiff = (select top 1 ClockDiff from dbo.ArchComm_ClockDiff_Center_USPD dc where dc.USPD_ID = @Parrent_ID order by EventDateTime desc)
			else set @ClockDiff = (select top 1 ClockDiff * 1000 from dbo.ArchComm_ClockDiff_Center_Meter a where a.Meter_ID=@Meter_ID	order by EventDateTime desc)
		end
		--Текущее состояние (показываем лампочкой красный зеленый) Е422
		set @EventCode = (select top 1 EventCode from dbo.Monit_Current_State_Meters_USPD where Meter_ID = @Meter_ID and USPD_ID = @Parrent_ID);
	end else if @ParrentMonitoringHierarchy = 2 begin
		--Если счетчик привязан к концентратору
		--последний разбег  времени концентратор
		set @ClockDiff = (select top 1 ClockDiff 
			from dbo.Hard_Concentrators hc
			join  dbo.ArchComm_ClockDiff_Meter_E422 a on a.E422_ID = hc.E422_ID
			where hc.Concentrator_ID = @Parrent_ID and a.Meter_ID=@Meter_ID
			order by EventDateTime desc);
		--Текущее состояние (показываем лампочкой красный зеленый) Е422
		set @EventCode = (select top 1 EventCode 
		from dbo.Hard_Concentrators hc
		join dbo.Monit_Current_State_Meters  a on a.E422_ID = hc.E422_ID
		where Meter_ID = @Meter_ID and hc.Concentrator_ID = @Parrent_ID);
	end else begin
	
		set @ClockDiff = (select top 1 ClockDiff 
			from dbo.ArchComm_ClockDiff_Center_Meter a where a.Meter_ID=@Meter_ID
			order by EventDateTime desc);
		set @EventCode = null;
	end;
		
	--Определяем каналы
	
	INSERT INTO @Results(Meter_ID, ClockDiff, EventCode) 
	VALUES(@Meter_ID, @ClockDiff, @EventCode) 
	RETURN 
END
go
grant select on usf2_Monit_GetMeterParams to [UserCalcService]
go