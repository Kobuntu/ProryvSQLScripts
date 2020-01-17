if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Utils_GetTIParams')
          and type in ('P','PC'))
 drop procedure usp2_Utils_GetTIParams
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август, 2009
--
-- Описание:
--
--		Возвращаем параметры ТИ (название, напряжение, серийный номер счетчика)
--
-- ======================================================================================

create proc [dbo].[usp2_Utils_GetTIParams]
(	
	@ti_id_Array varchar(4000), --список ТИ
	@datestart datetime,
	@dateend datetime
)
AS
BEGIN 
	--Данные по ФСК
	SELECT ti_list.ti_id, ti.TIName, ti.Voltage,MeterSerialNumber, 1 as IsOurSide from  
	(select TInumber as ti_id from usf2_Utils_iter_intlist_to_table(@ti_id_Array) where CHnumber = 1)  ti_list
	join
	(select ti_id, Voltage, TIName from  info_ti) ti
	on ti.ti_id = ti_list.ti_id
	left join 
		(select TI_ID,METER_ID,StartDateTime,FinishDateTime from dbo.Info_Meters_TO_TI) [t3]
  		on ti.TI_ID = [t3].TI_ID and [t3].StartDateTime = 
		(
			select max(StartDateTime)
			from dbo.Info_Meters_TO_TI
			where Info_Meters_TO_TI.TI_ID = [ti].TI_ID
				and StartDateTime <= @datestart 
				and FinishDateTime >= @dateend
		) and [t3].FinishDateTime >= @dateend
	left join 
		(select MeterSerialNumber,Meter_ID from dbo.Hard_Meters) [t4]
		on [t3].Meter_ID = [t4].Meter_ID
	
	--Объединяем с данными по КА
	union 
	SELECT ti_list.ti_id, ti.TIName, ti.Voltage,'', 0 as IsOurSide from  
	(select TInumber as ti_id from usf2_Utils_iter_intlist_to_table(@ti_id_Array)  where CHnumber = 0)  ti_list
	join
	(select contrti_id, Voltage, TIName from  info_contr_ti) ti
	on ti.contrti_id = ti_list.ti_id
	

END


go
   grant EXECUTE on usp2_Utils_GetTIParams to [UserCalcService]
go