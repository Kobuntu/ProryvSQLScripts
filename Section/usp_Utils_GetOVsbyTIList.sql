if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Utils_GetNotCarriedOVsbyTIList')
          and type in ('P','PC'))
 drop procedure usp2_Utils_GetNotCarriedOVsbyTIList
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
--		ОВ с параметрами и именами для указанных ТИ
--
-- ======================================================================================

create proc [dbo].[usp2_Utils_GetNotCarriedOVsbyTIList]
(	
	@ti_id_Array varchar(max),
	@datestart datetime,
	@dateend datetime
)
AS
BEGIN 

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	SELECT ti.ti_id, ti.PS_ID, ti.TIName, ti.Voltage, ti.titype, mt.MeterSerialNumber from  
	(
		select distinct p.ti_id from Hard_OV_Positions_List position_ov 
		join Hard_OV_List p on p.OV_ID = position_ov.ov_id
		where position_ov.ti_id in (select TInumber from usf2_Utils_iter_intlist_to_table(@ti_id_Array))
	) p
	join info_ti ti on ti.ti_id = p.ti_id
	outer apply
	(
		select top 1 hm.MeterSerialNumber, m.Meter_ID from dbo.Info_Meters_TO_TI m
		join dbo.Hard_Meters hm on hm.Meter_ID = m.METER_ID
		where m.TI_ID = ti.TI_ID 
		and m.StartDateTime <= @dateend and ISNULL(m.FinishDateTime, '21000101') >= @datestart
		order by m.StartDateTime desc
	) mt
	--left join 
	--	(select TI_ID,METER_ID,StartDateTime,FinishDateTime from dbo.Info_Meters_TO_TI) [t3]
 -- 		on ti.TI_ID = [t3].TI_ID and [t3].StartDateTime = 
	--	(
	--		select max(StartDateTime)
	--		from dbo.Info_Meters_TO_TI
	--		where Info_Meters_TO_TI.TI_ID = [ti].TI_ID
	--			and StartDateTime <= @dateend
	--			and FinishDateTime >= @datestart 
	--	) and [t3].FinishDateTime >= @dateend
	--left join 
	--	(select MeterSerialNumber,Meter_ID from dbo.Hard_Meters) [t4]
	--	on [t3].Meter_ID = [t4].Meter_ID
	
	order by ti.PS_ID, ti.ti_id
END

go
   grant EXECUTE on usp2_Utils_GetNotCarriedOVsbyTIList to [UserCalcService]
go