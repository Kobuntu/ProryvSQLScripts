if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetGlobalTree_TI_CTI')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetGlobalTree_TI_CTI
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create  proc [dbo].[usp2_Info_GetGlobalTree_TI_CTI]
as
begin
	--ТИ
	select  distinct ti.TI_ID as ID,
		ti.TIName as StringName,
		ti.PS_ID as P_ID, 
		ti.TIType,
		ti.Commercial,
		ti.Voltage,
		ti.SectionNumber,
		Cast (0 as bit) as isCa,
		ISNULL(ti.TPCoefOurSide,1) as TPCoef,
		cast (ISNULL(it.Coeff,1) as float(26)) as Coeff,
		cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsInvert,
		ti.TP_ID,0 as ContrObject_ID,
		ti.Deleted
	from Info_TI ti
	left join
	(select StartDateTime,TI_ID, COEFU*COEFI as Coeff from Info_Transformators) it
	on it.TI_ID = ti.TI_ID and StartDateTime =
					(
					select max(StartDateTime) from Info_Transformators where TI_ID = it.TI_ID						
					)
	--select Section_ID,ti.TI_ID
	--КА
	union  --Объединяем с контрагентами
	select distinct cti.ContrTI_ID as ID,
		cti.TIName as StringName,
		cti.Contr_PS_ID as P_ID, 
		cti.TIType,
		cti.Commercial,
		cti.Voltage,
		cti.SectionNumber,
		Cast (1 as bit) as isCa,
		ISNULL(cti.TPCoefContr,1) as TPCoef,
		cast (1 as float(26)) as Coeff,
		cast((case when cti.AIATSCode=2 then 1 else 0 end) as bit),
		cti.TP_ID2,cti.ContrObject_ID,
		cti.Deleted
	from dbo.Info_Contr_TI cti
	order by  isCa, ID,P_ID
end
go
   grant EXECUTE on usp2_Info_GetGlobalTree_TI_CTI to [UserCalcService]
go
