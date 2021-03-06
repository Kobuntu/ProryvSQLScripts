if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Hard_OV_List')
          and type in ('P','PC'))
   drop procedure usp2_Hard_OV_List
go



-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Выбираем список обходных выключателей и время замещения для датчик TI_ID
--
-- ======================================================================================
create proc [dbo].[usp2_Hard_OV_List]
	@TI_ID int = null,
	@DTStart datetime,
	@DTEnd datetime
as

select ov.TI_ID, ovl.TI_ID as OV_ID, 
case when sw.StartDateTime < @DTStart then @DTStart else sw.StartDateTime end as StartDateTime,
case when sw.FinishDateTime > @DTEnd then @DTEnd else sw.FinishDateTime end as FinishDateTime,
TPCoefOurSide,AIATSCode, AOATSCode, RIATSCode, ROATSCode, IsCoeffTransformationDisabled, TIType
from Hard_OV_Positions_List ov
join Hard_OV_List ovl on ov.OV_ID = ovl.OV_ID
join ArchComm_OV_Switches sw WITH (NOLOCK) on ovl.OV_ID=sw.OV_ID and ov.OVPosition_ID=sw.OVPosition_ID
join Info_TI on Info_TI.TI_ID = ovl.TI_ID
where ov.TI_ID=@TI_ID and sw.StartDateTime <=  @DTEnd and sw.FinishDateTime > @DTStart

go
   grant EXECUTE on usp2_Hard_OV_List to [UserCalcService]
go