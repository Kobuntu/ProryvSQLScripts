if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParams')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParams
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
--		Данные необходимые для расчетов балансов ПС 
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetMetersParams]
(	
	@balancePSID varchar(22),	
	@datestart datetime,
	@dateend datetime
	
)

AS
declare
@Num int,
@TI_ID int,
@ChannelType int

begin

	select ibd.TI_ID,ibd.ChannelType,ti.TIName,hm.MeterSerialNumber,ti.Voltage, mce.MeasuringComplexError,ibd.BalanceSectionType_UN, ibd.Coef, ibl.ForAutoUse,ibl.HighLimit,ibl.LowerLimit,ti.IsChannelsInverted, tr.CoeffTransformation from 
	(select TI_ID,BalancePS_UN,BalanceSectionType_UN,ChannelType,Coef from dbo.Info_Balance_PS_Description where BalancePS_UN = @balancePSID) ibd
	left join 
	(select BalancePS_UN, ForAutoUse,HighLimit,LowerLimit from dbo.Info_Balance_PS_List where BalancePS_UN = @balancePSID) ibl
	on ibl.BalancePS_UN = ibd.BalancePS_UN
	left join 
	(select TI_ID,TIName,Voltage,cast((case ISNULL(AIATSCode,1) when 1 then 0 else 1 end)as bit) as IsChannelsInverted  from dbo.Info_TI) ti
	on ti.TI_ID = ibd.TI_ID
	left join 
	(select TI_ID,StartDateTime,FinishDateTime,MeasuringComplexError from dbo.Info_MeasuringComplexError) mce
  	on mce.TI_ID = ibd.TI_ID and mce.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_MeasuringComplexError
				where Info_MeasuringComplexError.TI_ID = ibd.TI_ID
					and StartDateTime <= @datestart 
					and FinishDateTime >= @dateend
				)
	left join 
	(select TI_ID,METER_ID,StartDateTime,FinishDateTime from dbo.Info_Meters_TO_TI) mt
  	on mt.TI_ID = ibd.TI_ID and mt.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_Meters_TO_TI
				where Info_Meters_TO_TI.TI_ID = ibd.TI_ID
					and StartDateTime <= @datestart 
					and FinishDateTime >= @dateend
				)
	left join 
	(select MeterSerialNumber,Meter_ID from dbo.Hard_Meters)hm
	on hm.Meter_ID = mt.Meter_ID
	left join
	(select ti_id,COEFU*COEFI as CoeffTransformation from dbo.Info_Transformators
	where ISNULL(Info_Transformators.FinishDateTime, '21000101') >= @DateStart
	and Info_Transformators.StartDateTime <= @DateEnd)  tr
	on	ti.TI_ID = tr.TI_ID

end
go
   grant exec on usp2_Info_GetMetersParams to [UserCalcService]
go
