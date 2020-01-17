if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParams2')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParams2
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
--		Январь, 2009
--
-- Описание:
--
--		Данные необходимые для расчетов балансов ПС 2
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetMetersParams2]
(	
	@balancePSIDList varchar(4000),	
	@datestart datetime,
	@dateend datetime
)

AS
declare
@Num int,
@TI_ID int,
@ChannelType int

begin
	select BalancePS_UN=STRnumber, ibd.TI_ID,ibd.ChannelType,ibd.ContrTI_ID,ibd.TP_ID,ibd.Formula_UN,

	case 
		when not ibd.TI_ID is null then ti.TIName
		when not cti.TIName is null then cti.TIName
		when not tf.TP_ID is null then tf.StringName
		when not fp.Formula_UN is null then fp.FormulaName
	end as TIName,

	case 
		when not ibd.TI_ID is null then mt.MeterSerialNumber
		when not tf.TP_ID is null then tf.MeterSerialNumber
		when not fp.Formula_UN is null then fp.MeterSerialNumber
	end as MeterSerialNumber,

	case 
		when not ibd.TI_ID is null then ti.Voltage  
		when not cti.TIName is null then cti.Voltage
		when not tf.TP_ID is null then tf.Voltage
		when not fp.Formula_UN is null then fp.Voltage
	end as Voltage,
	
	ISNULL(case 
		when not ibd.TI_ID is null then mce.MeasuringComplexError
		when not tf.TP_ID is null then mcetp.MeasuringComplexError
	end, 1) as MeasuringComplexError,

	ibd.BalanceSectionType_UN, 
	cast(ISNULL(ibd.Coef,1) as float(26)) as Coef, 
	ibl.ForAutoUse,
	ibl.HighLimit,ibl.LowerLimit, ibl.HighLimitValue,ibl.LowerLimitValue,

	case 
		when not ibd.TI_ID is null then ti.IsChannelsInverted  
		when not cti.TIName is null then cti.IsChannelsInverted
	end as IsChannelsInverted,
	
	cast(ISNULL(case 
		when not ibd.TI_ID is null then tr.CoeffTransformation
		when not tf.TP_ID is null then trtp.CoeffTransformation
	end, 1) as float(26)) as CoeffTransformation,

	case 
		when not ibd.TI_ID is null then ti.TI_ID
		when not tf.TP_ID is null then tf.TI_ID
		when not fp.Formula_UN is null then fp.TI_ID
	end as InfoTI_ID,
	ti.PS_ID,
	ibd.StringNumber,
	TIType
	from 
	--Список идентификаторов балансов 
	(select STRnumber from usf2_Utils_iter_strlist_to_table(@balancePSIDList)) usf
	inner join 
	--Таблица с описанием баланса
	(select TI_ID,ContrTI_ID,TP_ID,BalancePS_UN,BalanceSectionType_UN,ChannelType,Coef,Formula_UN,StringNumber from dbo.Info_Balance_PS_Description_2) ibd
	on STRnumber = ibd.BalancePS_UN
	--Характеристики самого баланса
	inner join dbo.Info_Balance_PS_List_2 ibl on ibl.BalancePS_UN = ibd.BalancePS_UN
	--Параметры ТИ в балансе
	left join 
	(select TI_ID,TP_ID,TIName,Voltage,PS_ID, cast((case ISNULL(AIATSCode,1) when 1 then 0 else 1 end)as bit) as IsChannelsInverted, TIType from dbo.Info_TI) ti
	on ti.TI_ID = ibd.TI_ID
	--Параметры ТИ КА в балансе
	left join 
	(select ContrTI_ID,TIName,Voltage,cast((case ISNULL(AIATSCode,1) when 1 then 0 else 1 end)as bit) as IsChannelsInverted  from dbo.Info_Contr_TI) cti
	on cti.ContrTI_ID = ibd.ContrTI_ID
	--Параметры ТП в балансе
	outer apply usf2_Info_GetTPParams(ibd.TP_ID, ibd.ChannelType,@datestart,@dateend,1,0, null) tf
	--Погрешность измерения ТП
	left join 
	(select TI_ID,StartDateTime,FinishDateTime,MeasuringComplexError from dbo.Info_MeasuringComplexError) mcetp
  	on mcetp.TI_ID = tf.TI_ID and mcetp.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_MeasuringComplexError
				where Info_MeasuringComplexError.TI_ID = tf.TI_ID
					and StartDateTime <= @dateend
					and FinishDateTime > @datestart 
				)  and mcetp.FinishDateTime >= @dateend
	--Коэфф трансформации ТП
	left join
	(select ti_id,COEFU*COEFI as CoeffTransformation,StartDateTime,ISNULL(FinishDateTime, '21000101') as FinishDateTime from dbo.Info_Transformators )  trtp
	on	tf.TI_ID = trtp.TI_ID and trtp.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_Transformators
				where Info_Transformators.TI_ID = trtp.TI_ID
					and StartDateTime <= @dateend 
					and ISNULL(FinishDateTime, '21000101') >= @datestart
				)   and trtp.FinishDateTime >= @dateend
				
	--Параметры формулы в балансе
	outer apply usf2_Info_GetFormulaParams(ibd.Formula_UN,@datestart,@dateend) fp
	
	--Погрешность измерения ТИ
	left join 
	(select TI_ID,StartDateTime,FinishDateTime,MeasuringComplexError from dbo.Info_MeasuringComplexError) mce
  	on mce.TI_ID = ibd.TI_ID and mce.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_MeasuringComplexError
				where Info_MeasuringComplexError.TI_ID = ibd.TI_ID
					and StartDateTime <= @dateend
					and FinishDateTime > @datestart 
				)  and mce.FinishDateTime >= @dateend
	outer apply
	--Номер счетчика
	(
		select top 1 mt.METER_ID,StartDateTime,FinishDateTime, hm.MeterSerialNumber from dbo.Info_Meters_TO_TI mt
		join dbo.Hard_Meters hm on hm.Meter_ID = mt.METER_ID
		where mt.TI_ID = ibd.TI_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') > @datestart
		order by StartDateTime desc 
	) mt
	--Коэфф трансформации
	left join
	(
		select ti_id,COEFU*COEFI as CoeffTransformation,StartDateTime,ISNULL(FinishDateTime, '21000101') as FinishDateTime from dbo.Info_Transformators )  tr
		on	ti.TI_ID = tr.TI_ID and tr.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.Info_Transformators
				where Info_Transformators.TI_ID = ti.TI_ID
					and StartDateTime <= @dateend 
					and ISNULL(FinishDateTime, '21000101') >= @datestart
				)   and tr.FinishDateTime >= @dateend
	order by STRnumber

end
go
   grant exec on usp2_Info_GetMetersParams2 to [UserCalcService]
go