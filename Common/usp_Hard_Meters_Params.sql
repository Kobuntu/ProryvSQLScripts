if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Hard_Meters_Params')
          and type in ('P','PC'))
   drop procedure usp2_Hard_Meters_Params
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь, Боровиков Сергей
--
-- Дата создания:
--
--		Июнь, 2009 - Июль, 2013
--
-- Описание:
--
--		Дополнительные параметры для ТИ по одной ПС
--
-- ======================================================================================

create proc [dbo].[usp2_Hard_Meters_Params]
@PS_ID int = null,
	@TI_ID int = null
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@DateNow DateTime

set @DateNow = GetDate();

select ti.ti_id, ti.AbsentChannelsMask, tr.*
into #tis
from info_ti ti
outer apply 
(
	select top (1) cast(COEFU as float(26)) as COEFU, cast(COEFI as float(26)) as COEFI, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, 
		TransIType, TransUType, CoefIHigh, CoefILow, CoefUHigh, CoefULow,
		SerialTransIPhaseA, SerialTransIPhaseB, SerialTransIPhaseC, SerialTransUPhaseA, 
		SerialTransUPhaseB, SerialTransUPhaseC,
		TransIClass, TransUClass, TTA_ID, TTB_ID, TTC_ID, TNDesignation_ID, TNA_ID, TNB_ID, TNC_ID, UseBusSystem
	from dbo.Info_Transformators t
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and ISNULL(FinishDateTime, '21000101') >= @DateNow
	order by StartDateTime
)  tr
where (@PS_ID is not null and ps_id = @PS_ID) or (@TI_ID is not null and TI_ID = @TI_ID);



			
--Выборка трансформаторов тока
select Phase, TI_ID, ModelName, SerialNumber, FactLoad, NomLoad, Id, [Type], 
ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult,InstallationPlace,
NextCalibrationDate,ClassName,CalibrationInterval,RegistryNumber, CoeffHigh, CoeffLow

into #transInfo
from 
(
	select Phase= case 
			when httt.TT_ID = ti.TTA_ID then 'A'
			when httt.TT_ID = ti.TTB_ID then 'B'
			when httt.TT_ID = ti.TTC_ID then 'C'
			else '' end, ti.TI_ID, httt.ModelName, httt.SerialNumber, FactLoad, NomLoad, TT_ID as Id, 'T' as [Type] , 
	ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult,InstallationPlace,  
	NextCalibrationDate  = case when CalibrationDate is null or isnull(CalibrationInterval,0)<0 then null else DATEADD(m,CalibrationInterval,CalibrationDate) end,
	ClassName,CalibrationInterval,RegistryNumber,
	CoeffHigh= Dict_Transformators_TT_Model.CoefIHigh,CoeffLow= Dict_Transformators_TT_Model.CoefILow
	from 
	#tis ti
	join Hard_Transformators_TT httt on httt.TT_ID = ti.TTA_ID or httt.TT_ID = ti.TTB_ID or httt.TT_ID = ti.TTC_ID
	left join  Dict_Transformators_TT_Model on Dict_Transformators_TT_Model.TTModel_ID = httt.TTModel_ID	
	
) ti


--Выборка трансформаторов напряжения
--ТН на системе ШИН
insert into #transInfo
select Phase, TI_ID, ModelName, SerialNumber, FactLoad, NomLoad, Id, [Type], 
ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult,InstallationPlace,
NextCalibrationDate,ClassName,CalibrationInterval,RegistryNumber, CoeffHigh, CoeffLow
from
(
	select Phase= case 
			when httn.TN_ID = d.TNA_ID then 'A'
			when httn.TN_ID = d.TNB_ID then 'B'
			when httn.TN_ID = d.TNC_ID then 'C'
			else '' end,
			ti.TI_ID, httn.ModelName, httn.SerialNumber, FactLoad, NomLoad, TN_ID as Id, 'N' as [Type], 
			ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult,
			InstallationPlace = case when isnull(InstallationPlace,'') like '' then Dict_DistributingArrangement.StringName+'\'+Dict_BusSystem.StringName+'\'+d.StringName
			else InstallationPlace end,
			NextCalibrationDate  = case when CalibrationDate is null or isnull(CalibrationInterval,0)<0 then null else DATEADD(m,CalibrationInterval,CalibrationDate) end,
			ClassName,CalibrationInterval,RegistryNumber,
			CoeffHigh= Dict_Transformators_TN_Model.CoefUHigh,CoeffLow= Dict_Transformators_TN_Model.CoefULow
	from 
	#tis ti
	join Dict_TNDesignation d on d.TNDesignation_ID = ti.TNDesignation_ID
	join Hard_Transformators_TN httn on httn.TN_ID = d.TNA_ID or httn.TN_ID = d.TNB_ID or httn.TN_ID = d.TNC_ID
	join Dict_BusSystem on Dict_BusSystem.BusSystem_ID=d.BusSystem_ID	
	join Dict_DistributingArrangement on Dict_DistributingArrangement.DistributingArrangement_ID=Dict_BusSystem.DistributingArrangement_ID	
	left join  Dict_Transformators_TN_Model on Dict_Transformators_TN_Model.TNModel_ID = httn.TNModel_ID
	where ti.UseBusSystem = 1

) ti		
order by TI_ID, [Type], Phase

--ТН подключеные напрямую (встроенные)  - етсь такие
insert into #transInfo
select Phase, TI_ID, ModelName, SerialNumber, FactLoad, NomLoad, Id, [Type], 
ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult,InstallationPlace,
NextCalibrationDate,ClassName,CalibrationInterval,RegistryNumber , CoeffHigh, CoeffLow
from
(
	select Phase= case 
			when httt.TN_ID = ti.TNA_ID then 'A'
			when httt.TN_ID = ti.TNB_ID then 'B'
			when httt.TN_ID = ti.TNC_ID then 'C'
			else '' end,
		ti.TI_ID, httt.ModelName, httt.SerialNumber, FactLoad, NomLoad, TN_ID as Id, 'N' as [Type], 
		ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult,InstallationPlace,
		NextCalibrationDate  = case when CalibrationDate is null or isnull(CalibrationInterval,0)<0 then null else DATEADD(m,CalibrationInterval,CalibrationDate) end,
		ClassName,CalibrationInterval,RegistryNumber,
		CoeffHigh= Dict_Transformators_TN_Model.CoefUHigh,CoeffLow= Dict_Transformators_TN_Model.CoefULow
	from 
	#tis ti
	join Hard_Transformators_TN httt on (httt.TN_ID = ti.TNA_ID or httt.TN_ID = ti.TNB_ID or httt.TN_ID = ti.TNC_ID)
	left join  Dict_Transformators_TN_Model on Dict_Transformators_TN_Model.TNModel_ID = httt.TNModel_ID
	where ti.UseBusSystem = 0
) ti






select   ti.ti_id,hm.MeterType_ID, 
hm.MeterSerialNumber
	+ ISNULL((select top 1 ', №дисп.  ' + convert(varchar,AddxRemoteDisplay_ID) from  dbo.Hard_Meters_Addx_RemoteDisplay
	where  convert(varchar,AddxDevice_ID) like hm.MeterSerialNumber), '') as MeterSerialNumber, hm.LinkNumber,
CoeffTransformation = ISNULL(ti.COEFU * ti.COEFI,1),ti.COEFU, ti.COEFI
,dmet.MeterExtendedTypeName, ISNULL(imce.MeasuringComplexError,1) as MeasuringComplexError, dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
--трансфомраторы здесь только отображаем, т.к. в InfoTransforamtors уставревшие поля
TransIType = TTA.ModelName,
CoefIHigh=isnull(TTA.CoeffHigh, ti.CoefIHigh),  CoefILow= isnull(TTA.CoeffLow, ti.CoefILow), TransIClass =TTA.ClassName,
SerialTransIPhaseA=TTA.SerialNumber, SerialTransIPhaseB=TTB.SerialNumber, SerialTransIPhaseC= TTC.SerialNumber, 
TransUType= TNA.ModelName,
CoefUHigh=isnull(TNA.CoeffHigh, ti.CoefUHigh), CoefULow= isnull(TNA.CoeffLow, ti.CoefULow),TransUClass =TNA.ClassName, 
SerialTransUPhaseA=TNA.SerialNumber, SerialTransUPhaseB= TNB.SerialNumber, SerialTransUPhaseC= TNC.SerialNumber,
cntTI.ControlTI_ID, cntTI.IsInverted, FormulaUN1, FormulaUN2, 
FormulaUN3, FormulaUN4, ti.AbsentChannelsMask, hm.MeterModel_ID, hm.AllowTariffWrite, hm.CalibrationDate
from 
#tis ti
outer apply 
(
	select top (1) TI_ID,METER_ID,StartDateTime,ISNULL(FinishDateTime, '21000101') as FinishDateTime 
	from dbo.Info_Meters_TO_TI
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and ISNULL(FinishDateTime, '21000101') >= @DateNow
	order by StartDateTime desc
) mt
left join 
dbo.Hard_Meters hm on hm.Meter_ID = mt.Meter_ID
left join dbo.Dict_Meters_Extended_Types dmet
on dmet.MeterExtendedType_ID = hm.MeterExtendedType_ID

outer apply 
(
	select top(1) * from dbo.Info_MeasuringComplexError
	where TI_ID = ti.TI_ID	and StartDateTime <= @DateNow and ISNULL(FinishDateTime, '21000101') >= @DateNow
	order by StartDateTime
) imce
outer apply 
(
	select top(1) * from dbo.Info_TI_To_ControlTI 
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and ISNULL(FinishDateTime, '21000101') >= @DateNow
	order by StartDateTime
) cntTI
outer apply 
(
	select top(1) Formula_UN as FormulaUN1 from dbo.Info_TI_To_ControlFormula
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and FinishDateTime >= @DateNow and ChannelType = 1

) cntFormula1
outer apply 
(
	select top(1) Formula_UN as FormulaUN2 from dbo.Info_TI_To_ControlFormula
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and FinishDateTime >= @DateNow and ChannelType = 2

) cntFormula2
outer apply 
(
	select top(1) Formula_UN as FormulaUN3 from dbo.Info_TI_To_ControlFormula
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and FinishDateTime >= @DateNow and ChannelType = 3

) cntFormula3
outer apply 
(
	select top(1) Formula_UN as FormulaUN4 from dbo.Info_TI_To_ControlFormula
	where TI_ID = ti.TI_ID and StartDateTime <= @DateNow and FinishDateTime >= @DateNow and ChannelType = 4

) cntFormula4			
outer apply (select  top(1) * from  #transInfo  where TI_ID= ti.TI_ID  and [Type] like 'T' and Id is not null and Phase like 'A') as TTA 
outer apply (select  top(1) * from  #transInfo  where TI_ID= ti.TI_ID  and [Type] like 'T' and Id is not null and Phase like 'B') as TTB 
outer apply (select  top(1) * from  #transInfo  where TI_ID= ti.TI_ID  and [Type] like 'T' and Id is not null and Phase like 'C') as TTC
outer apply (select  top(1) * from  #transInfo  where TI_ID= ti.TI_ID  and [Type] like 'N' and Id is not null and Phase like '%A%') as TNA 
outer apply (select  top(1) * from  #transInfo  where TI_ID= ti.TI_ID  and [Type] like 'N' and Id is not null and Phase like '%B%') as TNB 
outer apply (select  top(1) * from  #transInfo  where TI_ID= ti.TI_ID  and [Type] like 'N' and Id is not null and Phase like '%C%') as TNC


select Phase, TI_ID, ModelName, SerialNumber, FactLoad, NomLoad, Id, [Type], 
ProductionDate,	CalibrationActNumber,	CalibrationDate,	CalibrationResult=isnull(CalibrationResult,0) ,InstallationPlace,
NextCalibrationDate, ClassName,CalibrationInterval,RegistryNumber, 
CoeffHigh= case when isnull(CoeffHigh,1)<1 then 1 else CoeffHigh end,
CoeffLow= case when isnull(CoeffLow,1)<1 then 1 else CoeffLow end 
from #transInfo
order by 
[type] desc, phase

drop table #tis
			        
end
go
   grant EXECUTE on usp2_Hard_Meters_Params to [UserCalcService]
go