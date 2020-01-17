if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ImportNSI_InterrogatoryList')
          and type in ('P','PC'))
   drop procedure [usp2_ImportNSI_InterrogatoryList]
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss 
ON st.schema_id = ss.schema_id WHERE st.name = N'ImportNSI_InterrogatoryList_TableType' AND ss.name = N'dbo')
DROP TYPE [dbo].[ImportNSI_InterrogatoryList_TableType]



Create TYPE [dbo].[ImportNSI_InterrogatoryList_TableType] 
AS TABLE(
	TINumber int NULL,
	TIShemNumber int NULL,
	TIVoltage float NULL,
	IsSmallTI int,
	IsCommercial int,	
	AbsentChannelsMask tinyint,
	AbsentChannelsMaskString varchar(200),	
	[MeterSerialNumber] [nvarchar](255) NULL,	
	[MeterCalibrationDate] [nvarchar](255) NULL,
	[MeterManufacturedDate]  int NULL,	
	[TTAType] [nvarchar](255) NULL,
	[TTBType] [nvarchar](255) NULL,
	[TTCType] [nvarchar](255) NULL,
	[TTCoeff] [nvarchar](255) NULL,
	[TTAClass] [nvarchar](255) NULL,
	[TTBClass] [nvarchar](255) NULL,
	[TTCClass] [nvarchar](255) NULL,
	[TTAReestrNumber] [nvarchar](255) NULL,
	[TTBReestrNumber] [nvarchar](255) NULL,
	[TTCReestrNumber] [nvarchar](255) NULL,
	[TTANomLoad] [nvarchar](255) NULL,
	[TTAFactLoad] [nvarchar](255) NULL,
	[TTBNomLoad] [nvarchar](255) NULL,
	[TTBFactLoad] [nvarchar](255) NULL,
	[TTCNomLoad] [nvarchar](255) NULL,
	[TTCFactLoad] [nvarchar](255) NULL,
	[TTASerialNumber] [nvarchar](255) NULL,
	[TTBSerialNumber] [nvarchar](255) NULL,
	[TTCSerialNumber] [nvarchar](255) NULL,
	[TTACalibrationDate] [nvarchar](255) NULL,
	[TTBCalibrationDate] [nvarchar](255) NULL,
	[TTCCalibrationDate] [nvarchar](255) NULL,
	[TTAMPI] int NULL,
	[TTBMPI] int NULL,
	[TTCMPI] int NULL,
	[TTPlace] [nvarchar](400) NULL,
	[TNAType] [nvarchar](255) NULL,
	[TNBType] [nvarchar](255) NULL,
	[TNCType] [nvarchar](255) NULL,
	[TNCoeff] [nvarchar](255) NULL,
	[TNAClass] [nvarchar](255) NULL,
	[TNBClass] [nvarchar](255) NULL,
	[TNCClass] [nvarchar](255) NULL,
	[TNAReestrNumber] [nvarchar](255) NULL,
	[TNBReestrNumber] [nvarchar](255) NULL,
	[TNCReestrNumber] [nvarchar](255) NULL,
	[TNANomLoad] [float] NULL,
	[TNAFactLoad] [float]NULL,
	[TNBNomLoad] [float] NULL,
	[TNBFactLoad] [float] NULL,
	[TNCNomLoad] [float] NULL,
	[TNCFactLoad] [float] NULL,
	[TNAULoss] [float] NULL,
	[TNBULoss] [float] NULL,
	[TNCULoss] [float] NULL,
	[TNASerialNumber] [nvarchar](255) NULL,
	[TNBSerialNumber] [nvarchar](255) NULL,
	[TNCSerialNumber] [nvarchar](255) NULL,
	[TNACalibrationDate] [nvarchar](255) NULL,
	[TNBCalibrationDate] [nvarchar](255) NULL,
	[TNCCalibrationDate] [nvarchar](255) NULL,
	[TNAMPI] int NULL,
	[TNBMPI] int NULL,
	[TNCMPI] int NULL,
	[TNPlace] [nvarchar](400) NULL,
	[TI_ID] [int] NULL,
	[TTModel_ID] [int] NULL,
	[TTA_ID] [int] NULL,
	[TTB_ID] [int] NULL,
	[TTC_ID] [int] NULL,
	[TTCoeffHi] [int] NULL,
	[TTCoeffLo] [int] NULL,
	[TNModel_ID] [int] NULL,
	[TNA_ID] [int] NULL,
	[TNB_ID] [int] NULL,
	[TNC_ID] [int] NULL,
	[TNCoeffHi] [int] NULL,
	[TNCoeffLo] [int] NULL,
	[BusSystem_ID] [int] NULL,
	[TNDesignation_ID] [int] NULL,
	[UseBusSystem] [bit] NULL,
	[PS_ID] [int] NULL,
	[BusSystemName] [nvarchar](400) NULL,
	[TNDesignationName] [nvarchar](400) NULL,
	[DistributingArrangementName] [nvarchar](400) NULL,
	[DistributingArrangement_ID] [int] NULL
) 

GO


grant EXECUTE on TYPE::dbo.ImportNSI_InterrogatoryList_TableType to UserDeclarator
go



-- ======================================================================================
-- Автор:
--
--		Карпов
--
-- Дата создания:
--
--		2015
--
-- Описание:
-- ОПРОСНЫЙ ЛИСТ - загрузка данных
--
-- ======================================================================================



create procedure [dbo].[usp2_ImportNSI_InterrogatoryList]
@SourceTable ImportNSI_InterrogatoryList_TableType READONLY, @Section_ID int 

AS
BEGIN
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

set dateformat dmy

declare 
@TempImportTransformators ImportNSI_InterrogatoryList_TableType,
 @InterrogatoryList_ID int , 
 @InterrogatoryList nvarchar(200)
 
select @InterrogatoryList=SectionName  from info_section_List 
where Section_ID like @Section_ID

insert into @TempImportTransformators
select * from @SourceTable




delete from @TempImportTransformators
where MeterSerialNumber is null

--delete from @TempImportTransformators
--where MeterSerialNumber like 'q'


update @TempImportTransformators
set 
TTACalibrationDate=REPLACE (isnull(TTACalibrationDate,''),';',''),
TTBCalibrationDate=REPLACE (isnull(TTBCalibrationDate,''),';',''),
TTCCalibrationDate=REPLACE (isnull(TTCCalibrationDate,''),';',''),
TNACalibrationDate=REPLACE (isnull(TNACalibrationDate,''),';',''),
TNBCalibrationDate=REPLACE (isnull(TNBCalibrationDate,''),';',''),
TNCCalibrationDate=REPLACE (isnull(TNCCalibrationDate,''),';',''),
[MeterCalibrationDate]=REPLACE (isnull([MeterCalibrationDate],''),';','')




update @TempImportTransformators
set 
TNBType= TNAType,
TNCType= TNAType,
TNBClass=TNAClass,
TNCClass=TNAClass,  
TNBReestrNumber=TNAReestrNumber,
TNCReestrNumber=TNAReestrNumber,
TNBNomLoad=TNANomLoad,
TNCNomLoad=TNANomLoad,

TNBFactLoad=TNAFactLoad,
TNCFactLoad=TNAFactLoad,
TNBULoss=TNAULoss,
TNCULoss=TNAULoss,
TNBSerialNumber=TNASerialNumber,
TNCSerialNumber=TNASerialNumber,
TNBCalibrationDate=TNACalibrationDate,
TNCCalibrationDate=TNACalibrationDate,
TNBMPI=TNAMPI,
TNCMPI=TNAMPI
--select * from @TempImportTransformators
where 
isnull(TNAType,'')<>''
and isnull(TNCType,'')=''
and 
(TNAType like '%НТМИ%' or TNAType like '%НАМИ%' )



update 
@TempImportTransformators set MeterSerialNumber= rtrim(ltrim(RTRIM(MeterSerialNumber)))


update 
@TempImportTransformators set TNCoeff= REPLACE(TNCoeff,N':√3','')


update @TempImportTransformators
set 
TTCoeffHi= cast(SUBSTRING(TTCoeff,1,CHARINDEX('/',TTCoeff,1)-1 ) as  float),
TTCoeffLo= cast(SUBSTRING(TTCoeff,CHARINDEX('/',TTCoeff,1)+1, len(TTCoeff) )as  float)
from @TempImportTransformators
where 
TTCoeff is not null
and ISNULL(TTCoeff,'')<>''

update @TempImportTransformators
set 
TNCoeffHi= (SUBSTRING(TNCoeff,1,CHARINDEX('/',TNCoeff,1)-1 ) ),
TNCoeffLo= (SUBSTRING(TNCoeff,CHARINDEX('/',TNCoeff,1)+1, len(TNCoeff) ))
from @TempImportTransformators
where 
TNCoeff is not null
and ISNULL(TNCoeff,'')<>''

 



--ИД ТИ во временную таблицу
update 
@TempImportTransformators
set TI_ID =Info_TI.TI_ID
from 
@TempImportTransformators TempImportTransformators
join Hard_Meters on isnull(Hard_Meters.MeterSerialNumber,'') like isnull(TempImportTransformators.MeterSerialNumber,'-')
join Info_Meters_TO_TI on Info_Meters_TO_TI.METER_ID=Hard_Meters.Meter_ID
join Info_TI on Info_TI.TI_ID=Info_Meters_TO_TI.TI_ID
where 
Info_Meters_TO_TI.StartDateTime<=GETDATE() and isnull(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>GETDATE()


--ПС
update 
@TempImportTransformators 
set PS_Id=Info_TI.PS_ID
from 
@TempImportTransformators TempImportTransformators join Info_TI on Info_TI.TI_ID=TempImportTransformators.TI_ID
where 
TempImportTransformators.TI_ID is not null




----Создаем модели ТТ
--select distinct  
--ShortTTType, ClassName , KTT1,	KTT2
--from TempOEK
--where ShortTTType not like ''


declare @maxID int
select @maxID=max(TTModel_ID)
from Dict_Transformators_TT_Model

select @maxID= ISNULL(@maxID,0)

insert into Dict_Transformators_TT_Model
(
TTModel_ID,
StringName,
CoefIHigh,
CoefILow,
ClassName,
CalibrationInterval,
RegistryNumber,
MinTemperature,
MaxTemperature,
Producer,
TakeOutOfProduction,
Voltage
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TTAType DESC)), res.*,
	0,0,'',0,0
from
( 
	select distinct 
	TTAType=rtrim(TempImportTransformators.TTAType) , 
	cast(TempImportTransformators.TTCoeffHi as int) KTT1,
	cast(TempImportTransformators.TTCoeffLo as int) KTT2,
	TTAClass=isnull(TempImportTransformators.TTAClass,'-'), 
	TTAMPI=isnull(TempImportTransformators.TTAMPI,0), 
	TTAReestrNumber=rtrim(isnull(TempImportTransformators.TTAReestrNumber,''))
	from @TempImportTransformators  TempImportTransformators
	where 
	not exists
	(select 1 from Dict_Transformators_TT_Model 
	where 
	StringName like rtrim(TempImportTransformators.TTAType)
	and CoefIHigh=isnull(TempImportTransformators.TTCoeffHi ,1)
	and CoefILow=isnull(TempImportTransformators.TTCoeffLo ,1)
	and ClassName like rtrim(isnull(TempImportTransformators.TTAClass,'-'))
	and isnull(CalibrationInterval,0)= isnull(TempImportTransformators.TTAMPI,0)
	and RegistryNumber= rtrim(isnull(TempImportTransformators.TTAReestrNumber,''))
	)
	and TTAType is not null
	and TTAType not like ''
	and TempImportTransformators.TI_ID is not null
)
as res



update @TempImportTransformators
set
TTModel_ID=Dict_Transformators_TT_Model.TTModel_ID
from 
@TempImportTransformators  TempImportTransformators join Dict_Transformators_TT_Model
on 
--Dict_Transformators_TT_Model.StringName like TempImportTransformators.TTAType 
--	and Dict_Transformators_TT_Model.CoefIHigh=TempImportTransformators.TTCoeffHi 
--	and Dict_Transformators_TT_Model.CoefILow=TempImportTransformators.TTCoeffLo
--	and Dict_Transformators_TT_Model.ClassName like TempImportTransformators.TTAClass
StringName like rtrim(TempImportTransformators.TTAType)
	and CoefIHigh=isnull(TempImportTransformators.TTCoeffHi ,1)
	and CoefILow=isnull(TempImportTransformators.TTCoeffLo ,1)
	and ClassName like rtrim(isnull(TempImportTransformators.TTAClass,'-'))
	and isnull(CalibrationInterval,0)= isnull(TempImportTransformators.TTAMPI,0)
	and RegistryNumber= rtrim(isnull(TempImportTransformators.TTAReestrNumber,''))
	

--по расположению проверяем
--теперь создаем трансофрматоры
--A
select @maxID=max(TT_ID)
from Hard_Transformators_TT

select @maxID= ISNULL(@maxID,0)

insert into Hard_Transformators_TT
(
TT_ID ,
TTModel_ID,
ModelName,
SerialNumber,
InstallationPlace,
CalibrationDate,
FactLoad,
NomLoad,
ProductionDate,
CalibrationActNumber,
CalibrationResult
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TTASerialNumber DESC)),
 res.*,
null, null, null
from
( 
	select distinct 
	TTModel_ID  ,
	TTAType, 
	TTASerialNumber, TTPlace, 
	cast (replace(TTACalibrationDate,';','') as datetime) TTACalibrationDate
	, cast (TTAFactload as float) TTAFactload
	, cast (TTANomLoad as float) TTANomLoad
	
	from @TempImportTransformators TempImportTransformators
	where 
	TTModel_ID is not null and ISNULL(TTASerialNumber,'')<>''
	and TI_ID is not null
)
as res
where not exists 
(select 1 from Hard_Transformators_TT 
where SerialNumber like res.TTASerialNumber and TTModel_ID = res.TTModel_ID and InstallationPlace like res.TTPlace
)


update @TempImportTransformators
set
TTA_ID=Hard_Transformators_TT.TT_ID
from 
@TempImportTransformators TempImportTransformators join Hard_Transformators_TT
on 	
	isnull(TempImportTransformators.TTModel_ID,0)=Hard_Transformators_TT.TTModel_ID
	and ISNULL(TempImportTransformators.TTASerialNumber,'-')=Hard_Transformators_TT.SerialNumber
	and Hard_Transformators_TT.InstallationPlace like TempImportTransformators.TTPlace
where 
	Hard_Transformators_TT.TTModel_ID is not null and ISNULL(TempImportTransformators.TTASerialNumber,'')<>''

 

---------------------
--B
select @maxID=max(TT_ID)
from Hard_Transformators_TT

select @maxID= ISNULL(@maxID,0)

insert into Hard_Transformators_TT
(
TT_ID ,
TTModel_ID,
ModelName,
SerialNumber,
InstallationPlace,
CalibrationDate,
FactLoad,
NomLoad,
ProductionDate,
CalibrationActNumber,
CalibrationResult
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TTBSerialNumber DESC)),
 res.*,
null, null, null
from
( 
	select distinct 
	TTModel_ID  ,
	TTBType, 
	TTBSerialNumber, TTPlace, 
	cast (replace(TTBCalibrationDate,';','') as datetime) TTBCalibrationDate
	, CAST (TTBFactload as float) TTBFactload
	, CAST (TTBNomLoad as float) TTBNomLoad
	
	from @TempImportTransformators TempImportTransformators
	where 
	TTModel_ID is not null and ISNULL(TTBSerialNumber,'')<>''
	and TI_ID is not null
)
as res
where not exists (select 1 from Hard_Transformators_TT where SerialNumber like res.TTBSerialNumber and TTModel_ID = res.TTModel_ID and InstallationPlace like res.TTPlace)


update @TempImportTransformators
set
TTB_ID=Hard_Transformators_TT.TT_ID
from 
@TempImportTransformators TempImportTransformators join Hard_Transformators_TT
on 	
	isnull(TempImportTransformators.TTModel_ID,0)=Hard_Transformators_TT.TTModel_ID
	and ISNULL(TempImportTransformators.TTBSerialNumber,'-')=Hard_Transformators_TT.SerialNumber
	and Hard_Transformators_TT.InstallationPlace like TempImportTransformators.TTPlace
where 
	Hard_Transformators_TT.TTModel_ID is not null and ISNULL(TempImportTransformators.TTBSerialNumber,'')<>''




---------------------
--C
select @maxID=max(TT_ID)
from Hard_Transformators_TT

select @maxID= ISNULL(@maxID,0)

insert into Hard_Transformators_TT
(
TT_ID ,
TTModel_ID,
ModelName,
SerialNumber,
InstallationPlace,
CalibrationDate,
FactLoad,
NomLoad,
ProductionDate,
CalibrationActNumber,
CalibrationResult
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TTCSerialNumber DESC)),
 res.*,
null, null, null
from
( 
	select distinct 
	TTModel_ID  ,
	TTCType, 
	TTCSerialNumber, TTPlace, 
	cast (replace(TTCCalibrationDate,';','') as datetime) TTCCalibrationDate
	, CAST (TTCFactload as float) TTCFactload
	, CAST (TTCNomLoad as float) TTCNomLoad
	
	from @TempImportTransformators TempImportTransformators
	where 
	TTModel_ID is not null and ISNULL(TTCSerialNumber,'')<>''
	and TI_ID is not null
)
as res
where not exists (select 1 from Hard_Transformators_TT where SerialNumber like res.TTCSerialNumber and TTModel_ID = res.TTModel_ID and InstallationPlace like res.TTPlace)


update @TempImportTransformators
set
TTC_ID=Hard_Transformators_TT.TT_ID
from 
@TempImportTransformators TempImportTransformators join Hard_Transformators_TT
on 	
	isnull(TempImportTransformators.TTModel_ID,0)=Hard_Transformators_TT.TTModel_ID
	and ISNULL(TempImportTransformators.TTCSerialNumber,'-')=Hard_Transformators_TT.SerialNumber
	and Hard_Transformators_TT.InstallationPlace like TempImportTransformators.TTPlace
where 
	Hard_Transformators_TT.TTModel_ID is not null and ISNULL(TempImportTransformators.TTCSerialNumber,'')<>''





--==ТРАНСФОРАТОРЫ НАПРЯЖЕНИЯ======================



 
 
select @maxID=max(TNModel_ID)
from Dict_Transformators_TN_Model

select @maxID= ISNULL(@maxID,0)

insert into Dict_Transformators_TN_Model
(
TNModel_ID,
IsThreePhase,
StringName,
CoefUHigh,
CoefULow,
ClassName,
CalibrationInterval,
RegistryNumber,
MinTemperature,
MaxTemperature,
Producer,
TakeOutOfProduction

)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TNAType DESC)), res.*,
	0,0,'',0
from
( 
	select distinct 
	IsThreePhase=case when ISNULL(TempImportTransformators.TNASerialNumber,'1')=ISNULL(TempImportTransformators.TNCSerialNumber,'2') then 1 else 0 end,
	TNAType =rtrim(isnull(TempImportTransformators.TNAType ,'')), 
	cast(TempImportTransformators.TNCoeffHi as int) KTN1,
	cast(TempImportTransformators.TNCoeffLo as int) KTN2,
	TNAClass=isnull(TempImportTransformators.TNAClass,'-'), 
	TNAMPI=isnull(TempImportTransformators.TNAMPI,0), 
	TNAReestrNumber=isnull(TempImportTransformators.TNAReestrNumber,'')
	from @TempImportTransformators  TempImportTransformators
	where 
	not exists
	(select 1 from Dict_Transformators_TN_Model 
	where 
	StringName like rtrim(isnull(TempImportTransformators.TNAType ,''))
	and CoefUHigh=TempImportTransformators.TNCoeffHi 
	and CoefULow=TempImportTransformators.TNCoeffLo 
	and ClassName like isnull(TempImportTransformators.TNAClass,'-')
	and CalibrationInterval= isnull(TempImportTransformators.TNAMPI,0)
	and RegistryNumber= isnull(TempImportTransformators.TNAReestrNumber,'')
	)
	and TNAType not like ''
	and TempImportTransformators.TI_ID is not null
	and TempImportTransformators.TNCoeffHi is not null	
)
as res



update @TempImportTransformators
set
TNModel_ID=Dict_Transformators_TN_Model.TNModel_ID
from 
@TempImportTransformators TempImportTransformators join Dict_Transformators_TN_Model
on 
	--Dict_Transformators_TN_Model.StringName like TempImportTransformators.TNAType 
	--and Dict_Transformators_TN_Model.CoefUHigh=TempImportTransformators.TNCoeffHi 
	--and Dict_Transformators_TN_Model.CoefULow=TempImportTransformators.TNCoeffLo
	--and Dict_Transformators_TN_Model.ClassName like TempImportTransformators.TNAClass
StringName like rtrim(isnull(TempImportTransformators.TNAType ,''))
	and CoefUHigh=TempImportTransformators.TNCoeffHi 
	and CoefULow=TempImportTransformators.TNCoeffLo 
	and ClassName like isnull(TempImportTransformators.TNAClass,'-')
	and CalibrationInterval= isnull(TempImportTransformators.TNAMPI,0)
	and RegistryNumber= isnull(TempImportTransformators.TNAReestrNumber,'')


--по расположению проверяем
--теперь создаем трансофрматоры
--TNA
select @maxID=max(TN_ID)
from Hard_Transformators_TN

select @maxID= ISNULL(@maxID,0)

insert into Hard_Transformators_TN
(
TN_ID ,
TNModel_ID,
ModelName,
SerialNumber,
InstallationPlace,
CalibrationDate,
FactLoad,
NomLoad,
VoltageLossPhaseA,
ProductionDate,
CalibrationActNumber,
CalibrationResult
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TNASerialNumber DESC)),
 res.*,
null, null, null
from
( 
	select distinct 
	TNModel_ID  ,
	TNAType, 
	TNASerialNumber, TNPlace, 
	cast (replace(TNACalibrationDate,';','') as datetime) TNACalibrationDate
	, CAST (TNAFactload as float) TNAFactload
	, CAST (TNANomLoad as float) TNANomLoad
	,TNAULoss
	from @TempImportTransformators TempImportTransformators
	where 
	TNModel_ID is not null and ISNULL(TNASerialNumber,'')<>''
	and TI_ID is not null
)
as res
where not exists (select 1 from Hard_Transformators_TN where SerialNumber like res.TNASerialNumber and TNModel_ID = res.TNModel_ID and InstallationPlace like res.TNPlace)


update @TempImportTransformators
set
TNA_ID=Hard_Transformators_TN.TN_ID
from 
@TempImportTransformators TempImportTransformators join Hard_Transformators_TN
on 	
	isnull(TempImportTransformators.TNModel_ID,0)=Hard_Transformators_TN.TNModel_ID
	and ISNULL(TempImportTransformators.TNASerialNumber,'-')=Hard_Transformators_TN.SerialNumber
	and Hard_Transformators_TN.InstallationPlace like TempImportTransformators.TNPlace
where 
	Hard_Transformators_TN.TNModel_ID is not null and ISNULL(TempImportTransformators.TNASerialNumber,'')<>''


 

--TNB
select @maxID=max(TN_ID)
from Hard_Transformators_TN

select @maxID= ISNULL(@maxID,0)

insert into Hard_Transformators_TN
(
TN_ID ,
TNModel_ID,
ModelName,
SerialNumber,
InstallationPlace,
CalibrationDate,
FactLoad,
NomLoad,
VoltageLossPhaseB,
ProductionDate,
CalibrationActNumber,
CalibrationResult
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TNBSerialNumber DESC)),
 res.*,
null, null, null
from
( 
	select distinct 
	TNModel_ID  ,
	TNBType, 
	TNBSerialNumber, TNPlace, 
	cast (replace(TNBCalibrationDate,';','') as datetime) TNBCalibrationDate
	, CAST (TNBFactload as float) TNBFactload
	, CAST (TNBNomLoad as float) TNBNomLoad
	,TNBULoss
	from @TempImportTransformators TempImportTransformators
	where 
	TNModel_ID is not null and ISNULL(TNBSerialNumber,'')<>''
	and TI_ID is not null
)
as res
where not exists (select 1 from Hard_Transformators_TN where SerialNumber like res.TNBSerialNumber and TNModel_ID = res.TNModel_ID and InstallationPlace like res.TNPlace)


update @TempImportTransformators
set
TNB_ID=Hard_Transformators_TN.TN_ID
from 
@TempImportTransformators TempImportTransformators join Hard_Transformators_TN
on 	
	isnull(TempImportTransformators.TNModel_ID,0)=Hard_Transformators_TN.TNModel_ID
	and ISNULL(TempImportTransformators.TNBSerialNumber,'-')=Hard_Transformators_TN.SerialNumber
	and Hard_Transformators_TN.InstallationPlace like TempImportTransformators.TNPlace
where 
	Hard_Transformators_TN.TNModel_ID is not null and ISNULL(TempImportTransformators.TNBSerialNumber,'')<>''



--TNC
select @maxID=max(TN_ID)
from Hard_Transformators_TN

select @maxID= ISNULL(@maxID,0)

insert into Hard_Transformators_TN
(
TN_ID ,
TNModel_ID,
ModelName,
SerialNumber,
InstallationPlace,
CalibrationDate,
FactLoad,
NomLoad,
VoltageLossPhaseC,
ProductionDate,
CalibrationActNumber,
CalibrationResult
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY TNCSerialNumber DESC)),
 res.*,
null, null, null
from
( 
	select distinct 
	TNModel_ID  ,
	TNCType, 
	TNCSerialNumber, TNPlace, 
	cast (replace(TNCCalibrationDate,';','') as datetime) TNCCalibrationDate
	, CAST (TNCFactload as float) TNCFactload
	, CAST (TNCNomLoad as float) TNCNomLoad
	, TNCULoss
	from @TempImportTransformators TempImportTransformators
	where 
	TNModel_ID is not null and ISNULL(TNCSerialNumber,'')<>''
	and TI_ID is not null
)
as res
where not exists (select 1 from Hard_Transformators_TN where SerialNumber like res.TNCSerialNumber and TNModel_ID = res.TNModel_ID and InstallationPlace like res.TNPlace)


update @TempImportTransformators
set
TNC_ID=Hard_Transformators_TN.TN_ID
from 
@TempImportTransformators TempImportTransformators join Hard_Transformators_TN
on 	
	isnull(TempImportTransformators.TNModel_ID,0)=Hard_Transformators_TN.TNModel_ID
	and ISNULL(TempImportTransformators.TNCSerialNumber,'-')=Hard_Transformators_TN.SerialNumber
	and Hard_Transformators_TN.InstallationPlace like TempImportTransformators.TNPlace
where 
	Hard_Transformators_TN.TNModel_ID is not null and ISNULL(TempImportTransformators.TNCSerialNumber,'')<>''





update 
@TempImportTransformators
set 
TNB_ID=TNA_ID,
TNC_ID=TNA_ID
where
TNAType is not null
and rtrim(isnull(TNAType,''))<>''
and isnull(TNASerialNumber,'2')=isnull(TNCSerialNumber,'1')








--системы шин UseBusSystem
declare @tempbus table (TNPlace nvarchar(400)  , Voltage float , PS_ID int,  TNCount int)

insert into 
@tempbus(TNPlace,Voltage,PS_ID, TNCount)
select Distinct 
TNPlace, TNCoeffHi/1000.0 , PS_ID, count (isnull(TNASerialNumber,'='))
from @TempImportTransformators
where 
TNA_ID is not null
group by TNPlace, TNCoeffHi, PS_ID
having count (isnull(TNASerialNumber,'='))>1


update @TempImportTransformators
set UseBusSystem=1
where 
TNA_ID is not null

update @TempImportTransformators
set UseBusSystem=0
where 
TNA_ID is not null
and TNAType like '%KOTEF%'

/*
--!!!!!!!!!!!!!
--не используем так как подстанции не в Dict_PS и получается что один и тот же ТН на разных Dict_PS
--Лучше под определенные типы ТН сделать UseBusSystem=0

update @TempImportTransformators
set UseBusSystem=1, BusSystemName=temp.TNPlace
from @TempImportTransformators  TempImportTransformators, @tempbus temp
where 
isnull(temp.PS_ID,0)=isnull(TempImportTransformators.PS_ID,0)
and isnull(TempImportTransformators.TNCoeffHi,0)/1000.0=Voltage
and isnull(TempImportTransformators.TNPlace,'')=isnull(temp.TNPlace,'')

 */



--Шинные ТН
update @TempImportTransformators 
set TNPlace='н/д'
where 
TNCoeff is not null
and ISNULL(TNPlace,'') like ''





--1) ОРУ
select @maxID=max(DistributingArrangement_ID)
from Dict_DistributingArrangement
select @maxID= ISNULL(@maxID,0)

insert into Dict_DistributingArrangement
(
DistributingArrangement_ID, PS_ID, StringName, DistributingArrangementType, Voltage
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY StringName DESC)),
 res.*
from
( 
	select distinct 
	PS_ID, 
	StringName='Распред. устройство '+convert(varchar, cast ((TempImportTransformators.TNCoeffHi/1000.0) as float))+' кВ', 
	DistributingArrangementType=0, 
	Voltage=TNCoeffHi/1000.0
	from @TempImportTransformators TempImportTransformators
	where 
	TNCoeffHi is not null
	and UseBusSystem=1
	and not exists (select 1 from Dict_DistributingArrangement
	 where PS_ID=TempImportTransformators.PS_ID 
	 and StringName='Распред. устройство '+convert(varchar, cast ((TempImportTransformators.TNCoeffHi/1000.0) as float))+' кВ' 
	 and Voltage=TempImportTransformators.TNCoeffHi/1000.0
	)
	)
as res



update @TempImportTransformators
set
DistributingArrangement_ID=Dict_DistributingArrangement.DistributingArrangement_ID,
DistributingArrangementName=Dict_DistributingArrangement.StringName
from 
@TempImportTransformators  TempImportTransformators
, Dict_DistributingArrangement 
where 
Dict_DistributingArrangement.PS_ID=TempImportTransformators.PS_ID 
and StringName='Распред. устройство '+convert(varchar, cast ((TNCoeffHi/1000.0) as float))+' кВ' 
and Voltage=TNCoeffHi/1000.0
and TempImportTransformators.PS_ID is not null
and isnull(TempImportTransformators.UseBusSystem,0)=1
and TempImportTransformators.TNCoeffHi is not null





--2) СШ
select @maxID=max(BusSystem_ID)
from Dict_BusSystem
select @maxID= ISNULL(@maxID,0)

insert into Dict_BusSystem
(
BusSystem_ID, DistributingArrangement_ID, StringName, Voltage
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY StringName DESC)),
 res.*
from
( 
	select distinct 
	DistributingArrangement_ID, 
	StringName=TNPlace, 
	Voltage=TNCoeffHi/1000.0
	from @TempImportTransformators TempImportTransformators
	where 
	DistributingArrangement_ID is not null	
	and not exists (select 1 from Dict_BusSystem
	 where DistributingArrangement_ID=TempImportTransformators.DistributingArrangement_ID 
	 and StringName=TNPlace 
	 and Voltage=TNCoeffHi/1000.0
	)
	)
as res


update @TempImportTransformators
set
BusSystem_ID=Dict_BusSystem.BusSystem_ID,
BusSystemName=Dict_BusSystem.StringName
from 
@TempImportTransformators  TempImportTransformators
, Dict_BusSystem 
where 
Dict_BusSystem.DistributingArrangement_ID=TempImportTransformators.DistributingArrangement_ID 
and StringName=TNPlace 
and Voltage=TNCoeffHi/1000.0	 
and TempImportTransformators.DistributingArrangement_ID is not null	
and isnull(TempImportTransformators.UseBusSystem,0)=1
and TempImportTransformators.TNCoeffHi is not null



--3) обозначение ТН (столько сколько разных сочетаний фаз)



select @maxID=max(TNDesignation_ID)
from Dict_TNDesignation
select @maxID= ISNULL(@maxID,0)

insert into Dict_TNDesignation
(
TNDesignation_ID, StringName,BusSystem_ID, Status, Description, TNA_ID, TNB_ID , TNC_ID
)

select 
@maxID+(ROW_NUMBER() OVER(ORDER BY res1.StringName DESC)),
 res1.StringName, res1.BusSystem_ID, 0, '', res1.TNA_ID, res1.TNB_ID, res1.TNC_ID
from
( 
	select distinct
	StringName='ТН '+convert(varchar,(ROW_NUMBER() OVER(partition BY BusSystem_ID order by TNA_ID DESC))),
	 res.*
	from
	(	
		select distinct 
		BusSystem_ID, 
		TNA_ID, TNB_ID , TNC_ID,		
		Voltage= cast(TNCoeffHi/1000.0 as float)
		from @TempImportTransformators TempImportTransformators
		where 
		DistributingArrangement_ID is not null	
		and not exists 
			(select 1 from Dict_TNDesignation 
			where BusSystem_ID=TempImportTransformators.BusSystem_ID 
			and isnull(TNA_ID,0)=isnull(TempImportTransformators.TNA_ID,0) 
			
			)
	)
	as res
	
) as res1
	

update @TempImportTransformators
set
TNDesignation_ID=Dict_TNDesignation.TNDesignation_ID,
TNDesignationName=Dict_TNDesignation.StringName
from 
@TempImportTransformators  TempImportTransformators
, Dict_TNDesignation 
where 
TempImportTransformators.BusSystem_ID is not null
and TempImportTransformators.TNA_ID is not null
and isnull(TempImportTransformators.UseBusSystem,0)=1
and TempImportTransformators.TNCoeffHi is not null
and Dict_TNDesignation.BusSystem_ID=TempImportTransformators.BusSystem_ID 
and isnull(Dict_TNDesignation.TNA_ID,0)=isnull(TempImportTransformators.tna_ID,0)
and isnull(Dict_TNDesignation.TNB_ID,0)=isnull(TempImportTransformators.tnb_ID,0)
and isnull(Dict_TNDesignation.TNC_ID,0)=isnull(TempImportTransformators.tnc_ID,0)


 


--удаляем
delete from Info_Transformators 
where TI_ID in (select isnull(TI_ID,0) from @TempImportTransformators where TI_ID is not null)

 
 
	
--добавляем
insert into Info_Transformators 
 (
 TI_ID,
StartDateTime,
FinishDateTime,
COEFU,
CoefUHigh,
CoefULow,
COEFI,
CoefIHigh,
CoefILow,
SerialTransIPhaseA,
SerialTransIPhaseB,
SerialTransIPhaseC,
TransIType,
TransIClass,
SerialTransUPhaseA,
SerialTransUPhaseB,
SerialTransUPhaseC,
TransUType,
TransUClass,
BusSystem_ID,
TNDesignation_ID,
TNA_ID,
TNB_ID,
TNC_ID,
TTA_ID,
TTB_ID,
TTC_ID,
CUS_ID, UseBusSystem
)
select TI_ID, 
'01-01-2010', '01-01-2100',

isnull( TempImportTransformators.TNCoeffHi,1)*1.0/isnull( TempImportTransformators.TNCoeffLo,1),
isnull( TempImportTransformators.TNCoeffHi,1),
isnull( TempImportTransformators.TNCoeffLo,1),
isnull( TempImportTransformators.TTCoeffHi,1)*1.0/isnull( TempImportTransformators.TTCoeffLo,1),
isnull( TempImportTransformators.TTCoeffHi,1),
isnull( TempImportTransformators.TTCoeffLo,1),
 TempImportTransformators.TTASerialNumber,
 TempImportTransformators.TTBSerialNumber,
 TempImportTransformators.TTCSerialNumber,
 TempImportTransformators.TTAType,
 TempImportTransformators.TTAClass,
 TempImportTransformators.TNASerialNumber,
 TempImportTransformators.TNBSerialNumber,
 TempImportTransformators.TNCSerialNumber,
 TempImportTransformators.TNAType,
 TempImportTransformators.TNAClass,
 case when ISNULL( TempImportTransformators.UseBusSystem,0)=1 then TempImportTransformators.BusSystem_ID else null end,
 case when ISNULL( TempImportTransformators.UseBusSystem,0)=1 then TempImportTransformators.TNDesignation_ID else null end,
 case when ISNULL( TempImportTransformators.UseBusSystem,0)=1 then null else TempImportTransformators.TNA_ID end,
 case when ISNULL( TempImportTransformators.UseBusSystem,0)=1 then null else TempImportTransformators.TNB_ID end,
 case when ISNULL( TempImportTransformators.UseBusSystem,0)=1 then null else TempImportTransformators.TNC_ID end,
 TempImportTransformators.TTA_ID,
 TempImportTransformators.TTB_ID,
 TempImportTransformators.TTC_ID,
0,
ISNULL( TempImportTransformators.UseBusSystem,0)
from  @TempImportTransformators TempImportTransformators
where TI_ID is not null
--and isnull( TempImportTransformators.TNAType,'')<>''
and not exists (select TI_ID from Info_Transformators where TI_ID= TempImportTransformators.ti_ID)



 


--Удаляем ТТ и ТН без связей
delete from Hard_Transformators_TT 
where 
TT_ID not in (select TTA_ID from Info_Transformators where TTA_ID is not null)
and TT_ID not in (select TTB_ID from Info_Transformators where TTB_ID is not null)
and TT_ID not in (select TTC_ID from Info_Transformators where TTC_ID is not null)

delete from Hard_Transformators_TN 
where 
TN_ID not in (select TNA_ID from Info_Transformators where TNA_ID is not null)
and TN_ID not in (select TNB_ID from Info_Transformators where TNB_ID is not null)
and TN_ID not in (select TNC_ID from Info_Transformators where TNC_ID is not null)
and TN_ID not in (select TNA_ID from Dict_TNDesignation where TNA_ID is not null)
and TN_ID not in (select TNB_ID from Dict_TNDesignation where TNB_ID is not null)
and TN_ID not in (select TNC_ID from Dict_TNDesignation where TNC_ID is not null)


delete from Dict_TNDesignation where TNDesignation_ID not in (select TNDesignation_ID from Info_Transformators where TNDesignation_ID is not null)



--обновляем даннные измерений в трансфораторах
--Для ТН считаем что в опросном листе указана полная факт нагрузка
update Hard_Transformators_TN
set 
VoltageLossPhaseA=TNAULoss, 
NomLoad=TempImportTransformators.TNANomLoad,
FactLoad=TempImportTransformators.TNAFactLoad, 
FactLoadPhaseA=case when Dict_Transformators_TN_Model.IsThreePhase=1 then  FactLoadPhaseA else TNAFactLoad end, 
CalibrationDate= cast (replace(TNACalibrationDate,';','') as datetime)
from 
Hard_Transformators_TN join @TempImportTransformators TempImportTransformators on Hard_Transformators_TN.TN_ID=TempImportTransformators.TNA_ID
join Dict_Transformators_TN_Model on Dict_Transformators_TN_Model.TNModel_ID=Hard_Transformators_TN.TNModel_ID
where 
TempImportTransformators.TNA_ID is not null


update Hard_Transformators_TN
set 
VoltageLossPhaseB=TNBULoss,
NomLoad=TempImportTransformators.TNBNomLoad,
FactLoad=TempImportTransformators.TNBFactLoad, 
FactLoadPhaseB=case when Dict_Transformators_TN_Model.IsThreePhase=1 then  FactLoadPhaseB else TNBFactLoad end,
CalibrationDate= cast (replace(TNBCalibrationDate,';','') as datetime)
from 
Hard_Transformators_TN  join @TempImportTransformators TempImportTransformators on Hard_Transformators_TN.TN_ID=TempImportTransformators.TNB_ID
join Dict_Transformators_TN_Model on Dict_Transformators_TN_Model.TNModel_ID=Hard_Transformators_TN.TNModel_ID
where 
TempImportTransformators.TNB_ID is not null


update Hard_Transformators_TN
set 
VoltageLossPhaseC=TNCULoss,
NomLoad=TempImportTransformators.TNCNomLoad,
FactLoad=TempImportTransformators.TNCFactLoad, 
FactLoadPhaseC=case when Dict_Transformators_TN_Model.IsThreePhase=1 then  FactLoadPhaseC else TNCFactLoad end, 
CalibrationDate= cast (replace(TNBCalibrationDate,';','') as datetime)
from 
Hard_Transformators_TN join @TempImportTransformators TempImportTransformators on Hard_Transformators_TN.TN_ID=TempImportTransformators.TNC_ID
join Dict_Transformators_TN_Model on Dict_Transformators_TN_Model.TNModel_ID=Hard_Transformators_TN.TNModel_ID
where 
TempImportTransformators.TNC_ID is not null


update Hard_Transformators_TT
set 
NomLoad=TempImportTransformators.TTANomLoad,
FactLoad=TempImportTransformators.TTAFactLoad, 
CalibrationDate= cast (replace(TTACalibrationDate,';','') as datetime)
from 
Hard_Transformators_TT join @TempImportTransformators TempImportTransformators on Hard_Transformators_TT.TT_ID=TempImportTransformators.TTA_ID
where 
TempImportTransformators.TTA_ID is not null

update Hard_Transformators_TT
set 
NomLoad=TempImportTransformators.TTBNomLoad,
FactLoad=TempImportTransformators.TTBFactLoad, 
CalibrationDate= cast (replace(TTBCalibrationDate,';','') as datetime)
from 
Hard_Transformators_TT join @TempImportTransformators TempImportTransformators on Hard_Transformators_TT.TT_ID=TempImportTransformators.TTB_ID
where 
TempImportTransformators.TTB_ID is not null

update Hard_Transformators_TT
set 
NomLoad=TempImportTransformators.TTCNomLoad,
FactLoad=TempImportTransformators.TTCFactLoad, 
CalibrationDate= cast (replace(TTCCalibrationDate,';','') as datetime)
from 
Hard_Transformators_TT join @TempImportTransformators TempImportTransformators on Hard_Transformators_TT.TT_ID=TempImportTransformators.TTC_ID
where 
TempImportTransformators.TTC_ID is not null


if (@Section_ID is not null)
begin
	select @InterrogatoryList_ID=InterrogatoryList_ID from Expl_InterrogatoryList 
	where  Section_ID=@Section_ID
	and StartDateTime in (select MAX(StartDateTime) from Expl_InterrogatoryList where  Section_ID=@Section_ID)
	
	if ISNULL(@InterrogatoryList_ID,0)=0
	begin
		select @InterrogatoryList_ID= MAX(InterrogatoryList_ID) from Expl_InterrogatoryList
		set @InterrogatoryList_ID= ISNULL(@InterrogatoryList_ID,0)+1

		insert into Expl_InterrogatoryList
		(StringName, Section_ID, StartDateTime, InterrogatoryList_ID)
		values (@InterrogatoryList,@Section_ID, '01-01-2010',@InterrogatoryList_ID)
	end
	
	
	delete from Expl_InterrogatoryList_Content
	where InterrogatoryList_ID=@InterrogatoryList_ID	
	
	insert into Expl_InterrogatoryList_Content (InterrogatoryList_ID, Number, NumberOnScheme, TI_ID)
	select distinct
	@InterrogatoryList_ID, [TINumber],	[TIShemNumber], TI_ID
	from @TempImportTransformators
	where TI_ID is not null
	--and TI_ID in 
	--(
	--	select distinct Info_TP2_OurSide_Formula_Description.TI_ID 
	--	from
	--	Info_Section_List
	--		join Info_Section_Description2 on Info_Section_Description2.Section_ID= Info_Section_List.Section_ID
	--		join Info_TP2 on Info_TP2.TP_ID = Info_Section_Description2.TP_ID
	--		join Info_TP2_OurSide_Formula_List on Info_TP2.TP_ID= Info_TP2_OurSide_Formula_List.TP_ID
	--		join Info_TP2_OurSide_Formula_Description 
	--				on Info_TP2_OurSide_Formula_Description.Formula_UN=Info_TP2_OurSide_Formula_List.Formula_UN
	--				and Info_TP2_OurSide_Formula_Description.TI_ID is not null
	--				where Info_Section_List.Section_ID=@Section_ID
 --    )
 
 
 
 
 
 --обновляем номер на схеме 
update 
Expl_InterrogatoryList_Content
set NumberOnScheme=lastNumberOnScheme
from Expl_InterrogatoryList_Content
	outer apply
			(	
			  --берем ближайший опросный лист где этот номер заполнен
			  select top 1
			  lastNumberOnScheme=	oldContent1.NumberOnScheme		
			  from 
			  Expl_InterrogatoryList_Content oldContent1
			  join Expl_InterrogatoryList oldList1 on oldContent1.InterrogatoryList_ID=oldList1.InterrogatoryList_ID
			  where 
			  oldContent1.TI_ID=Expl_InterrogatoryList_Content.TI_ID 
			  and oldContent1.NumberOnScheme is not null	
			  and oldList1.StartDateTime in (					 
							  select 
							  max(oldList.StartDateTime )
							  from 
							  Expl_InterrogatoryList_Content oldContent
							  join Expl_InterrogatoryList oldList on oldContent.InterrogatoryList_ID=oldList.InterrogatoryList_ID
							  where 
							  oldContent.TI_ID=Expl_InterrogatoryList_Content.TI_ID 
							  and oldContent.NumberOnScheme is not null		
							  )	  
			  
			) 
			Expl_InterrogatoryList_Content_old

where 
InterrogatoryList_ID=@InterrogatoryList_ID
and NumberOnScheme is null 


--обновляем номер в АИС если они пустые
update 
Expl_InterrogatoryList_Content
set NumberAIS=lastNumberAIS
from Expl_InterrogatoryList_Content
	outer apply
			(	
			  --берем ближайший опросный лист где этот номер заполнен
			  select  top 1
			  lastNumberAIS=	oldContent1.NumberAIS		
			  from 
			  Expl_InterrogatoryList_Content oldContent1
			  join Expl_InterrogatoryList oldList1 on oldContent1.InterrogatoryList_ID=oldList1.InterrogatoryList_ID
			  where 
			  oldContent1.TI_ID=Expl_InterrogatoryList_Content.TI_ID 
			  and oldContent1.NumberAIS is not null	
			  and oldList1.StartDateTime in (					 
							  select 
							  max(oldList.StartDateTime )
							  from 
							  Expl_InterrogatoryList_Content oldContent
							  join Expl_InterrogatoryList oldList on oldContent.InterrogatoryList_ID=oldList.InterrogatoryList_ID
							  where 
							  oldContent.TI_ID=Expl_InterrogatoryList_Content.TI_ID 
							  and oldContent.NumberAIS is not null		
							  )	  
			  
			) 
			Expl_InterrogatoryList_Content_old

where 
InterrogatoryList_ID=@InterrogatoryList_ID
and NumberAIS is null 

 


end



--либо напряжение импортировать тоже из ОЛ
update Info_TI
set
Voltage=isnull(TempImportTransformators.TIVoltage,isnull(TNCoeffHi,0)/1000.0)
from
Info_TI
join @TempImportTransformators  TempImportTransformators on TempImportTransformators.TI_ID=Info_TI.TI_ID
where 
TempImportTransformators.TI_ID is not null
and isnull(TempImportTransformators.TI_ID,0)<>0
--and isnull(Info_TI.Voltage,0)=0




update @TempImportTransformators 
set [MeterManufacturedDate]= 1900
where isnull([MeterManufacturedDate],1900)<=1900

 
update Hard_Meters
set
CalibrationDate= [MeterCalibrationDate],
ManufacturedDate=Cast ('01-01-'+convert(varchar,isnull([MeterManufacturedDate],1900)) as datetime)
from
Hard_Meters
join Info_meters_to_TI on Hard_Meters.Meter_ID=Info_meters_to_TI.Meter_ID
join @TempImportTransformators  TempImportTransformators on TempImportTransformators.TI_ID=Info_meters_to_TI.TI_ID
where 
TempImportTransformators.TI_ID is not null




declare @TICount int
select @TICount = COUNT(*) from @TempImportTransformators 

declare @TICountSuccess int
select @TICountSuccess = COUNT(*) from Expl_InterrogatoryList_Content
where 
TI_ID is not null
and InterrogatoryList_ID=@InterrogatoryList_ID


select result='ГОТОВО. Импортировано ТИ '+convert(varchar,(@TICountSuccess))+' из '+convert(varchar,(@TICount))


END
GO
 
 
grant EXECUTE on dbo.[usp2_ImportNSI_InterrogatoryList] to UserCalcService
go

grant EXECUTE on dbo.[usp2_ImportNSI_InterrogatoryList] to UserDeclarator
go
