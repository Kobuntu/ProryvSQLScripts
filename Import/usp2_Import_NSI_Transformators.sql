--трансформаторы импортируем, но если есть пересечений и коэфф не совпадает, то выдаем ошибку
--пломбы наверное импортируем на ТИ, только в примечаниях пишем место установки, чтобы можно было использовать в отчетах..
--как быть с пломбаим на ТН - будет дублирование? (да..)
--пломбы заменяем

--в ТН текстом пишут подлецы 10000/√3/100/√3

--надо как то обойти ситуацию, когда два одинаковых номера из за имопрта 18ЮЛ - умноже сделать.. на кол-во ТИ с таким номером 

--модели парсим на клиенте? например ТШП-0,66 1500/5 Кл.т. 0,5S  сначала по Кл.т. потом по первому пробелу...?
--так как не системы, то пока не парсим (моель все равно уникальна для название+класс+ коэфф)
 



if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp2_Import_NSI_Transformators')
          and type in ('P','PC'))
   drop procedure dbo.usp2_Import_NSI_Transformators
go


IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'Import_NSI_Transformators_TableType' AND ss.name = N'dbo')
DROP TYPE [dbo].Import_NSI_Transformators_TableType
-- Пересоздаем заново
CREATE TYPE [dbo].[Import_NSI_Transformators_TableType] AS TABLE 
(
 SheetNumber int,
 RowNumber int,
 ColNumber int,
 
HierLev1_Name nvarchar(400),
HierLev1_ID int,
HierLev2_Name nvarchar(400),
HierLev2_ID int,
HierLev3_Name nvarchar(400),
HierLev3_ID int,
PS_Name nvarchar(400),
PS_ID int,

TI_Name nvarchar(400),
TI_ID int,

Meter_Type nvarchar(200),			--производитель ПУ (если указан то ищем еще и по нему, более тсрого получается)
Meter_SerialNumber nvarchar(200),
Meter_CalibrationDate datetime,
Meter_Place nvarchar(200),
Meter_ID int,
Meter_StampNumber nvarchar(200),
Meter_StampID int,

TTCoef1 float,
TTCoef2 float,
TTCoef float,

TTA_Model nvarchar(400),
TTA_Model_ID int,
TTA_Klass nvarchar(200),
TTA_SerialNumber nvarchar(200),
TTA_StampNumber nvarchar(200),
TTA_CalibrationDate datetime,
TTA_ID int,
TTA_StampID int,


TTB_Model nvarchar(400),
TTB_Model_ID int,
TTB_Klass nvarchar(200),
TTB_SerialNumber nvarchar(200),
TTB_StampNumber nvarchar(200),
TTB_CalibrationDate datetime,
TTB_ID int,
TTB_StampID int,


TTC_Model nvarchar(400),
TTC_Model_ID int,
TTC_Klass nvarchar(200),
TTC_SerialNumber nvarchar(200),
TTC_StampNumber nvarchar(200),
TTC_CalibrationDate datetime ,
TTC_ID int,
TTC_StampID int,

DistributingArrangement_Name nvarchar(200),
DistributingArrangement_ID int,
BusSystem_Name nvarchar(200),
BusSystem_ID int,
TNDesignation_Name nvarchar(200),
TNDesignation_ID int,

IsTNThreePhase bit,
TNCoef1 float,
TNCoef2 float,
TNCoef float,

TNA_Model nvarchar(400),
TNA_Model_ID int,
TNA_Klass nvarchar(200),
TNA_SerialNumber nvarchar(200),
TNA_StampNumber nvarchar(200),
TNA_CalibrationDate datetime,
TNA_ID int,
TNA_StampID int,


TNB_Model nvarchar(400),
TNB_Model_ID int,
TNB_Klass nvarchar(200),
TNB_SerialNumber nvarchar(200),
TNB_StampNumber nvarchar(200),
TNB_CalibrationDate datetime,
TNB_ID int,
TNB_StampID int,


TNC_Model nvarchar(400),
TNC_Model_ID int,
TNC_Klass nvarchar(200),
TNC_SerialNumber nvarchar(200),
TNC_StampNumber nvarchar(200),
TNC_CalibrationDate datetime ,
TNC_ID int,
TNC_StampID int,

AllowLoad bit,

ResultMessage nvarchar(1200)


)
GO

grant EXECUTE on TYPE::Import_NSI_Transformators_TableType to UserCalcService
go
grant EXECUTE on TYPE::Import_NSI_Transformators_TableType to UserDeclarator
go
 

 

create procedure dbo.usp2_Import_NSI_Transformators 
        @items Import_NSI_Transformators_TableType readonly, 
		@User_ID nvarchar(50)

 as

begin
 

--delete from TEMP_Import_NSI_Transformators

--insert into TEMP_Import_NSI_Transformators
--select  * from @items

-- select distinct 
--SheetNumber,
--SheetName='Лист '+Convert(varchar,SheetNumber), 
--RowID=res.RowNumber,
--Message='',
--ErrorMessage= res.ResultMessage
--from TEMP_Import_NSI_Transformators res
--where 
--(AllowLoad=0 or isnull(ResultMessage,'') <>'')

--return;



declare  @res Import_NSI_Transformators_TableType

insert into @res (SheetNumber,RowNumber,ColNumber,HierLev1_Name,HierLev1_ID,HierLev2_Name,HierLev2_ID,
HierLev3_Name,HierLev3_ID,PS_Name,PS_ID,TI_Name,TI_ID,
Meter_Type,Meter_SerialNumber,Meter_CalibrationDate,Meter_Place,Meter_ID,Meter_StampNumber,Meter_StampID,
TTCoef1,TTCoef2,TTCoef,
TTA_Model,TTA_Model_ID,TTA_Klass,TTA_SerialNumber,TTA_StampNumber,TTA_CalibrationDate,TTA_ID,TTA_StampID,
TTB_Model,TTB_Model_ID,TTB_Klass,TTB_SerialNumber,TTB_StampNumber,TTB_CalibrationDate,TTB_ID,TTB_StampID,
TTC_Model,TTC_Model_ID,TTC_Klass,TTC_SerialNumber,TTC_StampNumber,TTC_CalibrationDate,TTC_ID,TTC_StampID,
DistributingArrangement_Name,DistributingArrangement_ID,BusSystem_Name,BusSystem_ID,TNDesignation_Name,TNDesignation_ID,
IsTNThreePhase,TNCoef1,TNCoef2,TNCoef,
TNA_Model,TNA_Model_ID,TNA_Klass,TNA_SerialNumber,TNA_StampNumber,TNA_CalibrationDate,TNA_ID,TNA_StampID,
TNB_Model,TNB_Model_ID,TNB_Klass,TNB_SerialNumber,TNB_StampNumber,TNB_CalibrationDate,TNB_ID,TNB_StampID,
TNC_Model,TNC_Model_ID,TNC_Klass,TNC_SerialNumber,TNC_StampNumber,TNC_CalibrationDate,TNC_ID,TNC_StampID,AllowLoad,ResultMessage)

select distinct
SheetNumber,RowNumber,ColNumber,vw_Dict_Hierarchy.HierLev1Name,vw_Dict_Hierarchy.HierLev1_ID,vw_Dict_Hierarchy.HierLev2Name,vw_Dict_Hierarchy.HierLev2_ID,
vw_Dict_Hierarchy.HierLev3Name,vw_Dict_Hierarchy.HierLev3_ID,vw_Dict_Hierarchy.PSName,vw_Dict_Hierarchy.PS_ID,vw_Dict_Hierarchy.TIName,Info_Meters_TO_TI.TI_ID,

Meter_Type,
Meter_SerialNumber,Meter_CalibrationDate,Meter_Place,Hard_Meters.Meter_ID,Meter_StampNumber,Meter_StampID,

TTCoef1,TTCoef2,TTCoef,
TTA_Model,TTA_Model_ID,TTA_Klass,TTA_SerialNumber,TTA_StampNumber,TTA_CalibrationDate,TTA_ID,TTA_StampID,
TTB_Model,TTB_Model_ID,TTB_Klass,TTB_SerialNumber,TTB_StampNumber,TTB_CalibrationDate,TTB_ID,TTB_StampID,
TTC_Model,TTC_Model_ID,TTC_Klass,TTC_SerialNumber,TTC_StampNumber,TTC_CalibrationDate,TTC_ID,TTC_StampID,
DistributingArrangement_Name,DistributingArrangement_ID,BusSystem_Name,BusSystem_ID,TNDesignation_Name,TNDesignation_ID,
IsTNThreePhase,TNCoef1,TNCoef2,TNCoef,
TNA_Model,TNA_Model_ID,TNA_Klass,TNA_SerialNumber,TNA_StampNumber,TNA_CalibrationDate,TNA_ID,TNA_StampID,
TNB_Model,TNB_Model_ID,TNB_Klass,TNB_SerialNumber,TNB_StampNumber,TNB_CalibrationDate,TNB_ID,TNB_StampID,
TNC_Model,TNC_Model_ID,TNC_Klass,TNC_SerialNumber,TNC_StampNumber,TNC_CalibrationDate,TNC_ID,TNC_StampID,isnull(AllowLoad,1),isnull(ResultMessage,'')
 from 
@items items
left join Hard_Meters  on len(isnull(items.Meter_SerialNumber,''))>3
	and  items.Meter_SerialNumber like Hard_Meters.MeterSerialNumber  
left join Info_Meters_TO_TI on Info_Meters_TO_TI.Meter_ID=Hard_Meters.Meter_ID 
	and Info_Meters_TO_TI.StartDateTime<=getdate() and (Info_Meters_TO_TI.FinishDateTime is null or Info_Meters_TO_TI.FinishDateTime>=getdate())

left join vw_Dict_Hierarchy on vw_Dict_Hierarchy.TI_ID= Info_Meters_TO_TI.TI_ID
where 
items.AllowLoad=1
order by RowNumber, TI_ID, Meter_ID


--трехфазные иногда одной файзой описывают!!!
update @res set 
TNB_Model= TNA_Model,
TNB_SerialNumber=TNA_SerialNumber,
TNB_StampNumber=TNA_StampNumber,
TNB_CalibrationDate= TNA_CalibrationDate,
TNC_Model= TNA_Model,
TNC_SerialNumber=TNA_SerialNumber,
TNC_StampNumber=TNA_StampNumber,
TNC_CalibrationDate= TNA_CalibrationDate
where 
TNA_Model is not null
and len(isnull(TNA_SerialNumber,''))>2
and isnull(TNB_SerialNumber,'')=''
and isnull(TNC_SerialNumber,'')=''


--коэфф тоже кривые 
update @res 
set AllowLoad=0, ResultMessage= ResultMessage+ ' некорректный коэфф тр. ТТ; '
where TTA_Model is not null
and TTA_Model<>''
and TTA_Model<>'-'
and (isnull(TTCoef1,0)<1
or isnull(TTCoef2,0)<1
or isnull(TTCoef,0)<1)


update @res 
set AllowLoad=0, ResultMessage= ResultMessage+ ' некорректный коэфф тр. ТН; '
where TNA_Model is not null
and TNA_Model<>''
and TNA_Model<>'-'
and (isnull(TNCoef1,0)<1
or isnull(TNCoef2,0)<1
or isnull(TNCoef,0)<1)


update @res set AllowLoad=0, ResultMessage= ResultMessage+ ' не найдена ТИ; '
where TI_ID is null



update @res set AllowLoad=0, ResultMessage= ResultMessage+' некорректный номер ТН; '
where 
 AllowLoad=1
and  TNA_Model is not null
and len(isnull(TNA_SerialNumber,''))>2
and len(isnull(TNA_SerialNumber,''))>0
and len(isnull(TNA_SerialNumber,''))<1


update @res set AllowLoad=0, ResultMessage= ResultMessage+' некорректный номер ТТ; '
where  AllowLoad=1
and  TTA_Model is not null
and len(isnull(TTA_SerialNumber,''))>2
and len(isnull(TTA_SerialNumber,''))>0
and len(isnull(TTA_SerialNumber,''))<1




Update @res 
set AllowLoad=0, 
ResultMessage=ResultMessage+' пломба указана для нескольких ПУ; '
from @res res
where exists 
(select top 1 1 from @res res2 where res.RowNumber<> res2.RowNumber and res.Meter_StampNumber = res2.Meter_StampNumber and res.Meter_SerialNumber<> res2.Meter_SerialNumber)
and isnull(res.Meter_StampNumber,'-')<>'-'
and isnull(res.Meter_SerialNumber,'-')<>'-'

Update @res 
set AllowLoad=0, 
ResultMessage=ResultMessage+' пломба TTA указана для нескольких трансформаторов; '
from @res res
where 
exists 
(select top 1 1 from @res res2 where res.RowNumber<> res2.RowNumber 
	and ((isnull(res.TTA_StampNumber,'') like isnull(res2.TTA_StampNumber,'') and isnull(res.TTA_SerialNumber,'')<> isnull(res2.TTA_SerialNumber,''))
	or
	(isnull(res.TTA_StampNumber,'') like isnull(res2.TTB_StampNumber,'') and isnull(res.TTA_SerialNumber,'')<> isnull(res2.TTB_SerialNumber,''))
	or
	(isnull(res.TTA_StampNumber,'') like isnull(res2.TTA_StampNumber,'') and isnull(res.TTA_SerialNumber,'')<> isnull(res2.TTA_SerialNumber,''))
	)
)
and isnull(res.TTA_StampNumber,'-')<>'-'
and isnull(res.TTA_SerialNumber,'-')<>'-'


Update @res 
set AllowLoad=0, 
ResultMessage=ResultMessage+' пломба TTB указана для нескольких трансформаторов; '
from @res res
where 
exists 
(select top 1 1 from @res res2 where res.RowNumber<> res2.RowNumber 
	and ((isnull(res.TTB_StampNumber,'') like isnull(res2.TTB_StampNumber,'') and isnull(res.TTB_SerialNumber,'')<> isnull(res2.TTB_SerialNumber,''))
	or
	(isnull(res.TTB_StampNumber,'') like isnull(res2.TTB_StampNumber,'') and isnull(res.TTB_SerialNumber,'')<> isnull(res2.TTB_SerialNumber,''))
	or
	(isnull(res.TTB_StampNumber,'') like isnull(res2.TTB_StampNumber,'') and isnull(res.TTB_SerialNumber,'')<> isnull(res2.TTB_SerialNumber,''))
	)
)
and isnull(res.TTB_StampNumber,'-')<>'-'
and isnull(res.TTB_SerialNumber,'-')<>'-'


Update @res 
set AllowLoad=0, 
ResultMessage=ResultMessage+' пломба TTC указана для нескольких трансформаторов; '
from @res res
where 
 isnull(res.TTC_StampNumber,'-')<>'-'
and isnull(res.TTC_SerialNumber,'-')<>'-'
and
exists 
(select top 1 1 from @res res2 
	where res.RowNumber<> res2.RowNumber 
	and (
		(isnull(res.TTC_StampNumber,'') like isnull(res2.TTC_StampNumber,'') and isnull(res.TTC_SerialNumber,'') not like isnull(res2.TTC_SerialNumber,''))
		or
		(isnull(res.TTC_StampNumber,'') like isnull(res2.TTC_StampNumber,'') and isnull(res.TTC_SerialNumber,'')not like isnull(res2.TTC_SerialNumber,''))
		or
		(isnull(res.TTC_StampNumber,'') like isnull(res2.TTC_StampNumber,'') and isnull(res.TTC_SerialNumber,'')not like isnull(res2.TTC_SerialNumber,''))
	)
)




--max ID
declare @maxID int
set @maxID=0

--=====================================================================

print ('РАСПРЕД УСТРОЙСТВО ')
--РАСПРЕД УСТРОЙСТВО 
--находим первое попавшиеся с таким же напряжением
 update @res
set 
DistributingArrangement_ID= Dict_DistributingArrangement.DistributingArrangement_ID,
DistributingArrangement_Name=Dict_DistributingArrangement.StringName
from @res res join Dict_DistributingArrangement on res.PS_ID= Dict_DistributingArrangement.PS_ID and res.TNCoef1= Dict_DistributingArrangement.Voltage*1000.0
where 
res.AllowLoad=1
and res.PS_ID is not null
and res.DistributingArrangement_ID is null
and isnull(res.TNA_Model,'')<>'-'
and isnull(res.TNA_Model,'')<>''
and isnull(res.TNA_SerialNumber,'')<>'-'
and isnull(res.TNA_SerialNumber,'')<>''
and res.TNCoef1 is not null
and res.TNCoef1>0.0

Begin transaction Dict_DistributingArrangement_1 

	--находим последний максимальный ИД
	select @maxID= max(DistributingArrangement_ID)
	from Dict_DistributingArrangement

	set @maxID= isnull(@maxID,0)


	 
	--вставляем отсутствующие
	insert into Dict_DistributingArrangement (
	DistributingArrangement_ID,
	PS_ID,
	StringName,
	Voltage,
	DistributingArrangementType)

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY PS_ID ASC) AS  DistributingArrangement_ID,
		PS_ID,
		DistributingArrangement_Name,
		Voltage,DistributingArrangementType=0
	from 
	(
		select distinct 
		PS_ID,
		DistributingArrangement_Name='РУ '+replace(convert(varchar(200),cast(res.TNCoef1/1000.0 as decimal(10,1))),'.0','')+'кВ',
		Voltage=res.TNCoef1/1000.0
		from @res res
		where
		AllowLoad=1
		and PS_ID is not null
		and DistributingArrangement_ID is null
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
		and TNCoef1 is not null
		and TNCoef1>0.0
		
	)
	as temp 
 

 COMMIT transaction Dict_DistributingArrangement_1



 --снова обновляем
 update @res
set 
DistributingArrangement_ID= Dict_DistributingArrangement.DistributingArrangement_ID,
DistributingArrangement_Name=Dict_DistributingArrangement.StringName
from @res res join Dict_DistributingArrangement on res.PS_ID= Dict_DistributingArrangement.PS_ID and res.TNCoef1= Dict_DistributingArrangement.Voltage*1000.0
where 
res.AllowLoad=1
and res.PS_ID is not null
and res.DistributingArrangement_ID is null
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
and res.TNCoef1 is not null
and res.TNCoef1>0.0
 
--=====================================================================


 --=====================================================================
 
print ('Система шин ')
--Система шин
--находим первое попавшиеся с таким же напряжением
 update @res
set 
BusSystem_ID= Dict_BusSystem.BusSystem_ID,
BusSystem_Name=Dict_BusSystem.StringName
from @res res join Dict_BusSystem on res.DistributingArrangement_ID= Dict_BusSystem.DistributingArrangement_ID and res.TNCoef1= Dict_BusSystem.Voltage*1000.0
where 
res.AllowLoad=1
and res.BusSystem_ID is null
and res.DistributingArrangement_ID is not null
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
and res.TNCoef1 is not null
and res.TNCoef1>0.0

Begin transaction BusSystem_ID_1 

	--находим последний максимальный ИД
	select @maxID= max(BusSystem_ID)
	from Dict_BusSystem

	set @maxID= isnull(@maxID,0)

	--вставляем отсутствующие
	insert into Dict_BusSystem (
	BusSystem_ID,
	DistributingArrangement_ID ,
	StringName,
	Voltage)
	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY DistributingArrangement_ID ASC) AS  BusSystem_ID,
		DistributingArrangement_ID,
		BusSystem_Name,
		Voltage 
	from 
	(
		select distinct 
		DistributingArrangement_ID,
		BusSystem_Name='СШ '+replace(convert(varchar(200),cast(res.TNCoef1/1000.0 as decimal(10,1))),'.0','')+'кВ',
		Voltage=res.TNCoef1/1000.0
		from @res res
		where
		AllowLoad=1
		and BusSystem_ID is null
		and DistributingArrangement_ID is not null
				and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
		and TNCoef1 is not null
		and TNCoef1>0.0
		
	)
	as temp 
 
 COMMIT transaction BusSystem_ID_1



--снова обновляем
 update @res
set 
BusSystem_ID= Dict_BusSystem.BusSystem_ID,
BusSystem_Name=Dict_BusSystem.StringName
from @res res join Dict_BusSystem on res.DistributingArrangement_ID= Dict_BusSystem.DistributingArrangement_ID and res.TNCoef1= Dict_BusSystem.Voltage*1000.0
where 
res.AllowLoad=1
and res.BusSystem_ID is null
and res.DistributingArrangement_ID is not null
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
and res.TNCoef1 is not null
and res.TNCoef1>0.0

--=====================================================================

--=====================================================================
--МОДЕЛИ ТН (пока считаем что одинаковые, по фазе а)

print ('МОДЕЛИ ТН')

update @res 
set 
TNA_Model_ID= Dict_Transformators_TN_Model.TNModel_ID ,
TNB_Model_ID= Dict_Transformators_TN_Model.TNModel_ID ,
TNC_Model_ID= Dict_Transformators_TN_Model.TNModel_ID 
from @res res join Dict_Transformators_TN_Model 
on res.TNA_Model like Dict_Transformators_TN_Model.StringName  
				and isnull(res.TNA_Klass,'') like isnull(Dict_Transformators_TN_Model.ClassName,'')
				and Dict_Transformators_TN_Model.CoefUHigh=convert(int,res.TNCoef1)
				and Dict_Transformators_TN_Model.CoefULow=convert(int,res.TNCoef2)
where 
res.AllowLoad=1 
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
and res.TNCoef is not null
and res.TNCoef1 is not null
and res.TNCoef2 is not null
and res.TNCoef>0.0
and res.TNA_Model_ID is null


Begin transaction TNModel_ID_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TNModel_ID)
	from Dict_Transformators_TN_Model

	set @maxID= isnull(@maxID,0)
	 

	--вставляем отсутствующие
	insert into Dict_Transformators_TN_Model(
	TNModel_ID,StringName,
		 IsThreePhase,
		 CoefUHigh,
		 CoefULow,ClassName )
	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY StringName ASC) AS  TNModel_ID,
		 StringName,
		 IsThreePhase,
		 CoefUHigh,
		 CoefULow,
		 ClassName
		 
	from 
	(


		select distinct 
		StringName= TNA_Model,
		IsThreePhase = case when isnull(TNA_SerialNumber,'')=isnull(TNB_SerialNumber,'') and  isnull(TNA_SerialNumber,'')=isnull(TNC_SerialNumber,'') and  isnull(TNB_SerialNumber,'')=isnull(TNC_SerialNumber,'') then 1
						else 0 end ,
		CoefUHigh=convert(int,TNCoef1),
		CoefULow=convert(int,TNCoef2),
		ClassName= isnull(TNA_Klass,'')
		
		from @res res
		 
		where
		AllowLoad=1 
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
		and res.TNCoef is not null
		and res.TNCoef1 is not null
		and res.TNCoef2 is not null
		and TNCoef1>0.0
		and TNA_Model_ID is null

		
	)
	as temp 
	
	COMMIT transaction TNModel_ID_1
 end try
 begin catch 
	rollback transaction TNModel_ID_1
 end catch

 --снова обновляем
update @res 
set 
TNA_Model_ID= Dict_Transformators_TN_Model.TNModel_ID ,
TNB_Model_ID= Dict_Transformators_TN_Model.TNModel_ID ,
TNC_Model_ID= Dict_Transformators_TN_Model.TNModel_ID 
from @res res join Dict_Transformators_TN_Model 
on res.TNA_Model like Dict_Transformators_TN_Model.StringName  
				and isnull(res.TNA_Klass,'') like isnull(Dict_Transformators_TN_Model.ClassName,'')
				and Dict_Transformators_TN_Model.CoefUHigh=convert(int,res.TNCoef1)
				and Dict_Transformators_TN_Model.CoefULow=convert(int,res.TNCoef2)
where 
res.AllowLoad=1 
		and isnull(res.TNA_Model,'')<>'-'
		and isnull(res.TNA_Model,'')<>''
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''
and res.TNCoef is not null
and res.TNCoef1 is not null
and res.TNCoef2 is not null
and res.TNCoef>0.0
and res.TNA_Model_ID is null



--=====================================================================

print ('ТРАНСФОРМАТОРЫ НАПРЯЖЕНИЯ ')

--ТРАНСФОРМАТОРЫ НАПРЯЖЕНИЯ 
--не будем ничего выдумывать.. ищем ТН с моделью и заводским номером -  они должны быть уникальны

--TNA
update @res 
set 
TNA_ID= Hard_Transformators_TN.TN_ID 
from @res res join Hard_Transformators_TN 
on res.TNA_Model_ID like Hard_Transformators_TN.TNModel_ID  and res.TNA_SerialNumber like  Hard_Transformators_TN.SerialNumber
where 
res.AllowLoad=1 
and res.TNA_ID is null
and res.TNA_Model_ID is not null 
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''

Begin transaction TNA_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TN_ID)
	from Hard_Transformators_TN

	set @maxID= isnull(@maxID,0)
	  
	--вставляем отсутствующие
	insert into Hard_Transformators_TN(
	TN_ID, TNModel_ID,ModelName,
		 SerialNumber, CalibrationDate)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY ModelName ASC) AS  TN_ID,
		TNModel_ID,ModelName,
		 SerialNumber, CalibrationDate		 
	from 
	(
		select distinct 
			TNModel_ID=res.TNA_Model_ID,
			ModelName= TNA_Model,
			SerialNumber= TNA_SerialNumber,
			CalibrationDate= TNA_CalibrationDate		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TNA_ID is null
		and res.TNA_Model_ID is not null
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''	
	)
	as temp 
	
	COMMIT transaction TNA_1
 end try
 begin catch 
	rollback transaction TNA_1
 end catch

 --снова обновляем
 update @res 
set 
TNA_ID= Hard_Transformators_TN.TN_ID 
from @res res join Hard_Transformators_TN 
on res.TNA_Model_ID like Hard_Transformators_TN.TNModel_ID  and res.TNA_SerialNumber like  Hard_Transformators_TN.SerialNumber
where 
res.AllowLoad=1 
and res.TNA_ID is null
and res.TNA_Model_ID is not null
		and isnull(res.TNA_SerialNumber,'')<>'-'
		and isnull(res.TNA_SerialNumber,'')<>''


--TNB
update @res 
set 
TNB_ID= Hard_Transformators_TN.TN_ID 
from @res res join Hard_Transformators_TN 
on res.TNB_Model_ID like Hard_Transformators_TN.TNModel_ID  and res.TNB_SerialNumber like  Hard_Transformators_TN.SerialNumber
where 
res.AllowLoad=1 
and res.TNB_ID is null
and res.TNB_Model_ID is not null 
and isnull(res.TNB_SerialNumber,'')<>'-'
and isnull(res.TNB_SerialNumber,'')<>''

Begin transaction TNB_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TN_ID)
	from Hard_Transformators_TN

	set @maxID= isnull(@maxID,0)
	  
	--вставляем отсутствующие
	insert into Hard_Transformators_TN(
	TN_ID, TNModel_ID,ModelName,
		 SerialNumber, CalibrationDate)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY ModelName ASC) AS  TN_ID,
		TNModel_ID,ModelName,
		 SerialNumber, CalibrationDate		 
	from 
	(
		select distinct 
			TNModel_ID=res.TNB_Model_ID,
			ModelName= TNB_Model,
			SerialNumber= TNB_SerialNumber,
			CalibrationDate= TNB_CalibrationDate		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TNB_ID is null
		and res.TNB_Model_ID is not null
and isnull(res.TNB_SerialNumber,'')<>'-'
and isnull(res.TNB_SerialNumber,'')<>''		
	)
	as temp 
	
	COMMIT transaction TNB_1
 end try
 begin catch 
	rollback transaction TNB_1
 end catch

 --снова обновляем
 update @res 
set 
TNB_ID= Hard_Transformators_TN.TN_ID 
from @res res join Hard_Transformators_TN 
on res.TNB_Model_ID like Hard_Transformators_TN.TNModel_ID  and res.TNB_SerialNumber like  Hard_Transformators_TN.SerialNumber
where 
res.AllowLoad=1 
and res.TNB_ID is null
and res.TNB_Model_ID is not null
and isnull(res.TNB_SerialNumber,'')<>'-'
and isnull(res.TNB_SerialNumber,'')<>''


--TNC
update @res 
set 
TNC_ID= Hard_Transformators_TN.TN_ID 
from @res res join Hard_Transformators_TN 
on res.TNC_Model_ID like Hard_Transformators_TN.TNModel_ID  and res.TNC_SerialNumber like  Hard_Transformators_TN.SerialNumber
where 
res.AllowLoad=1 
and res.TNC_ID is null
and res.TNC_Model_ID is not null 
and isnull(res.TNC_SerialNumber,'')<>'-'
and isnull(res.TNC_SerialNumber,'')<>''

Begin transaction TNC_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TN_ID)
	from Hard_Transformators_TN

	set @maxID= isnull(@maxID,0)
	  
	--вставляем отсутствующие
	insert into Hard_Transformators_TN(
	TN_ID, TNModel_ID,ModelName,
		 SerialNumber, CalibrationDate)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY ModelName ASC) AS  TN_ID,
		TNModel_ID,ModelName,
		 SerialNumber, CalibrationDate		 
	from 
	(
		select distinct 
			TNModel_ID=res.TNC_Model_ID,
			ModelName= TNC_Model,
			SerialNumber= TNC_SerialNumber,
			CalibrationDate= TNC_CalibrationDate		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TNC_ID is null
		and res.TNC_Model_ID is not null
		and isnull(res.TNC_SerialNumber,'')<>'-' 
and isnull(res.TNC_SerialNumber,'')<>''		
	)
	as temp 
	
	COMMIT transaction TNC_1
 end try
 begin catch 
	rollback transaction TNC_1
 end catch

 --снова обновляем
 update @res 
set 
TNC_ID= Hard_Transformators_TN.TN_ID 
from @res res join Hard_Transformators_TN 
on res.TNC_Model_ID like Hard_Transformators_TN.TNModel_ID  and res.TNC_SerialNumber like  Hard_Transformators_TN.SerialNumber
where 
res.AllowLoad=1 
and res.TNC_ID is null
and res.TNC_Model_ID is not null
and isnull(res.TNC_SerialNumber,'')<>'-' 
and isnull(res.TNC_SerialNumber,'')<>''


 
 
 --=====================================================================
  
print ('ОБОЗНАЧЕНИЕ ТН  ')
--ОБОЗНАЧЕНИЕ ТН на СШ 
--сначала надо создать сами трансформаторы чтобы строго по фазам искать

 update @res
set 
TNDesignation_ID= Dict_TNDesignation.TNDesignation_ID,
TNDesignation_Name=Dict_TNDesignation.StringName
from @res res join Dict_TNDesignation on res.BusSystem_ID= Dict_TNDesignation.BusSystem_ID  
where 
res.AllowLoad=1
and res.TNDesignation_ID is null
and res.BusSystem_ID is not null
and isnull(res.TNA_Model,'')<>'-'
and res.TNCoef is not null
and res.TNCoef>0.0
and isnull(res.TNA_ID,0)= isnull(Dict_TNDesignation.TNA_ID,0)
and isnull(res.TNB_ID,0)= isnull(Dict_TNDesignation.TNB_ID,0)
and isnull(res.TNC_ID,0)= isnull(Dict_TNDesignation.TNC_ID,0)

Begin transaction TNDesignation_ID_1 

	--находим последний максимальный ИД
	select @maxID= max(TNDesignation_ID)
	from Dict_TNDesignation

	set @maxID= isnull(@maxID,0)
	 

	--вставляем отсутствующие
	insert into Dict_TNDesignation (
	TNDesignation_ID,
	BusSystem_ID ,
	StringName,
		TNA_ID,
		TNB_ID,
		TNC_ID )
	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY BusSystem_ID ASC) AS  TNDesignation_ID,
		BusSystem_ID,
		TNDesignation_Name,
		TNA_ID,
		TNB_ID,
		TNC_ID
	from 
	(
		select distinct 
		BusSystem_ID,
		TNDesignation_Name='ТН - '+convert(varchar,(isnull(TNDesignationCount.TNMaxNumber,0)+1))+', '+ replace(convert(varchar(200),cast(res.TNCoef1/1000.0 as decimal(10,1))),'.0','')+'кВ',
		TNA_ID,
		TNB_ID,
		TNC_ID 
		from @res res
		outer apply (select TNMaxNumber= count(a.TNDesignation_ID) from Dict_TNDesignation  a where a.BusSystem_ID=res.BusSystem_ID) as TNDesignationCount
		where
		AllowLoad=1
		and TNDesignation_ID is null
		and BusSystem_ID is not null
		and isnull(TNA_Model,'')<>'-'
		and TNCoef1 is not null
		and TNCoef1>0.0
		and (TNA_ID is not null or TNB_ID is not null or TNC_ID is not null)
		
	)
	as temp 
 
 COMMIT transaction TNDesignation_ID_1

 --снова обновляем
 update @res
set 
TNDesignation_ID= Dict_TNDesignation.TNDesignation_ID,
TNDesignation_Name=Dict_TNDesignation.StringName
from @res res join Dict_TNDesignation on res.BusSystem_ID= Dict_TNDesignation.BusSystem_ID  
where 
res.AllowLoad=1
and res.TNDesignation_ID is null
and res.BusSystem_ID is not null
and isnull(res.TNA_Model,'')<>'-'
and res.TNCoef is not null
and res.TNCoef>0.0
and isnull(res.TNA_ID,0)= isnull(Dict_TNDesignation.TNA_ID,0)
and isnull(res.TNB_ID,0)= isnull(Dict_TNDesignation.TNB_ID,0)
and isnull(res.TNC_ID,0)= isnull(Dict_TNDesignation.TNC_ID,0)


--=====================================================================

 
 --=====================================================================
 
  
print ('МОДЕЛИ ТT ')
--МОДЕЛИ ТT (пока считаем что одинаковые, по фазе а)

update @res 
set 
TTA_Model_ID= Dict_Transformators_TT_Model.TTModel_ID ,
TTB_Model_ID= Dict_Transformators_TT_Model.TTModel_ID ,
TTC_Model_ID= Dict_Transformators_TT_Model.TTModel_ID 
from @res res join Dict_Transformators_TT_Model 
on res.TTA_Model like Dict_Transformators_TT_Model.StringName  
				and isnull(res.TTA_Klass,'') like isnull(Dict_Transformators_TT_Model.ClassName,'')
				and Dict_Transformators_TT_Model.CoefIHigh=convert(int,res.TTCoef1)
				and Dict_Transformators_TT_Model.CoefILow=convert(int,res.TTCoef2)
where 
res.AllowLoad=1 
and isnull(res.TTA_Model,'')<>'-'
and isnull(res.TTA_Model,'')<>''
and res.TTCoef is not null
and res.TTCoef1 is not null
and res.TTCoef2 is not null
and res.TTCoef>0.0
and res.TTA_Model_ID is null


Begin transaction TTModel_ID_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TTModel_ID)
	from Dict_Transformators_TT_Model

	set @maxID= isnull(@maxID,0)
	 

	--вставляем отсутствующие
	insert into Dict_Transformators_TT_Model(
	TTModel_ID,StringName, 
		 CoefIHigh,
		 CoefILow,ClassName )
	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY StringName ASC) AS  TTModel_ID,
		 StringName, 
		 CoefIHigh,
		 CoefILow,
		 ClassName
		 
	from 
	(


		select distinct 
		StringName= TTA_Model, 
		CoefIHigh=convert(int,TTCoef1),
		CoefILow=convert(int,TTCoef2),
		ClassName= isnull(TTA_Klass,'')
		
		from @res res
		 
		where
		AllowLoad=1 
		and isnull(TTA_Model,'')<>'-'
and isnull(res.TTA_Model,'')<>''
		and TTCoef is not null
		and TTCoef>0.0 
		and TTA_Model_ID is null
		
	)
	as temp 
	
	COMMIT transaction TTModel_ID_1
 end try
 begin catch 
	rollback transaction TTModel_ID_1
 end catch

 --снова обновляем
update @res 
set 
TTA_Model_ID= Dict_Transformators_TT_Model.TTModel_ID ,
TTB_Model_ID= Dict_Transformators_TT_Model.TTModel_ID ,
TTC_Model_ID= Dict_Transformators_TT_Model.TTModel_ID 
from @res res join Dict_Transformators_TT_Model 
on res.TTA_Model like Dict_Transformators_TT_Model.StringName  
				and isnull(res.TTA_Klass,'') like isnull(Dict_Transformators_TT_Model.ClassName,'')
				and Dict_Transformators_TT_Model.CoefIHigh=convert(int,res.TTCoef1)
				and Dict_Transformators_TT_Model.CoefILow=convert(int,res.TTCoef2)
where 
res.AllowLoad=1 
and isnull(res.TTA_Model,'')<>'-'
and isnull(res.TTA_Model,'')<>''
and res.TTCoef is not null
and res.TTCoef1 is not null
and res.TTCoef2 is not null
and res.TTCoef>0.0
and res.TTA_Model_ID is null


--=====================================================================


print ('ТРАНСФОРМАТОРЫ ТОКА ')

--ТРАНСФОРМАТОРЫ ТОКА
--не будем ничего выдумывать.. ищем ТР с моделью и заводским номером -  они должны быть уникальны

--TTA
update @res 
set 
TTA_ID= Hard_Transformators_TT.TT_ID 
from @res res join Hard_Transformators_TT 
on res.TTA_Model_ID like Hard_Transformators_TT.TTModel_ID  and res.TTA_SerialNumber like  Hard_Transformators_TT.SerialNumber
where 
res.AllowLoad=1 
and res.TTA_ID is null
and res.TTA_Model_ID is not null 
and isnull(res.TTA_SerialNumber,'')<>'-'
and isnull(res.TTA_SerialNumber,'')<>''

Begin transaction TTA_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TT_ID)
	from Hard_Transformators_TT

	set @maxID= isnull(@maxID,0)
	  
	--вставляем отсутствующие
	insert into Hard_Transformators_TT(
	TT_ID, TTModel_ID,ModelName,
		 SerialNumber, CalibrationDate)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY ModelName ASC) AS  TT_ID,
		TTModel_ID,ModelName,
		 SerialNumber, CalibrationDate		 
	from 
	(
		select distinct 
			TTModel_ID=res.TTA_Model_ID,
			ModelName= TTA_Model,
			SerialNumber= TTA_SerialNumber,
			CalibrationDate= TTA_CalibrationDate		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TTA_ID is null
		and res.TTA_Model_ID is not null
		and isnull(res.TTA_SerialNumber,'')<>'-' 	
and isnull(res.TTA_SerialNumber,'')<>''	
	)
	as temp 
	
	COMMIT transaction TTA_1
 end try
 begin catch 
	rollback transaction TTA_1
 end catch

 --снова обновляем
 update @res 
set 
TTA_ID= Hard_Transformators_TT.TT_ID 
from @res res join Hard_Transformators_TT 
on res.TTA_Model_ID like Hard_Transformators_TT.TTModel_ID  and res.TTA_SerialNumber like  Hard_Transformators_TT.SerialNumber
where 
res.AllowLoad=1 
and res.TTA_ID is null
and res.TTA_Model_ID is not null
and isnull(res.TTA_SerialNumber,'')<>'-' 
and isnull(res.TTA_SerialNumber,'')<>''


--TTB
update @res 
set 
TTB_ID= Hard_Transformators_TT.TT_ID 
from @res res join Hard_Transformators_TT 
on res.TTB_Model_ID like Hard_Transformators_TT.TTModel_ID  and res.TTB_SerialNumber like  Hard_Transformators_TT.SerialNumber
where 
res.AllowLoad=1 
and res.TTB_ID is null
and res.TTB_Model_ID is not null 
and isnull(res.TTB_SerialNumber,'')<>'-'
and isnull(res.TTB_SerialNumber,'')<>''

Begin transaction TTB_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TT_ID)
	from Hard_Transformators_TT

	set @maxID= isnull(@maxID,0)
	  
	--вставляем отсутствующие
	insert into Hard_Transformators_TT(
	TT_ID, TTModel_ID,ModelName,
		 SerialNumber, CalibrationDate)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY ModelName ASC) AS  TT_ID,
		TTModel_ID,ModelName,
		 SerialNumber, CalibrationDate		 
	from 
	(
		select distinct 
			TTModel_ID=res.TTB_Model_ID,
			ModelName= TTB_Model,
			SerialNumber= TTB_SerialNumber,
			CalibrationDate= TTB_CalibrationDate		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TTB_ID is null
		and res.TTB_Model_ID is not null
		and isnull(res.TTB_SerialNumber,'')<>'-' 	
and isnull(res.TTB_SerialNumber,'')<>''	
	)
	as temp 
	
	COMMIT transaction TTB_1
 end try
 begin catch 
	rollback transaction TTB_1
 end catch

 --снова обновляем
 update @res 
set 
TTB_ID= Hard_Transformators_TT.TT_ID 
from @res res join Hard_Transformators_TT 
on res.TTB_Model_ID like Hard_Transformators_TT.TTModel_ID  and res.TTB_SerialNumber like  Hard_Transformators_TT.SerialNumber
where 
res.AllowLoad=1 
and res.TTB_ID is null
and res.TTB_Model_ID is not null
and isnull(res.TTB_SerialNumber,'')<>'-' 
and isnull(res.TTB_SerialNumber,'')<>''

--TTC
update @res 
set 
TTC_ID= Hard_Transformators_TT.TT_ID 
from @res res join Hard_Transformators_TT 
on res.TTC_Model_ID like Hard_Transformators_TT.TTModel_ID  and res.TTC_SerialNumber like  Hard_Transformators_TT.SerialNumber
where 
res.AllowLoad=1 
and res.TTC_ID is null
and res.TTC_Model_ID is not null 
and isnull(res.TTC_SerialNumber,'')<>'-'
and isnull(res.TTC_SerialNumber,'')<>''

Begin transaction TTC_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(TT_ID)
	from Hard_Transformators_TT

	set @maxID= isnull(@maxID,0)
	  
	--вставляем отсутствующие
	insert into Hard_Transformators_TT(
	TT_ID, TTModel_ID,ModelName,
		 SerialNumber, CalibrationDate)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY ModelName ASC) AS  TT_ID,
		TTModel_ID,ModelName,
		 SerialNumber, CalibrationDate		 
	from 
	(
		select distinct 
			TTModel_ID=res.TTC_Model_ID,
			ModelName= TTC_Model,
			SerialNumber= TTC_SerialNumber,
			CalibrationDate= TTC_CalibrationDate		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TTC_ID is null
		and res.TTC_Model_ID is not null
		and isnull(res.TTC_SerialNumber,'')<>'-' 	
and isnull(res.TTC_SerialNumber,'')<>''	
	)
	as temp 
	
	COMMIT transaction TTC_1
 end try
 begin catch 
	rollback transaction TTC_1
 end catch

 --снова обновляем
 update @res 
set 
TTC_ID= Hard_Transformators_TT.TT_ID 
from @res res join Hard_Transformators_TT 
on res.TTC_Model_ID like Hard_Transformators_TT.TTModel_ID  and res.TTC_SerialNumber like  Hard_Transformators_TT.SerialNumber
where 
res.AllowLoad=1 
and res.TTC_ID is null
and res.TTC_Model_ID is not null
and isnull(res.TTC_SerialNumber,'')<>'-' 
and isnull(res.TTC_SerialNumber,'')<>''




 


---====================================================

print ('ПЛОМБЫ ')
--ПЛОМБЫ
--ПЛОМБЫ ТИ

--удаляем если есть старые
update  Info_Stamps
set PrevStamp_ID=null
where 
Info_Stamps.PrevStamp_ID in (select Stamp_ID from Info_Stamps_To_Device
						where 
							(Meter_ID is not null and Meter_ID in (select Meter_ID from @res  res where res.Meter_ID is not null))
							or
							(TTA_ID is not null and TTA_ID in (select TTA_ID from @res  res where res.TTA_ID is not null))
							or
							(TTB_ID is not null and TTB_ID in (select TTB_ID from @res  res where res.TTB_ID is not null))
							or
							( TTC_ID is not null and TTC_ID in (select TTC_ID from @res  res where res.TTC_ID is not null))
							or
							( TNA_ID is not null and TNA_ID in (select TNA_ID from @res  res where res.TNA_ID is not null))
							or
							( TNB_ID is not null and TNB_ID in (select TNB_ID from @res  res where res.TNB_ID is not null))
							or
							( TNC_ID is not null and TNC_ID in (select TNC_ID from @res  res where res.TNC_ID is not null))
						)

delete from Info_Stamps
where 
Info_Stamps.Stamp_ID in (select Stamp_ID from Info_Stamps_To_Device
						where 
							(Meter_ID is not null and Meter_ID in (select Meter_ID from @res  res where res.Meter_ID is not null))
							or
							(TTA_ID is not null and TTA_ID in (select TTA_ID from @res  res where res.TTA_ID is not null))
							or
							(TTB_ID is not null and TTB_ID in (select TTB_ID from @res  res where res.TTB_ID is not null))
							or
							( TTC_ID is not null and TTC_ID in (select TTC_ID from @res  res where res.TTC_ID is not null))
							or
							( TNA_ID is not null and TNA_ID in (select TNA_ID from @res  res where res.TNA_ID is not null))
							or
							( TNB_ID is not null and TNB_ID in (select TNB_ID from @res  res where res.TNB_ID is not null))
							or
							( TNC_ID is not null and TNC_ID in (select TNC_ID from @res  res where res.TNC_ID is not null))
						)


--теперь добавляем  
--СЧЕТЧИК
 
 --можно дополнительно сравнивать с номером+ коментарий  
update @res 
set 
Meter_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.Meter_StampNumber,'') like isnull(Info_Stamps.StampNumber,'=')  and isnull(Info_Stamps.InstallationComment,'') like  'ПУ'
where 
res.AllowLoad=1 
and res.Meter_StampID is null
and res.Meter_ID is not null
and isnull(res.Meter_StampNumber,'-')<>'-' 	
and isnull(res.Meter_StampNumber,'')<>'' 


declare @getdate datetime = getdate()

 


Begin transaction StampMeter_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(Stamp_ID)
	from Info_Stamps

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps(
	Stamp_ID, StampNumber,InstallationDate , InstallationComment, CreateDateTime)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY StampNumber ASC) AS  Stamp_ID,
		 StampNumber,InstallationDate , 'ПУ', @getdate	 
	from 
	(
		select distinct 
			StampNumber=Meter_StampNumber, InstallationDate=isnull(Meter_CalibrationDate,'2001-01-01')		
		from @res res		 
		where
		res.AllowLoad=1 
		and res.Meter_StampID is null
		and res.Meter_ID is not null
		and isnull(res.Meter_StampNumber,'-')<>'-' 		
and isnull(res.Meter_StampNumber,'')<>'' 			
	)
	as temp 
	
	COMMIT transaction StampMeter_1
 end try
 begin catch 
	rollback transaction StampMeter_1
 end catch

print ('StampMeter_1')

 --снова обновляем
update @res 
set 
Meter_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.Meter_StampNumber,'') like isnull(Info_Stamps.StampNumber,'=')  and isnull(Info_Stamps.InstallationComment,'') like  'ПУ'
where 
res.AllowLoad=1 
and res.Meter_StampID is null
and res.Meter_ID is not null
and isnull(res.Meter_StampNumber,'-')<>'-' 		
and isnull(res.Meter_StampNumber,'')<>'' 


--ТТА
update @res 
set 
TTA_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.TTA_StampNumber,'') like isnull(Info_Stamps.StampNumber,'')  and isnull(Info_Stamps.InstallationComment,'') like 'ТТ - фаза A'
where 
res.AllowLoad=1 
and res.TTA_StampID is null
and res.TTA_ID is not null
and isnull(res.TTA_StampNumber,'-')<>'-'  
and isnull(res.TTA_StampNumber,'')<>'' 	

Begin transaction StampTTA_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(Stamp_ID)
	from Info_Stamps

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps(
	Stamp_ID, StampNumber,InstallationDate , InstallationComment, CreateDateTime)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY StampNumber ASC) AS  Stamp_ID,
		 StampNumber,InstallationDate , 'ТТ - фаза A', @getdate	 
	from 
	(
		select distinct 
			StampNumber=TTA_StampNumber, InstallationDate=isnull(TTA_CalibrationDate,'2001-01-01')
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TTA_StampID is null
		and res.TTA_ID is not null
		and isnull(res.TTA_StampNumber,'')<>'-'  
and isnull(res.TTA_StampNumber,'')<>'' 			
	)
	as temp 
	
	COMMIT transaction StampTTA_1
 end try
 begin catch 
	rollback transaction StampTTA_1
 end catch

 --снова обновляем
update @res 
set 
TTA_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.TTA_StampNumber,'') like isnull(Info_Stamps.StampNumber,'')  and isnull(Info_Stamps.InstallationComment,'') like 'ТТ - фаза A'
where 
res.AllowLoad=1 
and res.TTA_StampID is null
and res.TTA_ID is not null
and isnull(res.TTA_StampNumber,'-')<>'-' 	 
and isnull(res.TTA_StampNumber,'')<>'' 	


--ТТB
update @res 
set 
TTB_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.TTB_StampNumber,'') like isnull(Info_Stamps.StampNumber,'')  and isnull(Info_Stamps.InstallationComment,'') like 'ТТ - фаза B'
where 
res.AllowLoad=1 
and res.TTB_StampID is null
and res.TTB_ID is not null
and isnull(res.TTB_StampNumber,'-')<>'-' 	
and isnull(res.TTB_StampNumber,'')<>'' 
	
 

Begin transaction StampTTB_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(Stamp_ID)
	from Info_Stamps

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps(
	Stamp_ID, StampNumber,InstallationDate , InstallationComment, CreateDateTime)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY StampNumber ASC) AS  Stamp_ID,
		 StampNumber,InstallationDate , 'ТТ - фаза B', @getdate	 
	from 
	(
		select distinct 
			StampNumber=TTB_StampNumber, InstallationDate=isnull(TTB_CalibrationDate,'2001-01-01')	
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TTB_StampID is null
		and res.TTB_ID is not null
		and isnull(res.TTB_StampNumber,'')<>'-' 	
and isnull(res.TTB_StampNumber,'')<>'' 		
	)
	as temp 
	
	COMMIT transaction StampTTB_1
 end try
 begin catch 
	rollback transaction StampTTB_1
 end catch

 --снова обновляем
update @res 
set 
TTB_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.TTB_StampNumber,'') like isnull(Info_Stamps.StampNumber,'')  and isnull(Info_Stamps.InstallationComment,'') like 'ТТ - фаза B'
where 
res.AllowLoad=1 
and res.TTB_StampID is null
and res.TTB_ID is not null
and isnull(res.TTB_StampNumber,'-')<>'-' 		
and isnull(res.TTB_StampNumber,'')<>'' 
	
--ТТC
update @res 
set 
TTC_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.TTC_StampNumber,'') like isnull(Info_Stamps.StampNumber,'')  and isnull(Info_Stamps.InstallationComment,'') like 'ТТ - фаза C'
where 
res.AllowLoad=1 
and res.TTC_StampID is null
and res.TTC_ID is not null
and isnull(res.TTC_StampNumber,'-')<>'-'
and isnull(res.TTC_StampNumber,'')<>'' 

Begin transaction StampTTC_1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(Stamp_ID)
	from Info_Stamps

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps(
	Stamp_ID, StampNumber,InstallationDate , InstallationComment, CreateDateTime)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY StampNumber ASC) AS  Stamp_ID,
		 StampNumber,InstallationDate , 'ТТ - фаза C', @getdate	 
	from 
	(
		select distinct 
			StampNumber=TTC_StampNumber, InstallationDate=isnull(TTC_CalibrationDate,'2001-01-01')	
		from @res res		 
		where
		res.AllowLoad=1 
		and res.TTC_StampID is null
		and res.TTC_ID is not null
		and isnull(res.TTC_StampNumber,'')<>'-' 
and isnull(res.TTC_StampNumber,'')<>'' 		
	)
	as temp 
	
	COMMIT transaction StampTTC_1
 end try
 begin catch 
	rollback transaction StampTTC_1
 end catch

 --снова обновляем
update @res 
set 
TTC_StampID= Info_Stamps.Stamp_ID 
from @res res join Info_Stamps on isnull(res.TTC_StampNumber,'') like isnull(Info_Stamps.StampNumber,'')  and isnull(Info_Stamps.InstallationComment,'') like 'ТТ - фаза C'
where 
res.AllowLoad=1 
and res.TTC_StampID is null
and res.TTC_ID is not null
and isnull(res.TTC_StampNumber,'-')<>'-' 	
and isnull(res.TTC_StampNumber,'')<>'' 



 


--на ТН пломб нет



 
print ('StampsToDevice ')

Begin transaction StampsToDevice_ID1 
begin try
	--находим последний максимальный ИД
	select @maxID= max(StampsToDevice_ID)
	from Info_Stamps_To_Device

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps_To_Device(
	StampsToDevice_ID, Stamp_ID,Meter_ID)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY Stamp_ID ASC) AS  StampsToDevice_ID,Stamp_ID,Meter_ID
	from 
	(
		select distinct 
			Stamp_ID=Meter_StampID,Meter_ID	
		from @res res		 
		where
		res.AllowLoad=1  
		and res.Meter_ID is not null 
		and res.Meter_StampID is not null 	
		and not exists (select top 1 1  from Info_Stamps_To_Device b where b.Stamp_ID= res.Meter_StampID and b.Meter_ID=res.Meter_ID)	
	)
	as temp 
	
	COMMIT transaction StampsToDevice_ID1
 end try
 begin catch 
	rollback transaction StampsToDevice_ID1
 end catch


 Begin transaction StampsToDevice_ID2 
begin try
	--находим последний максимальный ИД
	select @maxID= max(StampsToDevice_ID)
	from Info_Stamps_To_Device

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps_To_Device(
	StampsToDevice_ID, Stamp_ID,TTA_ID)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY Stamp_ID ASC) AS  StampsToDevice_ID,Stamp_ID,TT_ID
	from 
	(
		select distinct 
			Stamp_ID=TTA_StampID,TT_ID=TTA_ID	
		from @res res		 
		where
		res.AllowLoad=1  
		and res.TTA_ID is not null 
		and res.TTA_StampID is not null 		
		and not exists (select top 1 1  from Info_Stamps_To_Device b where b.Stamp_ID= res.TTA_StampID and b.TTA_ID=res.TTA_ID)	
	)
	as temp 


	select @maxID= max(StampsToDevice_ID)
	from Info_Stamps_To_Device

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps_To_Device(
	StampsToDevice_ID, Stamp_ID,TTB_ID)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY Stamp_ID ASC) AS  StampsToDevice_ID,Stamp_ID,TT_ID
	from 
	(
		select distinct 
			Stamp_ID=TTB_StampID,TT_ID=TTB_ID	
		from @res res		 
		where
		res.AllowLoad=1  
		and res.TTB_ID is not null 
		and res.TTB_StampID is not null 		
		and not exists (select top 1 1  from Info_Stamps_To_Device b where b.Stamp_ID= res.TTB_StampID and b.TTB_ID=res.TTB_ID)	
	)
	as temp 
	
	select @maxID= max(StampsToDevice_ID)
	from Info_Stamps_To_Device

	set @maxID= isnull(@maxID,0)
		 
	--вставляем отсутствующие
	insert into Info_Stamps_To_Device(
	StampsToDevice_ID, Stamp_ID,TTC_ID)	 

	select distinct 
		@maxID+ROW_NUMBER() OVER(ORDER BY Stamp_ID ASC) AS  StampsToDevice_ID,Stamp_ID,TT_ID
	from 
	(
		select distinct 
			Stamp_ID=TTC_StampID,TT_ID=TTC_ID	
		from @res res		 
		where
		res.AllowLoad=1  
		and res.TTC_ID is not null 
		and res.TTC_StampID is not null 		
		and not exists (select top 1 1  from Info_Stamps_To_Device b where b.Stamp_ID= res.TTC_StampID and b.TTC_ID=res.TTC_ID)		
	)
	as temp 

	COMMIT transaction StampsToDevice_ID2
 end try
 begin catch 
	rollback transaction StampsToDevice_ID2
 end catch



update Hard_Meters
set CalibrationDate= res.Meter_CalibrationDate, InstallationPlace= res.Meter_Place
from 
@res res join Hard_Meters on res.Meter_ID= Hard_Meters.Meter_ID
where 
res.AllowLoad=1 
and res.Meter_ID is not null
and res.Meter_CalibrationDate is not null

 

 
--удаляем все привязки к ТИ и создаем новую с 2000 года????
 
 
 update @res set TNCoef=1 where TNCoef is not null and TNCoef<1
 update @res set TNCoef1=1 where TNCoef1 is not null and TNCoef1<1
 update @res set TNCoef2=1 where TNCoef2 is not null and TNCoef2<1
 update @res set TTCoef=1 where TTCoef is not null and TTCoef<1
 update @res set TTCoef1=1 where TTCoef1 is not null and TTCoef1<1
 update @res set TTCoef2=1 where TTCoef2 is not null and TTCoef2<1

 --ИМпорт трансформаторов считается более корректным, поэтому удаляем все привязки и добваляем 1 раз (т.к. не указаны даты!!!)
 delete from 
 Info_Transformators
 where TI_ID in (select res.TI_ID from @res res where res.TI_ID is not null  and (res.TTA_ID is not null or res. TNA_ID is not null))

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
BusSystem_ID,
TNDesignation_ID,
TTA_ID,
TTB_ID,
TTC_ID,
CUS_ID,
UseBusSystem)

select distinct 
TI_ID,
'2001-01-01',
'2100-01-01',
isnull(TNCoef,1),
convert(int,isnull(TNCoef1,1)),
convert(int,isnull(TNCoef2,1)),
isnull(TTCoef,1),
convert(int,isnull(TTCoef1,1)),
convert(int,isnull(TTCoef2,1)),
BusSystem_ID,
TNDesignation_ID,
TTA_ID,
TTB_ID,
TTC_ID,
0,
1
from 
@res   
where 
TI_ID is not null 
and AllowLoad=1
and (TTA_ID is not null or TNA_ID is not null)



Update @res 
set 
ResultMessage='' where ResultMessage is null

print ('проверка ошибок')

Update @res 
set 
ResultMessage=ResultMessage+' не удалось найти ПУ; '
where 
isnull(Meter_SerialNumber ,'')<>'' 
and
isnull(Meter_SerialNumber ,'-')<>'-' 
and Meter_ID is null

Update @res 
set 
ResultMessage=ResultMessage+' не удалось добавить пломбу ПУ; '
where 
isnull(Meter_StampNumber ,'')<>'' 
and isnull(Meter_StampNumber ,'-')<>'-'
and Meter_StampID is null


Update @res 
set 
ResultMessage=ResultMessage+' не удалось найти ТТ; '
where 
(isnull(TTA_SerialNumber ,'-')<>'-' 
and isnull(TTA_SerialNumber ,'')<>'' 
and TTA_ID is null)
or
(isnull(TTB_SerialNumber ,'-')<>'-' 
and isnull(TTB_SerialNumber ,'')<>'' 
and TTB_ID is null)
or
(isnull(TTC_SerialNumber ,'-')<>'-' 
and isnull(TTC_SerialNumber ,'')<>'' 
and TTC_ID is null)


Update @res 
set 
ResultMessage=ResultMessage+' не удалось найти ТН; '
where 
(isnull(TNA_SerialNumber ,'-')<>'-' 
and isnull(TNA_SerialNumber ,'')<>'' 
and TNA_ID is null)
or
(isnull(TNB_SerialNumber ,'-')<>'-' 
and isnull(TNB_SerialNumber ,'')<>'' 
and TNB_ID is null)
or
(isnull(TNC_SerialNumber ,'-')<>'-' 
and isnull(TNC_SerialNumber ,'')<>'' 
and TNC_ID is null)


Update @res 
set 
ResultMessage=ResultMessage+' не удалось добавить пломбу ТТ; '
where 
(isnull(TTA_StampNumber ,'-')<>'-' and isnull(TTA_StampNumber ,'')<>'' and TTA_StampID is null)
or
(isnull(TTB_StampNumber ,'-')<>'-' and isnull(TTB_StampNumber ,'')<>'' and TTB_StampID is null)
or
(isnull(TTC_StampNumber ,'-')<>'-' and isnull(TTC_StampNumber ,'')<>'' and TTC_StampID is null)




--Удаляем СШ и тп без привязок, которые создавались автоматически (примерно)
delete from Dict_TNDesignation
where TNDesignation_ID not in (select TNDesignation_ID from Info_Meters_TO_TI )
and StringName like 'ТН -%[0-9]%'


delete from Dict_BusSystem
where 
BusSystem_ID not in (select BusSystem_ID from Dict_TNDesignation )
and  BusSystem_ID not in (select BusSystem_ID from Info_Transformators )
and StringName like 'СШ%'


delete from Dict_DistributingArrangement
where 
DistributingArrangement_ID not in (select DistributingArrangement_ID from Dict_BusSystem )
and StringName like 'РУ%'



 select distinct 
SheetNumber,
SheetName='Лист '+Convert(varchar,SheetNumber), 
RowID=res.RowNumber,
Message='',
ErrorMessage= res.ResultMessage
from @res res
where 
(AllowLoad=0 or isnull(ResultMessage,'') <>'')
 


end
GO

grant EXECUTE on usp2_Import_NSI_Transformators to UserCalcService
go
grant EXECUTE on usp2_Import_NSI_Transformators to UserDeclarator
go
 




--declare @items Import_NSI_Transformators_TableType
--insert into @items 
--select * from TEMP_Import_NSI_Transformators
--where TNA_Model <>''


--exec usp2_Import_NSI_Transformators @items, '80004Q4WZ1350KO8NT59RM'