if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ImportNSI_Report_OEK_Integrals')
          and type in ('P','PC'))
   drop procedure usp2_ImportNSI_Report_OEK_Integrals
go

if exists(select 1 from sysobjects where name='usp2_ImportNSI_Report_OEK_IntegralsData')
   drop procedure usp2_ImportNSI_Report_OEK_IntegralsData
go

 

if exists(select 1 from systypes where name='ImportNSI_Report_OEK_Integrals')
   drop type ImportNSI_Report_OEK_Integrals
go


CREATE TYPE [dbo].[ImportNSI_Report_OEK_Integrals] AS TABLE(
	
	RowID [int] NOT NULL,
	
	SheetName [nvarchar](400),
	
	SheetNumber int,
	
	SheetID int,
	
	JuridicalPersonName [nvarchar](400),	
	
	INN [nvarchar](400),
	
	JuridicalPerson_ID int,
	
	TariffName [nvarchar](400), 
	ChannelType int,

	HierLev1Name [nvarchar](400),
	HierLev1_ID int, 
	HierLev2Name [nvarchar](400),
	HierLev2_ID int, 
	HierLev3Name [nvarchar](400),
	HierLev3AddName [nvarchar](400),
	HierLev3_ID int, 
	PSName [nvarchar](400),
	PSAddName [nvarchar](400), 
	PSType [nvarchar](100), 
	PS_ID int, 
	
	TIName [nvarchar](400)  , 
	TIEPPCode [nvarchar](400)  ,
	TIATSCode [nvarchar](400)  ,
	TIType int ,
	TI_ID [int]  ,

	Model [nvarchar](400)  ,
	
	Manufacturer [nvarchar](400)  ,
	SerialNumber [nvarchar](400)  ,	
	Meter_ID [int] ,
	MeterModel_ID int,
	MeterType_ID int,
	
	Address[nvarchar](1000)  ,	
	Coeff float ,

	IntegralValue_Previous float ,
	IntegralValue_Current float ,	 

	Message nvarchar(1000),
	ResultCode bigint,
	
	FIASAddress nvarchar(1000)  ,	
	FIASCode nvarchar(100) ,
	AbonentCode nvarchar(100)  ,
	TRCoeff float  ,		
		
	MeterType nvarchar(200),
	IntegralValue_Expense float ,--расход
	IntegralValue_Losses float ,--потери
	IntegralValue_CommonExpense float , --общий расход
	ImportNumber uniqueidentifier,
	EventDate datetime,
	
	User_ID nvarchar(200),
	DispatchDateTime datetime,

	AllowLoad bit, --разрешить загрузку
	ErrorMessage nvarchar(1000),

	--для импорта своб иерархии
	SOName nvarchar(400),
	PESName nvarchar(400),
	VoltageLevel nvarchar(255),
	PriceCategory nvarchar(255),
	TariffFullName nvarchar(255),

	PRIMARY KEY (ImportNumber, SheetNumber,RowID )
	)
go 

grant execute on type::dbo.[ImportNSI_Report_OEK_Integrals] to UserCalcService
go
grant execute on type::dbo.[ImportNSI_Report_OEK_Integrals] to UserDeclarator
go
grant execute on type::dbo.[ImportNSI_Report_OEK_Integrals] to UserImportService
go
grant execute on type::dbo.[ImportNSI_Report_OEK_Integrals] to UserExportService
go



create procedure  usp2_ImportNSI_Report_OEK_IntegralsData
 @SourceTable [ImportNSI_Report_OEK_Integrals] READONLY,
 @UsedColumns [dbo].[StringType] readonly,
 @AutoReplace bit,
 @UserID nvarchar(200),
 @EventDateTime datetime,
 @DataSourceType int,
 @AllowLoadAddress bit = 0,
 @AllowLoadTRCoeff bit =0, 
 @LastImportNumber uniqueidentifier=null

AS
BEGIN

--!!!
--ИМОПРТИРУЕТСЯ ПО 2 ТЫС.. но обновления то потом по @LastImportNumber - тоесть каждый раз снова всю часть таблицы (текущий импорт)
--НАДО ИЛИ РАЗДЕЛИТЬ ИМПОРТ в таблицу и импорт.. или еще передавать номера первой/последней строки в текущем импорте..!!
 

declare @ObjectName nvarchar(200)='ImportNSI_Report_OEK_Integrals'


--@SourceTable - класс с импортируемыми данными
--@UsedColumns - списко используемых полей этого класса которые импортируются 
--не все могут накидать в конфигурацию, соотв они будут пустыми
--@AutoReplace - использовать автозамену в исходных данных в соответствии с таблицами замен
--в таблицах замен хранится название объекта, название поля, и старое/новое значение

set dateformat dmy
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted



declare @msg nvarchar(400)='';
set @msg=convert(varchar, getdate(),121)+ ' Начало' 
--print @msg 

insert into ImportNSI_SourceData_Report18Jur
(
RowID ,
SheetName,	
SheetNumber ,	
SheetID ,	
JuridicalPersonName ,	
INN ,	
JuridicalPerson_ID ,	
TariffName , 
ChannelType,

HierLev1Name ,
HierLev1_ID , 
HierLev2Name ,
HierLev2_ID , 
HierLev3Name ,
HierLev3AddName ,
HierLev3_ID , 
PSName ,
PSAddName ,
PSType ,
PS_ID , 
TIName   , 
TIEPPCode   ,
TIATSCode   ,
TIType  ,
TI_ID   ,
Model   ,
Manufacturer,
SerialNumber,	
Meter_ID  ,
MeterModel_ID ,
MeterType_ID ,
Address,
Coeff ,
IntegralValue_Previous ,
IntegralValue_Current ,
Message ,
ResultCode  ,

FIASAddress   ,	
FIASCode  ,
AbonentCode ,
TRCoeff ,

MeterType ,  --18
IntegralValue_Expense ,--24
IntegralValue_Losses ,--25
IntegralValue_CommonExpense ,--30
ImportNumber ,
EventDate ,

User_ID ,
DispatchDateTime ,
AllowLoad,
ErrorMessage,

SOName ,
PESName ,
VoltageLevel,
PriceCategory ,
TariffFullName 
)
select RowID ,
SheetName,	
SheetNumber ,	
SheetID ,	
JuridicalPersonName ,	
INN ,	
JuridicalPerson_ID ,	
TariffName , 
ChannelType,
HierLev1Name ,
HierLev1_ID , 
HierLev2Name ,
HierLev2_ID , 
HierLev3Name ,
HierLev3AddName ,
HierLev3_ID , 
PSName ,
PSAddName ,
PSType ,
PS_ID , 
TIName   , 
TIEPPCode   ,
TIATSCode   ,
TIType  ,
TI_ID   ,
Model   ,
Manufacturer,
SerialNumber,	
Meter_ID  ,
MeterModel_ID ,
MeterType_ID ,
Address,
Coeff ,
IntegralValue_Previous ,
IntegralValue_Current ,
Message ,
ResultCode ,

FIASAddress   ,	
FIASCode  ,
AbonentCode ,
TRCoeff  ,

MeterType ,
IntegralValue_Expense ,
IntegralValue_Losses ,
IntegralValue_CommonExpense ,
@LastImportNumber ,
@EventDateTime ,

@UserID ,
getdate() ,
AllowLoad=AllowLoad,
ErrorMessage=ErrorMessage,

SOName ,
PESName ,
VoltageLevel,
PriceCategory ,
TariffFullName 
from @SourceTable
  

set @msg=convert(varchar, getdate(),121)+ ' Добавили данные в БД' 
--print @msg


update ImportNSI_SourceData_Report18Jur 
set 
SheetName=  case when  isnull(SheetName,'')= '' then 'нет данных' else SheetName end  ,
JuridicalPersonName=  case when  isnull(JuridicalPersonName,'')= '' then 'нет данных' else JuridicalPersonName end  ,
PSName= case when  isnull(PSName,'')= '' then 'нет данных' else PSName end,
Message ='', 
ResultCode=0,
Manufacturer=isnull(Manufacturer,''), 
Model= isnull(model,'') ,
TIType= 15 ,
TRCoeff=  case when  isnull(TRCoeff,0)=0 then null else TRCoeff end,
TIName =
case
 when isnull(TIName,'') =  '' and isnull(TIEPPCode,'') <>'' then  isnull(TIEPPCode,'') 
 when isnull(TIName,'') =  '' and isnull(TIEPPCode,'') ='' and isnull(SerialNumber,'') <>''  then isnull(SerialNumber,'') 
 else TIName end,
 --(?) если все таки дошло  в экспоненциальной форме то преобразовываем так (конверторы криво приводят с потерей точности... а это чревато)
 TIEPPCode = 
 case when isnull(TIEPPCode,'')='' then '№'+SerialNumber 
	  else  replace (isnull(TIEPPCode,''),',','.') end
 
where  
ImportNumber= @LastImportNumber 
 

--просто берем все знаки без "степени.. E+"    
update ImportNSI_SourceData_Report18Jur 
set TIEPPCode=
replace(
left(substring(TIEPPCode,0,CHARINDEX('E',TIEPPCode))
+'000000000000000000000000000', 
CHARINDEX('.',TIEPPCode)+convert(int,replace(substring(TIEPPCode,CHARINDEX('E',TIEPPCode),len(TIEPPCode)),'E+',''))
),
'.',''
)
where   
ImportNumber= @LastImportNumber 
and TIEPPCode is not null
and TIEPPCode like '%E%'



set @msg=convert(varchar, getdate(),121)+ ' Загрузили данные в таблицу' 
--print @msg

  
end

go


grant execute on usp2_ImportNSI_Report_OEK_IntegralsData to UserCalcService
go
grant execute on usp2_ImportNSI_Report_OEK_IntegralsData to UserDeclarator
go
grant execute on usp2_ImportNSI_Report_OEK_IntegralsData to UserImportService
go
grant execute on usp2_ImportNSI_Report_OEK_IntegralsData to UserExportService
go
  


if exists(select 1 from sysobjects where name='usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy')
   drop procedure usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy
go

create procedure  usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy
 @LastImportNumber uniqueidentifier=null

AS
BEGIN
 

set dateformat dmy
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted



--название тарифа (так скзали определяется..)
update ImportNSI_SourceData_Report18Jur
set TariffFullName = 
	case when isnull(priceCategory,'') in ('4','5','6') then '2-ставочный тариф' 
	else '1-ставочный тариф' end
where 
ImportNumber=@LastImportNumber
and AllowLoad=1

--на всякий находим ИД ТИ по коду
update ImportNSI_SourceData_Report18Jur
set TI_ID= info_TI.TI_ID
from ImportNSI_SourceData_Report18Jur 
	join info_TI on 
	ImportNSI_SourceData_Report18Jur.TIEPPCode = info_TI.EPP_ID
where 
ImportNumber=@LastImportNumber
and AllowLoad=1
and ImportNSI_SourceData_Report18Jur.TI_ID is null 
and info_TI.EPP_ID is not null
and info_TI.EPP_ID<>'' 


 
--выход если не ТСО
if (not exists (select top 1 1 from ImportNSI_SourceData_Report18Jur
 where 
 ImportNumber=@LastImportNumber
	and AllowLoad=1
	and  SONAme like  '%Территориальная%сетевая%организация%'))
	return;



create table #tempAll (
SOTreeName nvarchar(400),
Tree_ID int,
SOName nvarchar(400),
SOFreeHierItem_ID int,
HierLev2_ID int, --это СО
PESName nvarchar(400), 
PESFreeHierItem_ID int, --просто узел
JuridicalPersonName nvarchar(400),
JuridicalPerson_ID int, 
HierLev3_ID int, -- это ЮЛ
JuridicalPersonFreeHierItem_ID int,
VoltageLevel  nvarchar(255),--просто узел
VoltageLevelFreeHierItem_ID int,
PriceCategory  nvarchar(255),  
PriceCategoryFreeHierItem_ID int,
TariffFullName  nvarchar(255),--просто узел
TariffFreeHierItem_ID int,
TIName nvarchar(400),
TI_ID int,
TIFreeHierItem_ID int
)
insert into #tempAll
(
SOTreeName,
SOName,
PESName, 
JuridicalPersonName, 
JuridicalPerson_ID, 
VoltageLevel,
PriceCategory, 
TariffFullName,
TIName,
TI_ID
)
select distinct 
'Территориальная сетевая организация', --SOName не используем - должно быть одно дерево
SheetName,
PESName, 
JuridicalPersonName, 
JuridicalPerson_ID, 
VoltageLevel,
PriceCategory, 
TariffFullName,
TIName,
TI_ID

from  ImportNSI_SourceData_Report18Jur
where 
ImportNumber=@LastImportNumber
and AllowLoad=1
and len(isnull(SOName,''))>=3
and TI_ID is not null

order by
PESName, 
JuridicalPersonName, 
VoltageLevel,
PriceCategory, 
TIName

 
--выход если нет данных для обработки 
if (not exists (select top 1 1 from #tempAll))
	return;




 
DECLARE @ParentHierID   hierarchyid, @LastNode hierarchyid



---------------------------------------------------------------------------------
--СОЗДАЕМ ДЕРЕВО ПО столбцу  2 (территорисальная сетевая организация)
---------------------------------------------------------------------------------

create table #soTreeTemp
(
SOTreeName nvarchar(400),
Tree_ID int
)
insert into #soTreeTemp
select distinct SOTreeName, Tree_ID
from #tempAll

--пробуем найти по названию (строго на втором уровне GetLevel()=1)
update #soTreeTemp
set Tree_ID = Dict_FreeHierarchyTypes.FreeHierTree_ID
from #soTreeTemp temp join Dict_FreeHierarchyTypes 
		on temp.SOTreeName= Dict_FreeHierarchyTypes.StringName 
		and Dict_FreeHierarchyTypes.HierID.GetLevel()=1



--теперь добавляем не найденные
declare @maxTreeID int =-1
BEGIN TRAN
begin try
		
		SET @ParentHierID = hierarchyid ::GetRoot()

		--находим максимальный ИД						
		select @maxTreeID=max(FreeHierTree_ID) from Dict_FreeHierarchyTypes
		select @maxTreeID= isnull(@maxTreeID,0)

		--находим последний узел
		SELECT @LastNode = max(HierID)
		FROM Dict_FreeHierarchyTypes
		WHERE 
		(Dict_FreeHierarchyTypes.HierID.GetAncestor(1) =@ParentHierID)
								
		--добваляем узлы после последнего					
		insert into Dict_FreeHierarchyTypes  (HierID,FreeHierTree_ID,StringName, ModuleFilter) 			
		select distinct
		@ParentHierID.ToString()+
		Convert(varchar,
				isnull(substring(@LastNode.ToString(),		
				--берем цифру из последнего бочернего					
				len(@LastNode.ToString()) - CHARINDEX('/',reverse(substring(@LastNode.ToString(),1,len(@LastNode.ToString())-1)))+1,
						CHARINDEX('/',reverse(substring(@LastNode.ToString(),1,len(@LastNode.ToString())-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		ID=@maxTreeID+Number, 
		SOTreeName	, 0			
		from  (select Number= ROW_NUMBER() OVER(ORDER BY aa.SOTreeName ASC),
				aa.SOTreeName
				from 
				(
					select distinct SOTreeName
					from #soTreeTemp 
					where Tree_ID is null					
				) as aa
		) as res
		
end try
begin catch
			
	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  

--обновляем идентификаторы... (ищем строго на втором уровне GetLevel()=1)
update #tempAll
set Tree_ID = Dict_FreeHierarchyTypes.FreeHierTree_ID
from 
#tempAll temp
join Dict_FreeHierarchyTypes 
		on temp.SOTreeName= Dict_FreeHierarchyTypes.StringName 
		and Dict_FreeHierarchyTypes.HierID.GetLevel()=1

drop table #soTreeTemp
 


---------------------------------------------------------------------------------
--2) создаем корневые узлы в соотв деревьях (СО = HierLev2)
---------------------------------------------------------------------------------

declare @tempFreeNodes table (Tree_ID int, HierID hierarchyid, StringName nvarchar(400), ID int, EnumFreeHierarchyItemType int,  ObjectInt_ID int)

SET @ParentHierID =null
SET @LastNode=null


create table #soTreeItemRootTemp
(
Tree_ID int,
SOName nvarchar(400),
SOFreeHierItem_ID int
)
insert into #soTreeItemRootTemp
select distinct  ta.Tree_ID,ta.SOName, null
from #tempAll ta 
where 
ta.Tree_ID is not null
and len(isnull(ta.SOName,''))>=3


--ищем корневые (GetLevel()=1) узлы с названиями равными СО (название листа) в соответствующих деревьях
update #soTreeItemRootTemp
set SOFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID
from #soTreeItemRootTemp temp
 join Dict_FreeHierarchyTree 
		on temp.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp.SOName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=1		
where
temp.Tree_ID is not null
and len(isnull(temp.SOName,''))>=3
 


--теперь добавляем не найденные
declare @maxFreeItemID int =-1
BEGIN TRAN
begin try		
		--находим максимальный ИД						
		select @maxFreeItemID=max(FreeHierItem_ID) from Dict_FreeHierarchyTree 
		select @maxFreeItemID= isnull(@maxFreeItemID,0)

	 

		 
		delete from @tempFreeNodes 							
		--добавляем узлы после последнего					
		insert into @tempFreeNodes  (Tree_ID, HierID,ID,StringName, EnumFreeHierarchyItemType,ObjectInt_ID) 			
		select distinct
		Tree_ID,
		'/'+Convert(varchar,
				isnull(substring(lastchild.ChildHierID.ToString(),		
				--берем цифру из последнего бочернего					
				len(lastchild.ChildHierID.ToString()) - CHARINDEX('/',reverse(substring(lastchild.ChildHierID.ToString(),1,len(lastchild.ChildHierID.ToString())-1)))+1,
						CHARINDEX('/',reverse(substring(lastchild.ChildHierID.ToString(),1,len(lastchild.ChildHierID.ToString())-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		
		SOFreeHierItem_ID=@maxFreeItemID+Number, 
		SOName	,
		
		EnumFreeHierarchyItemType= case when h2.HierLev2_ID is null then 0 else 2 end,
		ObjectInt_ID = h2.HierLev2_ID
				
		from  (select Tree_ID, Number= ROW_NUMBER() OVER(ORDER BY aa.SOName ASC),
				aa.SOName
				from 
				(
					select distinct Tree_ID,SOName
					from #soTreeItemRootTemp 
					where Tree_ID is not null and SOFreeHierItem_ID is null					
				) as aa
		) as res
		--находим СО в дереве 18_ЮЛ
		--1й уровень - 18ЮЛ, 2й уровень название СО
		outer apply (select top 1 HierLev2_ID from vw_Dict_HierarchyPS where StringName like '18%ЮЛ' and HierLev2StringName like res.SOName) h2

		
		--выбираем максимальный HierID, либо ставим 0, затем выше номер распарсится и подставится +1
		outer apply (  select top 1 ChildHierID=  cast(isnull( cast(max(HierID)as hierarchyid) .ToString(),'/0/')as hierarchyid)
							FROM Dict_FreeHierarchyTree
							WHERE 
							FreeHierTree_ID= res.Tree_ID
							and HierID.GetLevel()=1)   as lastchild
								 
								  


		--теперь добавляем в обе таблицы
		insert into Dict_FreeHierarchyTree
		(FreeHierTree_ID,	HierID,		FreeHierItem_ID,	StringName,	FreeHierItemType,	Expanded,	FreeHierIcon_ID,	SortNumber)
		select distinct Tree_ID, HierID,ID, StringName, EnumFreeHierarchyItemType, 0, null, null  
		from @tempFreeNodes

		insert into Dict_FreeHierarchyTree_Description
		(FreeHierItem_ID,IncludeObjectChildren,HierLev2_ID)
		select distinct ID, 0, ObjectInt_ID from @tempFreeNodes
		
end try
begin catch
			print ERROR_MESSAGE() 
	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  


--обновляем идентификаторы... (ищем строго на втором уровне GetLevel()=1)
update #tempAll
set 
SOFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID,
HierLev2_ID=Dict_FreeHierarchyTree_Description.HierLev2_ID
from 
--select * from 
#tempAll temp
 join Dict_FreeHierarchyTree 
		on temp.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp.SOName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=1 
left join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID
where
temp.Tree_ID is not null
and len(isnull(temp.SOName,''))>=3





drop table #soTreeItemRootTemp



---------------------------------------------------------------------------------
--3) создаем (2й уровень в дереве - просто узел) - PESNode
---------------------------------------------------------------------------------

SET @ParentHierID =null
SET @LastNode=null


create table #PES_TreeItemLvl2
(
Tree_ID int,
SOFreeHierItem_ID int, 
SOHierID hierarchyid,
PESName nvarchar(400),
PESFreeHierItem_ID int
)
insert into #PES_TreeItemLvl2
select distinct  
	ta.Tree_ID,
	ta.SOFreeHierItem_ID, 
	HierID, 
	ta.PESName, 
	null
from #tempAll ta 
	join Dict_FreeHierarchyTree on  FreeHierItem_ID=ta.SOFreeHierItem_ID
where 
ta.Tree_ID is not null
and ta.SOFreeHierItem_ID is not null
and len(isnull(ta.PESName,''))>=3



--ищем корневые (GetLevel()=1) узлы с названиями равными СО (название листа) в соответствующих деревьях
update #PES_TreeItemLvl2
set PESFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID
from #PES_TreeItemLvl2 temp
 join Dict_FreeHierarchyTree 
		on temp.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp.PESName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=2 --следующий уровень
			and HierID.GetAncestor (1) = temp.SOHierID 		--и предок равен HierID (первого уровня - SO)
where
temp.Tree_ID is not null
and temp.SOFreeHierItem_ID is not null
and len(isnull(temp.PESName,''))>=3
 
  

--теперь добавляем не найденные
set @maxFreeItemID =-1

BEGIN TRAN
begin try		
		--находим максимальный ИД						
		select @maxFreeItemID=max(FreeHierItem_ID) from Dict_FreeHierarchyTree 
		select @maxFreeItemID= isnull(@maxFreeItemID,0)

		delete from @tempFreeNodes						
		--добавляем узлы после последнего					
		insert into @tempFreeNodes  (Tree_ID, HierID,ID,StringName, EnumFreeHierarchyItemType,ObjectInt_ID) 			
		select distinct
		Tree_ID,
		res.SOHierID.ToString()+
		Convert(varchar,
				isnull(substring(lastchild.ChildHierID,		
				--берем цифру из последнего дочернего					
				len(lastchild.ChildHierID) - CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))+1,
						CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		PESFreeHierItem_ID=@maxFreeItemID+Number, 
		PESName	,
		EnumFreeHierarchyItemType=0,
		null
				
		from  (select Tree_ID,
					SOHierID, 
					Number= ROW_NUMBER() OVER(ORDER BY aa.PESName ASC),
					aa.PESName
				from 
				(
					select distinct Tree_ID,SOFreeHierItem_ID, SOHierID, PESName
					from #PES_TreeItemLvl2 
					where Tree_ID is not null 
							and SOFreeHierItem_ID is not null	
							and PESFreeHierItem_ID is null				
				) as aa
			  ) as res
			  --выбираем максимальный дочерний HierID, либо ставим 0, затем выше номер распарсится и подставится +1
			outer apply (  select top 1 ChildHierID=  isnull( cast(max(HierID) as hierarchyid).ToString(),res.SOHierID.ToString()+'0/')
								FROM Dict_FreeHierarchyTree
								WHERE 
								FreeHierTree_ID= res.Tree_ID
								and HierID.GetAncestor (1) = res.SOHierID)   as lastchild
								 


		 

		--теперь добавляем в обе таблицы
		insert into Dict_FreeHierarchyTree
		(FreeHierTree_ID,	HierID,		FreeHierItem_ID,	StringName,	FreeHierItemType,	Expanded,	FreeHierIcon_ID,	SortNumber)
		select distinct Tree_ID, HierID,ID, StringName, EnumFreeHierarchyItemType, 0, null, null  
		from @tempFreeNodes

		insert into Dict_FreeHierarchyTree_Description
		(FreeHierItem_ID,IncludeObjectChildren)
		select distinct ID, 0 from @tempFreeNodes
		
		
end try
begin catch
			
	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  



--обновляем идентификаторы... (ищем строго на втором уровне GetLevel()=2) 


update #tempAll
set PESFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID
from #tempAll temp
join #PES_TreeItemLvl2 temp2 
	on temp.Tree_ID= temp2.Tree_ID and temp.SOFreeHierItem_ID= temp2.SOFreeHierItem_ID
join Dict_FreeHierarchyTree 
		on temp2.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp2.PESName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=2 --следующий уровень
			and HierID.GetAncestor (1) = temp2.SOHierID 		--и предок равен HierID (первого уровня - SO)
where
temp.Tree_ID is not null
and temp.SOFreeHierItem_ID is not null
and len(isnull(temp.PESName,''))>=3
 

drop table #PES_TreeItemLvl2






---------------------------------------------------------------------------------
--4) создаем  (3й уровень в дереве - Потребитель- ЮЛ) = HierLev3
---------------------------------------------------------------------------------

SET @ParentHierID =null
SET @LastNode=null


create table #PES_TreeItemLvl3
(
Tree_ID int,
PESFreeHierItem_ID int, 
PESHierID hierarchyid,
HierLev2_ID int,
JuridicalPersonName nvarchar(400),
JuridicalPersonFreeHierItem_ID int
)
insert into #PES_TreeItemLvl3
select distinct  
	ta.Tree_ID,
	ta.PESFreeHierItem_ID, 
	HierID, 
	ta.HierLev2_ID,
	ta.JuridicalPersonName, 
	null
from #tempAll ta 
	join Dict_FreeHierarchyTree on  FreeHierItem_ID=ta.PESFreeHierItem_ID
where 
ta.Tree_ID is not null
and ta.PESFreeHierItem_ID is not null
and len(isnull(ta.JuridicalPersonName,''))>=3

 
  

update #PES_TreeItemLvl3
set JuridicalPersonFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID
from 
 #PES_TreeItemLvl3 temp2 
join Dict_FreeHierarchyTree 
		on temp2.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp2.JuridicalPersonName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=3 --следующий уровень
			and HierID.GetAncestor (1) = temp2.PESHierID 		--и предок равен HierID 
			
left join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID 
where
temp2.Tree_ID is not null
and temp2.PESFreeHierItem_ID is not null
and len(isnull(temp2.JuridicalPersonName,''))>=3

  

--теперь добавляем не найденные
set @maxFreeItemID =-1

BEGIN TRAN
begin try		
		--находим максимальный ИД						
		select @maxFreeItemID=max(FreeHierItem_ID) from Dict_FreeHierarchyTree 
		select @maxFreeItemID= isnull(@maxFreeItemID,0)

		delete from @tempFreeNodes						
		--добавляем узлы после последнего					
		insert into @tempFreeNodes  (Tree_ID, HierID,ID,StringName, EnumFreeHierarchyItemType,ObjectInt_ID) 			
		select distinct
		Tree_ID,
		res.PESHierID.ToString()+
		Convert(varchar,
				isnull(substring(lastchild.ChildHierID,		
				--берем цифру из последнего дочернего					
				len(lastchild.ChildHierID) - CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))+1,
						CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		JuridicalPersonFreeHierItem_ID=@maxFreeItemID+Number, 
		JuridicalPersonName	,
		EnumFreeHierarchyItemType= case when h3.HierLev3_ID is null then 0 else 3 end,
		ObjectInt_ID = h3.HierLev3_ID

		from  (select Tree_ID,
		HierLev2_ID,
					PESHierID, 
					Number= ROW_NUMBER() OVER(ORDER BY aa.JuridicalPersonName ASC),
					aa.JuridicalPersonName
				from 
				(
					select distinct Tree_ID, HierLev2_ID, PESFreeHierItem_ID, PESHierID, JuridicalPersonName
					from #PES_TreeItemLvl3 
					where Tree_ID is not null 
							and PESFreeHierItem_ID is not null	
							and HierLev2_ID is not null
							and JuridicalPersonFreeHierItem_ID is null				
				) as aa
			  ) as res
			  --выбираем максимальный дочерний HierID, либо ставим 0, затем выше номер распарсится и подставится +1
			outer apply (  select top 1 ChildHierID=  isnull( cast(max(HierID) as hierarchyid).ToString(),res.PESHierID.ToString()+'0/')
								FROM Dict_FreeHierarchyTree
								WHERE 
								FreeHierTree_ID= res.Tree_ID
								and HierID.GetAncestor (1) = res.PESHierID)   as lastchild 

			outer apply (select top 1 HierLev3_ID from Dict_HierLev3 
							where HierLev2_ID=res.HierLev2_ID and StringName like JuridicalPersonName) h3
		  

		--теперь добавляем в обе таблицы
		insert into Dict_FreeHierarchyTree
		(FreeHierTree_ID,	HierID,		FreeHierItem_ID,	StringName,	FreeHierItemType,	Expanded,	FreeHierIcon_ID,	SortNumber)
		select distinct Tree_ID, HierID,ID, StringName, EnumFreeHierarchyItemType, 0, null, null  
		from @tempFreeNodes

		insert into Dict_FreeHierarchyTree_Description
		(FreeHierItem_ID,IncludeObjectChildren, HierLev3_ID)
		select distinct ID, 0,ObjectInt_ID from @tempFreeNodes
		
		
end try
begin catch
			
	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  



--обновляем идентификаторы... (ищем строго на втором уровне GetLevel()=2) 


update #tempAll
set JuridicalPersonFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID,
HierLev3_ID= Dict_FreeHierarchyTree_Description.HierLev3_ID
from #tempAll temp
join #PES_TreeItemLvl3 temp2 
	on temp.Tree_ID= temp2.Tree_ID and temp.PESFreeHierItem_ID= temp2.PESFreeHierItem_ID and temp.JuridicalPersonName=temp2.JuridicalPersonName
join Dict_FreeHierarchyTree 
		on temp2.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp2.JuridicalPersonName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=3 --следующий уровень
			and HierID.GetAncestor (1) = temp2.PESHierID 		--и предок равен HierID 
			
left join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID 
where
temp.Tree_ID is not null
and temp.PESFreeHierItem_ID is not null
and len(isnull(temp.JuridicalPersonName,''))>=3
 

drop table #PES_TreeItemLvl3

 



 ---------------------------------------------------------------------------------
--4) создаем  (4й уровень в дереве - напряжение)
---------------------------------------------------------------------------------

SET @ParentHierID =null
SET @LastNode=null


create table #PES_TreeItemLvl4
(
Tree_ID int,
JuridicalPersonFreeHierItem_ID int, 
JuridicalPersonHierID hierarchyid,
HierLev3_ID int,
VoltageLevelName nvarchar(400),
VoltageLevelFreeHierItem_ID int
)
insert into #PES_TreeItemLvl4
select distinct  
	ta.Tree_ID,
	ta.JuridicalPersonFreeHierItem_ID, 
	HierID, 
	ta.HierLev3_ID,
	ta.VoltageLevel, 
	null
from #tempAll ta 
	join Dict_FreeHierarchyTree on  FreeHierItem_ID=ta.JuridicalPersonFreeHierItem_ID
where 
ta.Tree_ID is not null
and ta.JuridicalPersonFreeHierItem_ID is not null
and len(isnull(ta.VoltageLevel,''))>=1


 
 


--ищем корневые (GetLevel()=3) узлы с названиями равными ЮЛ в соответствующих деревьях
update #PES_TreeItemLvl4
set VoltageLevelFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID
from #PES_TreeItemLvl4 temp
 join Dict_FreeHierarchyTree 
		on temp.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp.VoltageLevelName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=4 --следующий уровень
			and HierID.GetAncestor (1) = temp.JuridicalPersonHierID 		--и предок равен HierID  
where
temp.Tree_ID is not null
and temp.JuridicalPersonFreeHierItem_ID is not null
and len(isnull(temp.VoltageLevelName,''))>=1
 
  

--теперь добавляем не найденные
set @maxFreeItemID =-1

BEGIN TRAN
begin try		
		--находим максимальный ИД						
		select @maxFreeItemID=max(FreeHierItem_ID) from Dict_FreeHierarchyTree 
		select @maxFreeItemID= isnull(@maxFreeItemID,0)

		delete from @tempFreeNodes						
		--добавляем узлы после последнего					
		insert into @tempFreeNodes  (Tree_ID, HierID,ID,StringName, EnumFreeHierarchyItemType,ObjectInt_ID) 			
		select distinct
		Tree_ID,
		res.JuridicalPersonHierID.ToString()+
		Convert(varchar,
				isnull(substring(lastchild.ChildHierID,		
				--берем цифру из последнего дочернего					
				len(lastchild.ChildHierID) - CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))+1,
						CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		VoltageLevelFreeHierItem_ID=@maxFreeItemID+Number, 
		VoltageLevelName	,
		EnumFreeHierarchyItemType= 0,
		ObjectInt_ID = null

		from  (select Tree_ID,		
					JuridicalPersonHierID, 
					Number= ROW_NUMBER() OVER(ORDER BY aa.VoltageLevelName ASC),
					aa.VoltageLevelName
				from 
				(
					select distinct Tree_ID,  JuridicalPersonFreeHierItem_ID, JuridicalPersonHierID, VoltageLevelName
					from #PES_TreeItemLvl4 rr 
					where Tree_ID is not null 
							and JuridicalPersonFreeHierItem_ID is not null	 
							and VoltageLevelFreeHierItem_ID is null				
				) as aa
			  ) as res
			  --выбираем максимальный дочерний HierID, либо ставим 0, затем выше номер распарсится и подставится +1
			outer apply (  select top 1 ChildHierID=  isnull( cast(max(HierID) as hierarchyid).ToString(),res.JuridicalPersonHierID.ToString()+'0/')
								FROM Dict_FreeHierarchyTree
								WHERE 
								FreeHierTree_ID=  res.Tree_ID
								and HierID.GetAncestor (1) = res.JuridicalPersonHierID)   as lastchild 
		 

		--теперь добавляем в обе таблицы
		insert into Dict_FreeHierarchyTree
		(FreeHierTree_ID,	HierID,		FreeHierItem_ID,	StringName,	FreeHierItemType,	Expanded,	FreeHierIcon_ID,	SortNumber)
		select distinct Tree_ID, HierID,ID, StringName, EnumFreeHierarchyItemType, 0, null, null  
		from @tempFreeNodes

		insert into Dict_FreeHierarchyTree_Description
		(FreeHierItem_ID,IncludeObjectChildren)
		select distinct ID, 0 from @tempFreeNodes
		
		
end try
begin catch

	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  



--обновляем идентификаторы... (ищем строго на втором уровне GetLevel()=2) 


update #tempAll
set VoltageLevelFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID  
from #tempAll temp
join #PES_TreeItemLvl4 temp2 
	on temp.Tree_ID= temp2.Tree_ID and temp.JuridicalPersonFreeHierItem_ID= temp2.JuridicalPersonFreeHierItem_ID and temp.VoltageLevel=temp2.VoltageLevelName
join Dict_FreeHierarchyTree 
		on temp2.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp2.VoltageLevelName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=4 --следующий уровень
			and HierID.GetAncestor (1) = temp2.JuridicalPersonHierID 		--и предок равен HierID 
			
left join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID 
where
temp.Tree_ID is not null
and temp.JuridicalPersonFreeHierItem_ID is not null
and len(isnull(temp.VoltageLevel,''))>=1
 

drop table #PES_TreeItemLvl4

 




 
 ---------------------------------------------------------------------------------
--5) создаем  (5й уровень в дереве - тариф)
---------------------------------------------------------------------------------

SET @ParentHierID =null
SET @LastNode=null


create table #PES_TreeItemlvl5
(
Tree_ID int,
VoltageLevelFreeHierItem_ID int, 
VoltageLevelHierID hierarchyid,
HierLev3_ID int,
TariffName nvarchar(400),
TariffFreeHierItem_ID int
)
insert into #PES_TreeItemlvl5
select distinct  
	ta.Tree_ID,
	ta.VoltageLevelFreeHierItem_ID, 
	HierID, 
	ta.HierLev3_ID,
	ta.TariffFullName, 
	null
from #tempAll ta 
	join Dict_FreeHierarchyTree on  FreeHierItem_ID=ta.VoltageLevelFreeHierItem_ID
where 
ta.Tree_ID is not null
and ta.VoltageLevelFreeHierItem_ID is not null
and len(isnull(ta.TariffFullName,''))>=1


 
 


--ищем корневые (GetLevel()=3) узлы с названиями равными ЮЛ в соответствующих деревьях
update #PES_TreeItemlvl5
set TariffFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID
from #PES_TreeItemlvl5 temp
 join Dict_FreeHierarchyTree 
		on temp.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp.TariffName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=5 --следующий уровень
			and HierID.GetAncestor (1) = temp.VoltageLevelHierID 		--и предок равен HierID  
where
temp.Tree_ID is not null
and temp.VoltageLevelFreeHierItem_ID is not null
and len(isnull(temp.TariffName,''))>=1
 
  

--теперь добавляем не найденные
set @maxFreeItemID =-1

BEGIN TRAN
begin try		
		--находим максимальный ИД						
		select @maxFreeItemID=max(FreeHierItem_ID) from Dict_FreeHierarchyTree 
		select @maxFreeItemID= isnull(@maxFreeItemID,0)

		delete from @tempFreeNodes						
		--добавляем узлы после последнего					
		insert into @tempFreeNodes  (Tree_ID, HierID,ID,StringName, EnumFreeHierarchyItemType,ObjectInt_ID) 			
		select distinct
		Tree_ID,
		res.VoltageLevelHierID.ToString()+
		Convert(varchar,
				isnull(substring(lastchild.ChildHierID,		
				--берем цифру из последнего дочернего					
				len(lastchild.ChildHierID) - CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))+1,
						CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		TariffFreeHierItem_ID=@maxFreeItemID+Number, 
		TariffName	,
		EnumFreeHierarchyItemType= 0,
		ObjectInt_ID = null

		from  (select Tree_ID,		
					VoltageLevelHierID, 
					Number= ROW_NUMBER() OVER(ORDER BY aa.TariffName ASC),
					aa.TariffName
				from 
				(
					select distinct Tree_ID,  VoltageLevelFreeHierItem_ID, VoltageLevelHierID, TariffName
					from #PES_TreeItemlvl5 rr 
					where Tree_ID is not null 
							and VoltageLevelFreeHierItem_ID is not null	 
							and TariffFreeHierItem_ID is null				
				) as aa
			  ) as res
			  --выбираем максимальный дочерний HierID, либо ставим 0, затем выше номер распарсится и подставится +1
			outer apply (  select top 1 ChildHierID=  isnull( cast(max(HierID) as hierarchyid).ToString(),res.VoltageLevelHierID.ToString()+'0/')
								FROM Dict_FreeHierarchyTree
								WHERE 
								FreeHierTree_ID=  res.Tree_ID
								and HierID.GetAncestor (1) = res.VoltageLevelHierID)   as lastchild 
		 

		--теперь добавляем в обе таблицы
		insert into Dict_FreeHierarchyTree
		(FreeHierTree_ID,	HierID,		FreeHierItem_ID,	StringName,	FreeHierItemType,	Expanded,	FreeHierIcon_ID,	SortNumber)
		select distinct Tree_ID, HierID,ID, StringName, EnumFreeHierarchyItemType, 0, null, null  
		from @tempFreeNodes

		insert into Dict_FreeHierarchyTree_Description
		(FreeHierItem_ID,IncludeObjectChildren)
		select distinct ID, 0 from @tempFreeNodes
		
		
end try
begin catch
			
	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  



--обновляем идентификаторы... (ищем строго на втором уровне GetLevel()=2) 


update #tempAll
set TariffFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID  
from #tempAll temp
join #PES_TreeItemlvl5 temp2 
	on temp.Tree_ID= temp2.Tree_ID and temp.VoltageLevelFreeHierItem_ID= temp2.VoltageLevelFreeHierItem_ID and temp.TariffFullName=temp2.TariffName
join Dict_FreeHierarchyTree 
		on temp2.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp2.TariffName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=5 --следующий уровень
			and HierID.GetAncestor (1) = temp2.VoltageLevelHierID 		--и предок равен HierID 
			
left join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID 
where
temp.Tree_ID is not null
and temp.VoltageLevelFreeHierItem_ID is not null
and len(isnull(temp.TariffFullName,''))>=1
 

drop table #PES_TreeItemlvl5

 



  ---------------------------------------------------------------------------------
--6) создаем  (6й уровень в дереве - тариф)
---------------------------------------------------------------------------------

SET @ParentHierID =null
SET @LastNode=null


--у нас есть ИД тарифа и есть ти ИД - удаляем узлы ТИ которые не соответствуют
--которые соответствуют оставляем...


create table #PES_TreeItemlvl6
(
Tree_ID int,
TariffNameFreeHierItem_ID int, 
TariffNameHierID hierarchyid, 
TIName nvarchar(400),
TI_ID int,
TIFreeHierItem_ID int
)
insert into #PES_TreeItemlvl6
select distinct  
	ta.Tree_ID,
	ta.TariffFreeHierItem_ID, 
	HierID, 
	ta.TIName, 
	ta.TI_ID,
	null
from #tempAll ta 
	join Dict_FreeHierarchyTree on  FreeHierItem_ID=ta.TariffFreeHierItem_ID --находим HierID родителя
where 
ta.Tree_ID is not null
and ta.TariffFreeHierItem_ID is not null
and TI_ID is not null


--ищем имеющиеся (родитель не изменился)
update #PES_TreeItemlvl6
set TIFreeHierItem_ID = fd.FreeHierItem_ID
from 
#PES_TreeItemlvl6 temp
 join Dict_FreeHierarchyTree -- все дочерние для тарифа
		on temp.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID  
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=6 --следующий уровень
			and HierID.GetAncestor (1) = temp.TariffNameHierID 		--и предок равен HierID  
join Dict_FreeHierarchyTree_Description fd --конкретные ТИ
	on fd.FreeHierItem_ID=Dict_FreeHierarchyTree.FreeHierItem_ID 
		and fd.TI_ID is not null and fd.TI_ID= temp.TI_ID
where
temp.Tree_ID is not null
and temp.TariffNameFreeHierItem_ID is not null
and temp.TI_ID is not null


--находим и удаляем ОТЛИЧАЮЩИЕСЯ связи ТИ- родитель - их надо будет удалить
 
 declare @OldPorentNode_TI table (TI_ID int, TIFreeHierarchyID int, OldParentID int)
 insert into @OldPorentNode_TI (TI_ID, TIFreeHierarchyID, OldParentID)
 select 
 distinct temp.TI_ID, freeTi.FreeHierItem_ID, freeTi.ParentFreeHierItem_ID 
 from 
  #PES_TreeItemlvl6 temp
  cross apply (select top 1 
						f.HierID, 
						f.FreeHierItem_ID,
						ParentHierID= f.HierID.GetAncestor(1) ,
						ParentFreeHierItem_ID=pfd.FreeHierItem_ID
				from 
				Dict_FreeHierarchyTree  f 
					join Dict_FreeHierarchyTree_Description fd	on	fd.FreeHierItem_ID=f.FreeHierItem_ID  
					join Dict_FreeHierarchyTree pfd		on	f.HierID.GetAncestor(1) =pfd.HierID
				where 
				f.FreeHierTree_ID= temp.Tree_ID 
				and f.FreeHierItemType= 5 
				and fd.TI_ID is not null and fd.TI_ID = temp.TI_ID
				and pfd.FreeHierItem_ID<> temp.TariffNameFreeHierItem_ID --??
					) as freeTi

--удаляем старые узлы (ТИ) если расположение ТИ поменялось ( будем создавать новую запись на новом TariffNameFreeHierItem_ID)
delete from Dict_FreeHierarchyTree where FreeHierItem_ID in (select TIFreeHierarchyID from @OldPorentNode_TI where TIFreeHierarchyID is not null)

  

--теперь добавляем не найденные
set @maxFreeItemID =-1

BEGIN TRAN
begin try		
		--находим максимальный ИД						
		select @maxFreeItemID=max(FreeHierItem_ID) from Dict_FreeHierarchyTree 
		select @maxFreeItemID= isnull(@maxFreeItemID,0)

		delete from @tempFreeNodes						
		--добавляем узлы после последнего					
		insert into @tempFreeNodes  (Tree_ID, HierID,ID,StringName, EnumFreeHierarchyItemType,ObjectInt_ID) 			
		select distinct
		Tree_ID,
		res.TariffNameHierID.ToString()+
		Convert(varchar,
				isnull(substring(lastchild.ChildHierID,		
				--берем цифру из последнего дочернего					
				len(lastchild.ChildHierID) - CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))+1,
						CHARINDEX('/',reverse(substring(lastchild.ChildHierID,1,len(lastchild.ChildHierID)-1)))-1),0)
				--инкрементируем для последующих
				+Number ) +'/',	
		TIFreeHierItem_ID=@maxFreeItemID+Number, 
		res.TIName	,
		EnumFreeHierarchyItemType= 5,
		ObjectInt_ID = res.TI_ID

		from  (select Tree_ID,		
					TariffNameHierID, 
					Number= ROW_NUMBER() OVER(ORDER BY aa.TIName , aa.TI_ID ASC),
					aa.TIName,
					aa.TI_ID

				from 
				(
					select distinct Tree_ID,  TariffNameFreeHierItem_ID, TariffNameHierID, TIName, TI_ID
					from #PES_TreeItemlvl6 rr 
					where Tree_ID is not null 
							and TariffNameFreeHierItem_ID is not null	 
							and TIFreeHierItem_ID is null		
							and TI_ID is not null		
				) as aa
			  ) as res
			  --выбираем максимальный дочерний HierID, либо ставим 0, затем выше номер распарсится и подставится +1
			outer apply (  select top 1 ChildHierID=  isnull( cast(max(HierID) as hierarchyid).ToString(),res.TariffNameHierID.ToString()+'0/')
								FROM Dict_FreeHierarchyTree
								WHERE 
								FreeHierTree_ID= res.Tree_ID
								and HierID.GetAncestor (1) = res.TariffNameHierID)   as lastchild 
		 

		--теперь добавляем в обе таблицы
		insert into Dict_FreeHierarchyTree
		(FreeHierTree_ID,	HierID,		FreeHierItem_ID,	StringName,	FreeHierItemType,	Expanded,	FreeHierIcon_ID,	SortNumber)
		select distinct Tree_ID, HierID,ID, StringName, EnumFreeHierarchyItemType, 0, null, null  
		from @tempFreeNodes

		insert into Dict_FreeHierarchyTree_Description
		(FreeHierItem_ID,IncludeObjectChildren, TI_ID)
		select distinct ID, 0, ObjectInt_ID	from @tempFreeNodes
		
		
end try
begin catch
			
	IF @@TRANCOUNT > 0  
	ROLLBACK TRANSACTION;  
end catch

IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION;  


--снова обновляем идентификаторы...
update #tempAll
set TIFreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID  
from #tempAll temp
join #PES_TreeItemlvl6 temp2 
	on temp.Tree_ID= temp2.Tree_ID and temp.TariffFreeHierItem_ID= temp2.TariffNameFreeHierItem_ID and temp.TI_ID=temp2.TI_ID
join Dict_FreeHierarchyTree 
		on temp2.Tree_ID= Dict_FreeHierarchyTree.FreeHierTree_ID 
			and  temp2.TIName= Dict_FreeHierarchyTree.StringName 
			and	Dict_FreeHierarchyTree.HierID.GetLevel()=5 --следующий уровень
			and HierID.GetAncestor (1) = temp2.TariffNameHierID 		--и предок равен HierID 
			
left join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID 
where
temp.Tree_ID is not null
and temp.TariffFreeHierItem_ID is not null
and temp.TI_ID is not null
 


drop table #PES_TreeItemlvl6

 

--удаялем узлы из загружаемого коренвого узла,  у которых нет дочерних ТИ
BEGIN TRY 
	 delete from Dict_FreeHierarchyTree
	 where FreeHierItem_ID
	 in (
		select tr.FreeHierItem_ID from 
		Dict_FreeHierarchyTree tr
		cross apply (select top 1 * 
						from Dict_FreeHierarchyTree lvl1 
						join #tempAll on lvl1.FreeHierTree_ID= #tempAll.Tree_ID 
						and  lvl1.HierLevel=1 --для загружаемых объектов первого уровня в дереве совб иерархии
						and tr.HierID<>lvl1.HierID 
						and tr.HierID.IsDescendantOf(lvl1.HierID)=1 ) as FreeHierLev1
		where 
		tr.FreeHierTree_ID=13 
		and not exists --где отсутствуют дочерние ТИ
			(select top 1 1 
			from Dict_FreeHierarchyTree ti 
			where 
			tr.FreeHierTree_ID = ti.FreeHierTree_ID 
			and ti.HierID<>tr.HierID
			and  ti.HierID.IsDescendantOf(tr.HierID)=1
			and ti.FreeHierItemType=5
			)
		and tr.HierLevel<6
		)

END TRY  
BEGIN CATCH     
        print ERROR_MESSAGE()   
END CATCH;  


drop table #tempAll

   
--select HierID.ToString(),* 
--from Dict_FreeHierarchyTree 
--join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree_Description.FreeHierItem_ID= Dict_FreeHierarchyTree.FreeHierItem_ID
--where FreeHierTree_ID=13
--order by HierLevel, StringName
 
END
GO
 

grant execute on usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy to UserCalcService
go
grant execute on usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy to UserDeclarator
go
grant execute on usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy to UserImportService
go
grant execute on usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy to UserExportService
go
  



if exists(select 1 from sysobjects where name='usp2_ImportNSI_Report_OEK_Section1')
   drop procedure usp2_ImportNSI_Report_OEK_Section1
go

if exists(select 1 from sysobjects where name='usp2_ImportNSI_Report_OEK_Section1')
   drop procedure usp2_ImportNSI_Report_OEK_Section1
go

create procedure  usp2_ImportNSI_Report_OEK_Section1
 @importNumber uniqueidentifier,
 @dt datetime,
 @UserID nvarchar(200)
AS
BEGIN


declare @msg nvarchar(400)

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


---пока только строго заменяем ТП/сечения для текущего месяца, поэтому импорт максимум 100тыс записей сделаем...

create table #tempSectionAll
(TSOName nvarchar(200), TSO_ID int, JuridicalPersonName nvarchar(250), JuridicalPerson_ID int, ContractName nvarchar(256) , ContractNumber nvarchar(256), Contract_ID int,
SectionName nvarchar(250), Section_ID int,
HierLev3_ID int,
TPName nvarchar(200), TP_ID int,
TI_ID int, TIName nvarchar(400), TPVoltageLevelName nvarchar(100), TPVoltageLevel int,
FormulaUN nvarchar(32), FormulaName nvarchar(200), 
StartDateTime datetime, FinishDateTime datetime, 
MeterSerialNumber nvarchar(200))

insert into #tempSectionAll
(TSOName,TSO_ID,
 JuridicalPersonName , JuridicalPerson_ID , ContractName  , ContractNumber , Contract_ID ,
SectionName , Section_ID ,
HierLev3_ID ,
TPName , TP_ID ,
TI_ID , TIName, TPVoltageLevelName , TPVoltageLevel,
FormulaUN, FormulaName, StartDateTime, FinishDateTime,
MeterSerialNumber)

select distinct 
PESName, null,
JuridicalPersonName, JuridicalPerson_ID, '','',null,
SectionName=JuridicalPersonName, null,
HierLev3_ID,
null, null,
TI_ID,TIName, VoltageLevel, null, 
null, 'Формула '+ isnull(TIName,''),
EventDatePrevious, EventDateCurrent,
SerialNumber

 from ImportNSI_SourceData_Report18Jur
where ImportNumber=@importNumber
and AllowLoad=1
and TI_ID is not null


set @msg=convert(varchar, getdate(),121)+ ' Добавляем организацию "Энерго"' 
--print @msg

	--находим имеющиеся
	update #tempSectionAll
	set 
	TSO_ID = Dict_JuridicalPersons.JuridicalPerson_ID
	from
	#tempSectionAll res
	join Dict_JuridicalPersons  on  Dict_JuridicalPersons.StringName = ltrim(rtrim(res.TSOName))
	where
	res.TSO_ID is null
	
	declare @maxTSO_Id int =0

	BEGIN TRANSACTION JuridicalPerson;	
		begin try

			select @maxTSO_Id=max(JuridicalPerson_ID) from Dict_JuridicalPersons
			select @maxTSO_Id= isnull(@maxTSO_Id,0)
		
			--добавляем 
			insert into Dict_JuridicalPersons (JuridicalPerson_ID, StringName, JuridicalAbonentCode, JuridicalINN)
			select distinct  @maxTSO_Id+ROW_NUMBER() OVER(ORDER BY aa.JuridicalpersonName ASC),aa.JuridicalpersonName,'',''
			from 
			(
			select distinct  JuridicalpersonName=ltrim(rtrim(TSOName))
			from #tempSectionAll 
			where 
			TSO_ID is null
			) as aa

		end try
		begin catch
			set @maxTSO_Id =0
		
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
	IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;


	--повторно обновляем
	update #tempSectionAll
	set 
	TSO_ID = Dict_JuridicalPersons.JuridicalPerson_ID
	from
	#tempSectionAll res
	join Dict_JuridicalPersons  on  Dict_JuridicalPersons.StringName = ltrim(rtrim(res.TSOName))
	where
	res.TSO_ID is null


		BEGIN TRANSACTION JuridicalPerson;	
		begin try

			--добавляем ЮЛ на первый попавшийся объект 1го уровня..  
			insert into Dict_JuridicalPersons_To_HierLevels
			(JuridicalPerson_ID, HierLev1_ID)
			select distinct temp.TSO_ID, Dict_HierLev2.HierLev1_ID
			from #tempSectionAll temp
			join Dict_HierLev3 on Dict_HierLev3.HierLev3_ID=temp.HierLev3_ID
			join Dict_HierLev2 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID
			where 
			TSO_ID is not null
			and temp.TSO_ID not in (select JuridicalPerson_ID from Dict_JuridicalPersons_To_HierLevels)

		end try
		begin catch 
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
		IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;

set @msg=convert(varchar, getdate(),121)+ ' Добавляем Юрлица' 
--print @msg

	--находим имеющиеся
	update #tempSectionAll
	set 
	JuridicalPerson_ID = Dict_JuridicalPersons.JuridicalPerson_ID
	from
	#tempSectionAll res
	join Dict_JuridicalPersons  on  Dict_JuridicalPersons.StringName = ltrim(rtrim(res.JuridicalpersonName))
	where
	res.JuridicalPerson_ID is null
	
	declare @maxJuridicalPersonId int =0

	BEGIN TRANSACTION JuridicalPerson;	
		begin try

			select @maxJuridicalPersonId=max(JuridicalPerson_ID) from Dict_JuridicalPersons
			select @maxJuridicalPersonId= isnull(@maxJuridicalPersonId,0)
		
			--добавляем 
			insert into Dict_JuridicalPersons (JuridicalPerson_ID, StringName, JuridicalAbonentCode, JuridicalINN)
			select distinct  @maxJuridicalPersonId+ROW_NUMBER() OVER(ORDER BY aa.JuridicalpersonName ASC),aa.JuridicalpersonName,'',''
			from 
			(
			select distinct  JuridicalpersonName=ltrim(rtrim(JuridicalpersonName))
			from #tempSectionAll 
			where 
			JuridicalPerson_ID is null
			) as aa

		end try
		begin catch
			set @maxJuridicalPersonId =0
		
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
	IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;


	--повторно обновляем
	update #tempSectionAll
	set 
	JuridicalPerson_ID = Dict_JuridicalPersons.JuridicalPerson_ID
	from
	#tempSectionAll res
	join Dict_JuridicalPersons  on  Dict_JuridicalPersons.StringName = ltrim(rtrim(res.JuridicalpersonName))
	where
	res.JuridicalPerson_ID is null



	BEGIN TRANSACTION JuridicalPerson;	
		begin try

			--добавляем ЮЛ на.. первый попавшийся объект 3го уровня..  
			insert into Dict_JuridicalPersons_To_HierLevels
			(JuridicalPerson_ID, HierLev3_ID)
			select distinct temp.JuridicalPerson_ID, Dict_HierLev3.HierLev3_ID
			from #tempSectionAll temp
			join Dict_HierLev3 on Dict_HierLev3.HierLev3_ID=temp.HierLev3_ID
			join Dict_HierLev2 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID
			where 
			JuridicalPerson_ID is not null
			and temp.JuridicalPerson_ID not in (select JuridicalPerson_ID from Dict_JuridicalPersons_To_HierLevels)

		end try
		begin catch		
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch		
		IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;


--для корректного иморта необходимы ИНН организаций, номера и даты договоров, так как этих данных нет, 
--то создаем один договор для ЮЛ-ТСО и в него добавляем все ТИ этого ЮЛ (+ фильтр по ТСО)

set @msg=convert(varchar, getdate(),121)+ ' Добавляем Договора ЮЛ-ТСО' 
--print @msg
 
	--находим имеющиеся
	update #tempSectionAll
	set 
	Contract_ID = Dict_JuridicalPersons_Contracts.JuridicalPersonContract_ID,
	ContractName= left(concat(TSOName,'-',JuridicalPersonName),256),
	ContractNumber= Dict_JuridicalPersons_Contracts.ContractNumber
	from
	#tempSectionAll res
	join Dict_JuridicalPersons_Contracts  
			on  	--будет один договор в который мы будем вносить изменения (в сечение)
			Dict_JuridicalPersons_Contracts.JuridicalPerson_ID=res.JuridicalPerson_ID and Dict_JuridicalPersons_Contracts.PowerSupplyingIntermediary_ID=res.TSO_ID
	where
	res.JuridicalPerson_ID is not null
	and res.TSO_ID is not null 
	and res.Contract_ID is null

	
	declare @maxContractId int =0

	BEGIN TRANSACTION JuridicalPerson;	
		begin try

			select @maxContractId=max(JuridicalPersonContract_ID) from Dict_JuridicalPersons_Contracts
			select @maxContractId= isnull(@maxContractId,0)
		
			--добавляем 
			insert into Dict_JuridicalPersons_Contracts (JuridicalPersonContract_ID, 
														JuridicalPerson_ID, StringName, ContractNumber, AccountNumber, 
														SignDate, WarrantedSupplier_ID, PowerSupplyingIntermediary_ID)
			select distinct @maxContractId+ROW_NUMBER() OVER(ORDER BY aa.ContractNumber ASC),
			JuridicalPerson_ID, 
			ContractName,
			ContractNumber,'',
			'01-01-2010', TSO_ID, TSO_ID
			from 
			(
			select distinct  
			JuridicalPerson_ID,
			TSO_ID,
			ContractName=left(concat(TSOName,'-',JuridicalPersonName),256), 
			ContractNumber=CONCAT('ТСО_',left (concat('00000',TSO_ID),10),':ЮЛ_',left (concat('00000',JuridicalPerson_ID),10))
			from #tempSectionAll 
			where 
			JuridicalPerson_ID is not null
			and TSO_ID is not null 
			and Contract_ID is null

			) as aa

		end try
		begin catch
			set @maxJuridicalPersonId =0
		
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
	IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;


	--повторно обновляем
	update #tempSectionAll
	set 
	Contract_ID = Dict_JuridicalPersons_Contracts.JuridicalPersonContract_ID,
	ContractName= Dict_JuridicalPersons_Contracts.StringName,
	ContractNumber= Dict_JuridicalPersons_Contracts.ContractNumber
	from
	#tempSectionAll res
	join Dict_JuridicalPersons_Contracts  
			on  	--будет один договор в который мы будем вносить изменения (в сечение)
			Dict_JuridicalPersons_Contracts.JuridicalPerson_ID=res.JuridicalPerson_ID and Dict_JuridicalPersons_Contracts.PowerSupplyingIntermediary_ID=res.TSO_ID
	where
	res.JuridicalPerson_ID is not null
	and res.TSO_ID is not null 
	and res.Contract_ID is null

	 


set @msg=convert(varchar, getdate(),121)+ ' Добавляем СЕЧЕНИЕ под объект 3го уровня' 
--print @msg

--объект третьего уровня это и есть юрлицо
--но сечений может быть несколько поэтмоу находим по HierLev3_ID+ название (какое?)

	--находим имеющиеся
	update #tempSectionAll
	set 
	Section_ID= Info_Section_List.Section_ID
	from
	#tempSectionAll res
	join Info_Section_List  
			on  	
			Info_Section_List.PS_ID is null 
			and Info_Section_List.HierLev3_ID is not null
			and Info_Section_List.HierLev3_ID=res.HierLev3_ID and Info_Section_List.SectionName=left(concat(TSOName,'-',JuridicalPersonName),1024)
	where
	res.JuridicalPerson_ID is not null
	and res.TSO_ID is not null 
	and res.Contract_ID is not null
	and res.HierLev3_ID is not null
	and res.Section_ID is null

	 
	declare @maxSectionId int =0

	BEGIN TRANSACTION JuridicalPerson;	
		begin try

			select @maxSectionId=max(Section_ID) from Info_Section_List
			select @maxSectionId= isnull(@maxSectionId,0)
		
			--добавляем 
			insert into Info_Section_List 
			(Hierlev3_ID, Section_ID, SectionName, SectionType,CUS_ID)
			select distinct 
			Hierlev3_ID, @maxSectionId+ROW_NUMBER() OVER(ORDER BY aa.SectionName ASC),
			SectionName,0,0			
			from 
			(
				select distinct  
					HierLev3_ID,
					SectionName=left(concat(TSOName,'-',JuridicalPersonName),1024)			
				from #tempSectionAll 
				where 
				JuridicalPerson_ID is not null
				and TSO_ID is not null 
				and Contract_ID is not null
				and HierLev3_ID is not null
				and Section_ID is null
			) as aa

		end try
		begin catch
			set @maxJuridicalPersonId =0
		
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
	IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;


	--повторно обновляем
	update #tempSectionAll
	set 
	Section_ID= Info_Section_List.Section_ID
	from
	#tempSectionAll res
	join Info_Section_List  
			on  	
			Info_Section_List.PS_ID is null 
			and Info_Section_List.HierLev3_ID is not null
			and Info_Section_List.HierLev3_ID=res.HierLev3_ID and Info_Section_List.SectionName=left(concat(TSOName,'-',JuridicalPersonName),1024)
	where
	res.JuridicalPerson_ID is not null
	and res.TSO_ID is not null 
	and res.Contract_ID is not null
	and res.HierLev3_ID is not null
	and res.Section_ID is null



set @msg=convert(varchar, getdate(),121)+ ' Привязка сечения к договору' 
--print @msg

 insert into Info_Section_To_JuridicalContract
(Section_ID, JuridicalPersonContract_ID, CUS_ID) 
select distinct Section_ID, Contract_ID,0
from #tempSectionAll
where 
Section_ID is not null
and Contract_ID is not null
and Section_ID not in (select Section_ID from Info_Section_To_JuridicalContract)



--точки поставки сначала добавляем для сечение+ТИ, 
--время действия ТП неограничено.. огарничиваем только время действяи формулы?????
set @msg=convert(varchar, getdate(),121)+ ' добавляем Точки поставки' 
--print @msg 

	--находим имеющиеся  Код ТП = ИД сечния+ТИ
	update #tempSectionAll
	set 
	TP_ID= Info_TP2.TP_ID,
	TPName= Info_TP2.StringName
	from
	#tempSectionAll res
	join Info_TP2  
			on  	
			Info_TP2.TPATSCode  is not null 
			and Info_TP2.TPATSCode=left(concat('Section=',convert(varchar(20),Section_ID),';TI=',convert(varchar(20),TI_ID)),128)
	where
	res.Section_ID is not null
	and res.TI_ID is not null
	and res.TP_ID is null

	 
	declare @maxTPId int =0

	BEGIN TRANSACTION JuridicalPerson;	
		begin try

			select @maxTPId=max(TP_ID) from Info_TP2
			select @maxTPId= isnull(@maxTPId,0)
		
			--добавляем 
			insert into Info_TP2 
			(TP_ID,
				DirectConsumer_ID,
				StringName,
				TPMode,
				IsMoneyOurSide,
				IsMoneyOurSideMode2,
				EvalModeOurSide,
				EvalModeContr,
				ExcludeFromXMLExport,
				IASection,
				TPATSCode,
				Voltage,
				CUS_ID)
			select distinct 
			 @maxTPId+ROW_NUMBER() OVER(ORDER BY aa.TPATSCode ASC),
			 null, 
			 StringName,
			 1,
			 1,0,
			 1,0,0,
			 null, 	
			 TPATSCode, 0, 0					
			from 
			(
				select distinct 
					SectioN_ID,
					TI_ID, 
					StringName=TIName,
					TPATSCode=left(concat('Section=',convert(varchar(20),Section_ID),';TI=',convert(varchar(20),TI_ID)),128)
				from #tempSectionAll 
				where 
				Section_ID is not null
				and TI_ID is not null
				and TP_ID is null
			) as aa

		end try
		begin catch
			set @maxJuridicalPersonId =0
		
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
	IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;


	--повторно обновляем
	update #tempSectionAll
	set 
	TP_ID= Info_TP2.TP_ID,
	TPName= Info_TP2.StringName
	from
	#tempSectionAll res
	join Info_TP2  
			on  	
			Info_TP2.TPATSCode  is not null 
			and Info_TP2.TPATSCode=left(concat('Section=',convert(varchar(20),Section_ID),';TI=',convert(varchar(20),TI_ID)),128)
	where
	res.Section_ID is not null
	and res.TI_ID is not null
	and res.TP_ID is null


	 
declare @endDate datetime =dateadd(S,-1, dateadd(M,1,@dt))

			
set @msg=convert(varchar, getdate(),121)+ ' Точки поставки периоды' 
--print @msg


	BEGIN TRANSACTION JuridicalPerson;	
		begin try
		
		--для всех ТИ из файла и отдельно для всех сечений из файла оганичиваем периоды входящий в текущий месяц....

		
		--ОГРАНИЧИВАЕМ периоды действия  
		--точки поставки добавили, теперь добавляем/редактируем связи с сечениями		


		--для всех ТП загружаемых СЕЧЕНИЙ в диапазоны которых полностью входит текущий месяц  - разбиваем на два - до него и после
		--из БД
		--дополнительно добавляем ТП для всех ТИ из загружаемых сечений (т.к. она может быть перенесена, соотв в бывшем сечении надо ограничить эту ТП)
		declare @TPSplitTable1 table (TP_ID int, Section_ID int, StartDateTime datetime)
		insert into @TPSplitTable1
		select distinct TP_ID,Section_ID,StartDateTime
		from Info_Section_Description2
		where 		
		(
		 --из импортируемых сечений
		 Section_ID in (select distinct Section_ID from #tempSectionAll where Section_ID is not null)
			or
		 --из связей с ТИ для импортируемых сечений так как ТИ входит в 1 сечение в 1 момент времени
		 TP_ID in (select Info_TP2_OurSide_Formula_List.TP_ID from 
					Info_TP2_OurSide_Formula_List 
					join Info_TP2_OurSide_Formula_Description on Info_TP2_OurSide_Formula_List.Formula_UN =Info_TP2_OurSide_Formula_Description.Formula_UN
					join #tempSectionAll temp on  Info_TP2_OurSide_Formula_Description.TI_ID = isnull(temp.TI_ID,-1)
					 )
		) 


		--print '====================================================================='

		--1) удаляем периоды внутри текущего месяца
		--всего сечения (чтобы не было проблем с "исчезнувшими" ТП из сечения в новой загрузке)
		delete from Info_Section_Description2
		where 
		Section_ID in (select distinct Section_ID from @TPSplitTable1)
		and StartDateTime>=@dt and StartDateTime<=@endDate
		and isnull(FinishDateTime, '01-01-2100')>=@dt and isnull(FinishDateTime, '01-01-2100')<=@endDate
				

		--2) изменяем дату начала для диапазонов, которые начинаются в текущем месяце, а заканчиваются в следующих
		update 	Info_Section_Description2
		set StartDateTime=  dateadd(M,1,@dt)
		from Info_Section_Description2 
		join @TPSplitTable1 temp on Info_Section_Description2.Section_ID=temp.Section_ID 
									and Info_Section_Description2.TP_ID=temp.TP_ID
									and isnull(Info_Section_Description2.FinishDateTime,'01-01-2100') >@endDate
									and Info_Section_Description2.StartDateTime between @dt and @endDate
		
		--3) изменяем дату завершения для диапазонов, 
		--которые начинаются в предыдущие периоды а заканчиваются в текущем месяце
		update 	Info_Section_Description2
		set FinishDateTime=  dateadd(S,-1,@dt)
		from Info_Section_Description2 
		join @TPSplitTable1 temp 
		on Info_Section_Description2.Section_ID=temp.Section_ID 
					and Info_Section_Description2.TP_ID=temp.TP_ID
					and Info_Section_Description2.StartDateTime<@dt
					and isnull(Info_Section_Description2.FinishDateTime,'01-01-2100') between @dt and @endDate
									
												

		--4) запоминаем периоды, которые будем разрывать, чтобы потом вставить второй период
		-- это только периоды которые заканчиваются в следующих месяцах..
		declare @tempPeriod2 table (TP_ID int, Section_ID int, StartDateTime datetime , FinishDateTime datetime)		
		insert into @tempPeriod2
		select distinct Info_Section_Description2.TP_ID, Info_Section_Description2.Section_ID, Info_Section_Description2.StartDateTime, Info_Section_Description2.FinishDateTime
		from Info_Section_Description2 
		join @TPSplitTable1 temp on Info_Section_Description2.Section_ID=temp.Section_ID 
									and Info_Section_Description2.TP_ID=temp.TP_ID
									and Info_Section_Description2.StartDateTime< @dt --temp.StartDateTime
									and isnull(Info_Section_Description2.FinishDateTime,'01-01-2100') > @endDate
											
		--первая часть периода
		update Info_Section_Description2
		set FinishDateTime=  dateadd(S,-1,@dt)
			from Info_Section_Description2 
		join @tempPeriod2 temp on Info_Section_Description2.Section_ID=temp.Section_ID 
									and Info_Section_Description2.TP_ID=temp.TP_ID
									and Info_Section_Description2.StartDateTime=temp.StartDateTime
	
		--вторая часть периода
		--если не добавлять то исчезнут остальные периоды при повторной загрузке например середины года
		--если добавлять то.. получается два периода действия до 2100 года для одной ТИ.. (если ее срок ограничен или перенесена..)
		insert into Info_Section_Description2(TP_ID, Section_ID, StartDateTime, FinishDateTime, IsTransit, CUS_ID)
		select distinct 
		TP_ID, Section_ID,DATEADD(M,1, @dt), FinishDateTime ,0,0
		from @tempPeriod2 temp
		where 
		not exists (select top 1 1 
					from Info_Section_Description2 
					where Section_ID=temp.Section_ID and TP_ID = temp.TP_ID and StartDateTime= temp.StartDateTime)


		--таким образом.. внутри месяца у нас нет диапазонов
		--добавляем диапазоны текущего месяца из текущей загрузки (!)
		insert into Info_Section_Description2(TP_ID, Section_ID, StartDateTime, FinishDateTime, IsTransit, CUS_ID)
		select distinct 
		TP_ID, Section_ID,StartDateTime, FinishDateTime ,0,0
		from #tempSectionAll temp
		where 
		not exists (select top 1 1 
					from Info_Section_Description2 
					where Section_ID=temp.Section_ID and TP_ID = temp.TP_ID and StartDateTime= temp.StartDateTime)
						

		--объединяем диапазоны у котрых даты конец=начало-1сек
		--пока есть такие диапазоны и максимум 5 раз
			
		set @msg=convert(varchar, getdate(),121)+ ' Точки поставки периоды - совмещаем' 
		--print @msg

		declare @count int =1
		while @count<5 and exists (select top 1 1 from
									Info_Section_Description2 isd1
									cross apply (select top 1 FinishDateTime=isnull(isd2.FinishDateTime,'01-01-2100') from Info_Section_Description2 isd2 
												where  isd2.Section_ID=isd1.Section_ID 
												and isd2.TP_ID = isd1.TP_ID 
												and isd1.FinishDateTime is not null
												and isd1.FinishDateTime= DATEADD(s,-1, isd2.StartDateTime)) as nextPeriod
									where 
									isd1.Section_ID in (select distinct Section_ID from #tempSectionAll where Section_ID is not null))
		begin

			update Info_Section_Description2
			set FinishDateTime=nextPeriod.FinishDateTime
			from
			Info_Section_Description2 isd1
			cross apply (select top 1 FinishDateTime=isnull(isd2.FinishDateTime,'01-01-2100') from Info_Section_Description2 isd2 
						where  isd2.Section_ID=isd1.Section_ID 
						and isd2.TP_ID = isd1.TP_ID 
						and isd1.FinishDateTime is not null
						and isd1.FinishDateTime= DATEADD(s,-1, isd2.StartDateTime)) as nextPeriod
			where 
			isd1.Section_ID in (select distinct Section_ID from #tempSectionAll where Section_ID is not null)
	
			set @count= @count+1

		end

		--удаляем для импортируемых сечений периоды ТП входящие в другие
		delete from Info_Section_Description2
		where 
		Section_ID in (select distinct Section_ID from #tempSectionAll where Section_ID is not null)
		and FinishDateTime is not null
		and exists (select top 1 1 from Info_Section_Description2 isd2 
					where  isd2.Section_ID=Info_Section_Description2.Section_ID 
						and isd2.TP_ID = Info_Section_Description2.TP_ID 
						and Info_Section_Description2.StartDateTime between isd2.StartDateTime and isnull(isd2.FinishDateTime,'01-01-2100')
						and isnull(Info_Section_Description2.FinishDateTime,'01-01-2100') between isd2.StartDateTime and isnull(isd2.FinishDateTime,'01-01-2100')
						and Info_Section_Description2.StartDateTime <> isd2.StartDateTime
						)


		--самой последней ТП если это текущий месяц и дата завершения = 23-59 последнего дня  - ставим конец =2100гг тк проблемы с отображением в АРМе будут
		-- при импорте следующих месяцев давты все равно будут ограничены (таким образом исчезнувшие ТП номрально обработаются)
		update Info_Section_Description2
		set 
		FinishDateTime='01-01-2100'
		where 
		TP_ID in (select distinct TP_ID from #tempSectionAll where TP_ID is not null)
		and FinishDateTime= @endDate
		and not exists (select top 1 1 from Info_Section_Description2 isd2 where
								isd2.Section_ID=Info_Section_Description2.Section_ID 
												and isd2.TP_ID = Info_Section_Description2.TP_ID 
												and isd2.StartDateTime>Info_Section_Description2.StartDateTime )

				
			
		end try
		begin catch
		
			print ERROR_MESSAGE()
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION JuridicalPerson; 
		end catch
		
	IF @@TRANCOUNT > 0  
		COMMIT TRANSACTION JuridicalPerson;



set @msg=convert(varchar, getdate(),121)+ ' Формулы - пересоздаем в соотв с периодами ТП' 
--print @msg


	BEGIN TRANSACTION JuridicalPerson;	
	begin try
	
		delete from Info_TP2_OurSide_Formula_List
		where 
		TP_ID in (select distinct TP_ID from #tempSectionAll where TP_ID is not null)
		
		declare @tempFormulaTable table (TP_ID int, Formula_UN varchar(32), FormulaName nvarchar(200), 
										StartDateTime datetime, FinishDateTime datetime, TI_ID int )

		insert into @tempFormulaTable
		select 
		Info_TP2.TP_ID,
		Formula_UN=dbo.usf2_CLR_Generate_NewID(57330),
		FormulaName=left(Info_TP2.StringName,200),
		StartDateTime, FinishDateTime,
		TPDescr.TI_ID
		from 
		Info_Section_Description2
		join Info_TP2 on Info_TP2.TP_ID=Info_Section_Description2.TP_ID
		cross apply (select top 1 TI_ID, TIName from #tempSectionAll temp where Info_Section_Description2.TP_ID=temp.TP_ID ) as TPDescr
		where 
		Info_TP2.TP_ID in (select distinct TP_ID from #tempSectionAll where TP_ID is not null)
		
		insert into Info_TP2_OurSide_Formula_List 
		(
		TP_ID, 
		Formula_UN, 
		FormulaName, 
		StartDateTime, FinishDateTime,
		ForAutoUse, FormulaType_ID,ChannelType, CUS_ID, 
		User_ID
		)
		select distinct 
		TP_ID, 
		Formula_UN, 
		FormulaName, 
		StartDateTime, FinishDateTime,
		1,0,1,0,
		@UserID
		from @tempFormulaTable
		
		insert into Info_TP2_OurSide_Formula_Description
		(Formula_UN,StringNumber,OperBefore, TI_ID, ChannelType, OperAfter,CUS_ID)
		select distinct 
		Formula_UN, 
		'1', '', TI_ID, 1, '', 0
		from @tempFormulaTable

	end try
	begin catch
		
		IF @@TRANCOUNT > 0  
			ROLLBACK TRANSACTION JuridicalPerson; 
	end catch
		
	IF @@TRANCOUNT > 0  
	COMMIT TRANSACTION JuridicalPerson;


drop table #tempSectionAll
 
 
END
GO

grant execute on usp2_ImportNSI_Report_OEK_Section1 to UserCalcService
go
grant execute on usp2_ImportNSI_Report_OEK_Section1 to UserDeclarator
go
grant execute on usp2_ImportNSI_Report_OEK_Section1 to UserImportService
go
grant execute on usp2_ImportNSI_Report_OEK_Section1 to UserExportService
go
  



create procedure  usp2_ImportNSI_Report_OEK_Integrals
 @UserID nvarchar(200),
 @EventDateTime datetime,
 @DataSourceType int,
 @AllowLoadAddress bit = 0,
 @AllowLoadTRCoeff bit =0, 
 @LastImportNumber uniqueidentifier=null, 
 @FreeHierarchyTypeLoad tinyint = 0 --0 - не загружаем, 1 - загружаем, 2 - загружаем только НСИ и дерево своб иерархии

AS
BEGIN
 
--приводим дату к первому числу если накосячат
set  @EventDateTime=DATEADD(m, DATEDIFF(m, 0,@EventDateTime), 0)
 

declare @msg nvarchar(400)='';
declare @ObjectName nvarchar(200)='ImportNSI_Report_OEK_Integrals'

set dateformat dmy
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


set @msg=convert(varchar, getdate(),121)+ ' Обновили данные' 
--print @msg

 
update  ImportNSI_SourceData_Report18Jur 
 set EventDate=@EventDateTime 
 where ImportNumber = @LastImportNumber
   
--а поиск по названию?
--update   ImportNSI_SourceData_Report18Jur
--set TIName= TIName+ ' ('+SerialNumber+')'
--where ImportNumber = @LastImportNumber
--and len(isnull(TIName,''))>1
--and len(isnull(SerialNumber,''))>1


--пропускаем импорт транзитных ТИ дублей - для которых есть расчетные
update ImportNSI_SourceData_Report18Jur 
set AllowLoad=0, ErrorMessage =a.ErrorMessage+ ' дубль (Тип ПУ = транзитный);'
from ImportNSI_SourceData_Report18Jur a
where 
a.ImportNumber= @LastImportNumber 
and AllowLoad=1 
and a.MeterType = 'транзитный'
and  exists (select top 1 1 
					from ImportNSI_SourceData_Report18Jur b
					 where 
						a.ImportNumber= b.ImportNumber 
						and AllowLoad=1 
						and a.SheetName= b.SheetName 				
						and a.TIEPPCode = b.TIEPPCode 
						and a.TariffName= b.TariffName 
						and b.MeterType='расчетный'
						and a.RowID<> b.RowID
						)

--это может быть переход через 0, поэтому оставляем
----пропускаем некорректные показания
--update ImportNSI_SourceData_Report18Jur 
--set AllowLoad=0, ErrorMessage =a.ErrorMessage+ ' некорректные показания;'
--from ImportNSI_SourceData_Report18Jur a
--where 
--a.ImportNumber= @LastImportNumber 
--and AllowLoad=1 -- берем только оставшиеся корректные
--and isnull(a.IntegralValue_Current,0)<isnull(a.IntegralValue_Previous,0)




set @msg=convert(varchar, getdate(),121)+ ' Поиск ТИ по коду' 
--print @msg

--находим ТИ по уникальному коду, который храним в ЕПП
--по заводскому номеру как предполагалось ранее не ищем - так как балансы уже настроены на чужие ТИ.. 
--т.е. (наши ТИ не будем ломать поиском по зав номеру!!!)

--если код не указан то ранее указали его как №Зав.номер, поэтому всегда ищем только по коду ЕПП!
update ImportNSI_SourceData_Report18Jur
set 
TI_ID = Info_TI.TI_ID 
from
ImportNSI_SourceData_Report18Jur res
join Info_TI on Info_TI.EPP_ID=res.TIEPPCode
where
res.ImportNumber= @LastImportNumber 
and res.AllowLoad=1
and res.TI_ID is null 
and len(rtrim(isnull(res.TIEPPCode,'')))>3


--ПОКА Не используем
-- автозамена из таблиы замен для используемых столбцов
--if (@AutoReplace=1)
--begin

--	declare @FieldName nvarchar(200)='', @sql nvarchar(max)=''
	
--	DECLARE replace_Cursor CURSOR FOR  
--	SELECT StringValue FROM @UsedColumns
--	where StringValue not like '%_ID' --исключаем идентификаторы
--	and (StringValue  like '%name%'  or StringValue  like '%model%'  )
--	order by StringValue  

--	OPEN replace_Cursor;      
--	FETCH NEXT FROM replace_Cursor into @FieldName 
  
--	WHILE @@FETCH_STATUS = 0  
--	BEGIN   
	   
--		--случаи где будет явно указан идентификатор обрабатываем отдельно  (адрес на ФИАС код)

--		--остальные поля обновляем из таблицы замен
--		set @sql = N'	
--		update  ImportNSI_SourceData_Report18Jur set '+@FieldName+'= REPLACE('+@FieldName+','''+'replaceData.OldValue'+''','''+ 'replaceData.NewValue'+''')
--		--select * 
--		from ImportNSI_SourceData_Report18Jur res 
--		cross apply (select top 1 NewValue,OldValue from  ImportNSI_ReplaceData
--						where
--						 ObjectName = '''+@ObjectName+'''
--						 and FieldName ='''+@FieldName+'''
--						 and OldValue is not null 
--						 and NewValue is not null	
--						 and NewID is  null) replaceData
--		where 
--		res.ImportNumber= '''+convert(varchar,@LastImportNumber) +''' 
--		and res.AllowLoad =1 and
--		'+@FieldName+' is not null ' 

--		--exec sp_executesql @sql
	
--	   FETCH NEXT FROM replace_Cursor into @FieldName 
--	END    
--	CLOSE replace_Cursor;  
--	DEALLOCATE replace_Cursor;  	
--end


--set @msg=convert(varchar, getdate(),121)+ ' Сделали автозамену' 
--print @msg


set @msg=convert(varchar, getdate(),121)+ ' Добавляем родительские объекты' 
--print @msg


declare @H1Defaultname nvarchar(200)= '18_ЮЛ'
declare @H1DefaultID tinyint = 9
  
--====================================================================
--уровень 1 создан и задается жестко =18_ЮЛ, 
update ImportNSI_SourceData_Report18Jur
set HierLev1_ID = @H1DefaultID
where 
ImportNumber= @LastImportNumber
and AllowLoad=1  

if (not exists (select top 1 1 from Dict_HierLev1 where HierLev1_ID=@H1DefaultID))
begin
	INSERT INTO Dict_HierLev1
		(HierLev1_ID,
StringName,
SortNumber,
KPOCode,
Description)
	values(@H1DefaultID,@H1Defaultname,null,null, null)
end

set @msg=convert(varchar, getdate(),121)+ ' Добавляем родительские объекты - ур2' 
--print @msg

--====================================================================
--поиск, добавление уровня 2  по названию листа, 


	--находим имеющиеся
	update ImportNSI_SourceData_Report18Jur
	set 
	HierLev2_ID = Dict_HierLev2.HierLev2_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_HierLev2  on Dict_HierLev2.HierLev1_ID= res.HierLev1_ID and  Dict_HierLev2.StringName = ltrim(rtrim(res.SheetName))
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and res.HierLev1_ID is not null 
	and res.HierLev2_ID is null
	
	declare @maxidH2 int =0

	begin Transaction H2;	
	begin try

		select @maxidH2=max(HierLev2_ID) from Dict_HierLev2
		select @maxidH2= isnull(@maxidH2,0)
		
		--добавляем 
		insert into Dict_HierLev2 (HierLev1_ID, HierLev2_ID, StringName)
		select aa.HierLev1_ID, @maxidH2+ROW_NUMBER() OVER(ORDER BY aa.SheetName ASC),aa.SheetName
		from 
		(
		select distinct  SheetName=ltrim(rtrim(SheetName)), HierLev1_ID
		from ImportNSI_SourceData_Report18Jur 
		where 
		ImportNumber= @LastImportNumber  
		and AllowLoad=1
		and HierLev1_ID is not null
		and HierLev2_ID is null
		) as aa

	end try
	begin catch
		set @maxidH2 =0
	end catch
	commit Transaction H2;

	--повторно обновляем
	update ImportNSI_SourceData_Report18Jur
	set 
	HierLev2_ID = Dict_HierLev2.HierLev2_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_HierLev2  on Dict_HierLev2.HierLev1_ID= res.HierLev1_ID and  Dict_HierLev2.StringName = ltrim(rtrim(res.SheetName))
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and res.HierLev1_ID is not null 
	and res.HierLev2_ID is null

	update ImportNSI_SourceData_Report18Jur
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' не удалось создать уровень 2 (лист);'
	where 
	ImportNumber= @LastImportNumber  
	and AllowLoad=1
	and HierLev2_ID is null

	
	set @msg=convert(varchar, getdate(),121)+ ' Добавляем родительские объекты - ур3' 
--print @msg

--====================================================================
--поиск, добавление уровня 3 по  названию ЮЛ

	declare @maxidH3 int =0

	--находим имеющиеся
	update ImportNSI_SourceData_Report18Jur
	set 
	HierLev3_ID = Dict_HierLev3.HierLev3_ID   
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_HierLev3  on Dict_HierLev3.HierLev2_ID= res.HierLev2_ID and  Dict_HierLev3.StringName = ltrim(rtrim(res.JuridicalPersonName))
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and res.HierLev2_ID is not null and res.HierLev3_ID is null
	
	begin Transaction H3;	
	begin try
		
		select @maxidH3=max(HierLev3_ID) from Dict_HierLev3
		select @maxidH3= isnull(@maxidH3,0)


		--добавляем 
		insert into Dict_HierLev3 (HierLev2_ID, HierLev3_ID, StringName)
		select aa.HierLev2_ID, @maxidH3+ROW_NUMBER() OVER(ORDER BY aa.JuridicalPersonName ASC),aa.JuridicalPersonName
		from 
		(
		select distinct  JuridicalPersonName=ltrim(rtrim(JuridicalPersonName)), HierLev2_ID
		from ImportNSI_SourceData_Report18Jur 
		where 
		ImportNumber= @LastImportNumber  
		and AllowLoad=1
		and HierLev2_ID is not null
		and HierLev3_ID is null
		) as aa

	end try
	begin catch
		set @maxidH3 =0
	end catch
	commit Transaction H3;

	--повторно обновляем
	update ImportNSI_SourceData_Report18Jur
	set 
	HierLev3_ID = Dict_HierLev3.HierLev3_ID   
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_HierLev3  on Dict_HierLev3.HierLev2_ID= res.HierLev2_ID and  Dict_HierLev3.StringName = ltrim(rtrim(res.JuridicalPersonName))
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and res.HierLev2_ID is not null and res.HierLev3_ID is null

	update ImportNSI_SourceData_Report18Jur
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' не удалось создать уровень 3 (ЮЛ);'
	where 
	ImportNumber= @LastImportNumber  
	and AllowLoad=1
	and HierLev3_ID is null
 
--====================================================================
--поиск, добавление PS 

	declare @maxidPS int =0

	--находим имеющиеся
	update ImportNSI_SourceData_Report18Jur
	set 
	PS_ID = Dict_PS.PS_ID   
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_PS  on Dict_PS.HierLev3_ID= res.HierLev3_ID and  Dict_PS.StringName = ltrim(rtrim(res.PSName))
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and res.HierLev3_ID is not null and res.PS_ID is null
	
	begin Transaction PS;	
	begin try

		select @maxidPS=max(PS_ID) from Dict_PS
		select @maxidPS= isnull(@maxidPS,0)

		--добавляем 
		insert into Dict_PS (HierLev3_ID, PS_ID,PSProperty,PSVoltage, BalancePSProperty,PSType, StringName)
		select aa.HierLev3_ID, @maxidPS+ROW_NUMBER() OVER(ORDER BY aa.PSName ASC),0,0,0,0, aa.PSName
		from 
		(
		select distinct  PSName=ltrim(rtrim(PSName)), HierLev3_ID
		from ImportNSI_SourceData_Report18Jur 
		where 
		ImportNumber= @LastImportNumber  
		and AllowLoad=1
		and HierLev3_ID is not null
		and PS_ID is null
		) as aa

	end try
	begin catch
		set @maxidPS =0
	end catch
	commit Transaction PS;
	

	--повторно обновляем
	update ImportNSI_SourceData_Report18Jur
	set 
	PS_ID = Dict_PS.PS_ID   
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_PS  on Dict_PS.HierLev3_ID= res.HierLev3_ID and  Dict_PS.StringName = ltrim(rtrim(res.PSName))
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and res.HierLev3_ID is not null and res.PS_ID is null

	update ImportNSI_SourceData_Report18Jur
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' не удалось создать уровень ПС;'
	where 
	ImportNumber= @LastImportNumber  
	and AllowLoad=1
	and PS_ID is null


set @msg=convert(varchar, getdate(),121)+ ' Нашли/добавили уровни 1-4' 
--print @msg

--====================================================================
 



declare @maxid int 

--ДОБАВЛЕНЕ ТИ, отсутствующие в БД (по коду)
BEGIN TRANSACTION; 
 
	select @maxid=max(TI_ID) from Info_TI
	select @maxid= isnull(@maxid,0)
 
	insert into Info_TI (PS_ID, TI_ID, TIType, TIName, EPP_ID, TIATSCode, Commercial, Voltage, 
						AccountType, Deleted, IsCoeffTransformationDisabled, CreateDateTime, CUS_ID) 	
	 select 
		PS_ID, @maxid+ROW_NUMBER() OVER(ORDER BY aa.TIName ASC), TIType, TIName, TIEPPCode, TIATSCode, Commercial=0, Voltage= 0, Accounttype = 0, Deleted= 0, IsCoeffTransformationDisabled=0, CreateDateTime= GETDATE(), CUS_ID=0
	from 
	(
	 select 
	  PS_ID, TIType=isnull(TIType,15), TIName, 
	  a.TIEPPCode, TIATSCode, Commercial=0, Voltage= 0, Accounttype = 0, Deleted= 0, IsCoeffTransformationDisabled=0, CreateDateTime= GETDATE(), CUS_ID=0  
	   from 
	   --берем уникальный код
			(
				select distinct TIEPPCode=isnull(result.TIEPPCode,'') 
				from ImportNSI_SourceData_Report18Jur result
				where    
				result.ImportNumber= @LastImportNumber
				and result.AllowLoad=1  
				and	result.PS_ID is not null
				and result.TI_ID is null
				and isnull(result.TIName,'') <> '' 
				and isnull(result.TIEPPCode,'')<>''  
				and not exists (select top 1 1 from info_TI where isnull(EPP_ID,'') = isnull(result.TIEPPCode,''))
			 ) 
		 as a 
		--добавляем остальные данные - (1 строку берем для исключения дублей ПС/названий ТИ)
		cross apply (select top 1 * from ImportNSI_SourceData_Report18Jur r 
						where    
						r.ImportNumber= @LastImportNumber 
						and r.AllowLoad=1
						and	 r.PS_ID is not null
						and r.TI_ID is null
						and isnull(r.TIName,'') <> ''  
						and	 isnull(r.TIEPPCode,'') = isnull(a.TIEPPCode,'')) as descr
		) 
	as aa
COMMIT;

 --обновляем TI_ID по коду 
update 
ImportNSI_SourceData_Report18Jur
set 
	TI_ID = Info_TI.TI_ID,
	PS_ID= info_TI.PS_ID,
	TIType= info_TI.TIType,
	ResultCode= ResultCode |2
from 
	ImportNSI_SourceData_Report18Jur result, 
	Info_TI 
where  
	result.ImportNumber= @LastImportNumber  
	and result.AllowLoad=1
	and	 result.PS_ID is not null
	and result.TI_ID is null 
	and isnull(result.TIEPPCode,'') <> ''
	and (isnull(Info_TI.EPP_ID,'') = result.TIEPPCode) 
 
set @msg=convert(varchar, getdate(),121)+ ' Добавили ненайденные ТИ' 
--print @msg


--тип ТИ обновляем по найденныйм ТИ (по идее не должен меняться.. но..) 	 	 
update ImportNSI_SourceData_Report18Jur
set  
TIType= info_TI.TIType
from 
ImportNSI_SourceData_Report18Jur result 
join Info_TI on result.TI_ID = info_TI.TI_ID
where 
result.ImportNumber= @LastImportNumber  
and result.AllowLoad=1
and	result.TI_ID is not null
 
 
set @msg=convert(varchar, getdate(),121)+ ' Обновили тип ТИ' 
--print @msg


 --меняем структуру в соотв с последними файлами (только для ветки 18ЮЛ)
 --переносим ТИ (чужие) на ПС указанные в листе 
 update Info_TI
 set PS_ID = res.PS_ID
 from ImportNSI_SourceData_Report18Jur res join Info_TI on res.TI_ID= info_TI.TI_ID
 where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and	 res.TI_ID is not null
 and res.PS_ID<>Info_TI.PS_ID
 and res.TI_ID in (select TI_ID from vw_Dict_Hierarchy where HierLev1_ID=@H1DefaultID)

		
		
set @msg=convert(varchar, getdate(),121)+ ' Перенесли ТИ' 
--print @msg	
		
BEGIN TRANSACTION; 

--удаляем пустые родительские объекты без связей после переноса ( в ветке 18 ЮЛ )
delete from Dict_PS 
where
PS_ID in (select PS_ID from vw_Dict_HierarchyPS where PS_ID is not null and vw_Dict_HierarchyPS.HierLev1_ID=@H1DefaultID  )
and PS_ID not in (select PS_ID from Info_TI)
and PS_ID not in (select PS_ID from Hard_CommChannels where PS_ID is not null)
and PS_ID not in (select PS_ID from Info_Section_List where PS_ID is not null)
and PS_ID not in (select PS_ID from Dict_PS_PowerSupply_PS_List where PS_ID is not null)
and PS_ID not in (select PS_ID from Dict_JuridicalPersons_To_HierLevels where PS_ID is not null)
and PS_ID not in (select PS_ID from Info_Balance_FreeHierarchy_Objects where PS_ID is not null)

COMMIT;



BEGIN TRANSACTION; 

delete  from Dict_HierLev3 
where 
HierLev2_ID in (select   HierLev2_ID from Dict_HierLev2 where  Dict_HierLev2.HierLev1_ID=@H1DefaultID  )
and HierLev3_ID not in  (select HierLev3_ID from Dict_PS where HierLev3_ID is not null)
and HierLev3_ID not in  (select HierLev3_ID from Info_Section_List where HierLev3_ID is not null)
and HierLev3_ID not in  (select HierLev3_ID from Dict_JuridicalPersons_To_HierLevels where HierLev3_ID is not null)
and HierLev3_ID not in  (select HierLev3_ID from Info_Balance_FreeHierarchy_Objects where HierLev3_ID is not null)

COMMIT;



BEGIN TRANSACTION;

delete  from Dict_HierLev2
where 
HierLev1_ID in (select   HierLev1_ID from Dict_HierLev1 where  Dict_HierLev1.HierLev1_ID=@H1DefaultID  )
and HierLev2_ID not in  (select HierLev2_ID from Dict_HierLev3 where HierLev2_ID is not null)
and HierLev2_ID not in  (select HierLev2_ID from Info_Section_List where HierLev2_ID is not null)
and HierLev2_ID not in  (select HierLev2_ID from Dict_JuridicalPersons_To_HierLevels where HierLev2_ID is not null)
and HierLev2_ID not in  (select HierLev2_ID from Info_Balance_FreeHierarchy_Objects where HierLev2_ID is not null)

COMMIT;
 

set @msg=convert(varchar, getdate(),121)+ ' Удалили пустые уровни 1-4' 
--print @msg





--находим номера каналов в соответствии с описанием тарифа
-- 2-х зонный Д-51, Н-61
--3-х зонный П-51, П/П-61, Н-71
--К не знаем  =1


--print 'test set channels '


update ImportNSI_SourceData_Report18Jur
set ChannelType= 1
where
	ImportNumber= @LastImportNumber  
	and	
TI_ID is not null
and ChannelType is null
and (isnull(TariffName ,'') like 'К' )

--2-х зонный Д строго только  Д-51  &  Н-61
update ImportNSI_SourceData_Report18Jur
set ChannelType= 51 
from ImportNSI_SourceData_Report18Jur r
where
	r.ImportNumber= @LastImportNumber  
and	r.TI_ID is not null
and r.ChannelType is null
and isnull(r.TariffName ,'') like 'Д'
and exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where 
	res1.ImportNumber= @LastImportNumber  
	and	res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'Н'  )
and not exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'П%'  )

--2-х зонный Н строго только  Д-51  &  Н-61
update ImportNSI_SourceData_Report18Jur
set ChannelType= 61 
from ImportNSI_SourceData_Report18Jur r
where
	r.ImportNumber= @LastImportNumber  
	and	
r.TI_ID is not null
and r.ChannelType is null
and isnull(r.TariffName ,'') like 'Н' 
and exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where 
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'Д'  )
and not exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'П%'  )




--3-х зонный П-51, П/П-61, Н-71
update ImportNSI_SourceData_Report18Jur
set ChannelType= 51 
from ImportNSI_SourceData_Report18Jur r
where
	r.ImportNumber= @LastImportNumber  
	and	
r.TI_ID is not null
and r.ChannelType is null
and isnull(r.TariffName ,'') like 'П'
and exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where 
	res1.ImportNumber= @LastImportNumber  
	and	res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'Н'  )
and not exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'Д'  )

update ImportNSI_SourceData_Report18Jur
set ChannelType= 61
from ImportNSI_SourceData_Report18Jur r
where
	r.ImportNumber= @LastImportNumber  
	and	
r.TI_ID is not null
and r.ChannelType is null
and isnull(r.TariffName ,'') like 'П%П' 
and exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where 
	res1.ImportNumber= @LastImportNumber  
	and	res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'П'  )
and not exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'Д'  )

update ImportNSI_SourceData_Report18Jur
set ChannelType= 71 
from ImportNSI_SourceData_Report18Jur r
where
	r.ImportNumber= @LastImportNumber  
	and	
r.TI_ID is not null
and r.ChannelType is null
and isnull(r.TariffName ,'') like 'Н' 
and exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where 
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'П%'  )
and not exists (select top 1 1 from ImportNSI_SourceData_Report18Jur res1 where
	res1.ImportNumber= @LastImportNumber  
	and	 res1.TI_ID is not null and res1.TI_ID =r.TI_ID and isnull(res1.TariffName ,'') like 'Д'  )


update ImportNSI_SourceData_Report18Jur
set AllowLoad=0, ErrorMessage=ErrorMessage+' некорректный тариф (не определен номер канала);'
where 
ImportNumber= @LastImportNumber
and AllowLoad=1
and ChannelType is null

	




--Добавляем производителя для такого импорта
--искать сначала будем по названию в других производителях, если не нашли - будем добавлять в нет данных..
--ЕСЛИ ПУ на другой ТИ, то не будем доабвлять.. это может быть наша Ти 

--ЕСЛИ НЕТ КОДА НЕ ЗАГРУЖАЕМ ТИ!! и ВСЕ!!!
--ПУ ПОТОМ НА НЕЙ ПОМЕНЯЕТСЯ.. как найдем.. по ПУ



set @msg=convert(varchar, getdate(),121)+ ' Добавление производителя ПУ' 
--print @msg


declare @MeterTypeID_NoData int
 
BEGIN TRANSACTION; 

select 
@MeterTypeID_NoData=MeterType_ID
from Dict_Meters_Types
where 
MeterType_ID>=10000
and MeterTypeName like 'нет данных'

	if (@MeterTypeID_NoData is null)
	begin

		select @MeterTypeID_NoData= max(MeterType_ID)
		from Dict_Meters_Types
		where 
		MeterType_ID>=10000

		set @MeterTypeID_NoData= isnull(@MeterTypeID_NoData,10000)+1
		
		insert into Dict_Meters_Types(MeterType_ID, MeterTypeName)
		values(@MeterTypeID_NoData,'нет данных')
	end

COMMIT;


set @msg=convert(varchar, getdate(),121)+ ' Добавлены/найдены производители ПУ' 
--print @msg


 --МОДЕЛЬ ПУ  
begin

 	--находим имеющихя производителей
	--ставим производителя 1999 всегда тк не указывают
	update ImportNSI_SourceData_Report18Jur
	set 
	MeterType_ID = Dict_Meters_Types.MeterType_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_Meters_Types  on Dict_Meters_Types.MeterTypeName= res.Manufacturer 
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and	res.TI_ID is not null	
	and res.MeterType_ID is null
	and res.Manufacturer is not null


	update ImportNSI_SourceData_Report18Jur
	set 
	MeterType_ID = @MeterTypeID_NoData
	from
	ImportNSI_SourceData_Report18Jur res	
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and	res.TI_ID is not null	
	and res.MeterType_ID is null 

 
 	--находим имеющеся модели и по производителю
	update ImportNSI_SourceData_Report18Jur
	set 
	MeterModel_ID = Dict_Meters_Model.MeterModel_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_Meters_Model on  res.MeterType_ID=Dict_Meters_Model.MeterType_ID and  Dict_Meters_Model.StringName= res.Model 
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1
	and	res.TI_ID is not null	
	and res.MeterModel_ID is null
	and res.MeterType_ID is not null
 
  	--находим просто по модели первый попавшийся
	update ImportNSI_SourceData_Report18Jur
	set 
	MeterModel_ID = Dict_Meters_Model.MeterModel_ID,
	MeterType_ID = Dict_Meters_Model.MeterType_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_Meters_Model on  Dict_Meters_Model.StringName = isnull(res.Model ,'')
	where
	res.ImportNumber= @LastImportNumber
	and res.AllowLoad=1  
	and	res.TI_ID is not null	
	and res.MeterModel_ID is null 

	begin transaction

	set @maxid=0
	select @maxid=max(MeterModel_ID) from Dict_Meters_Model
	select @maxid= isnull(@maxid,0)

	--добавляем 
	insert into Dict_Meters_Model (MeterType_ID,MeterModel_ID,StringName )
	select MeterType_ID, @maxid+ROW_NUMBER() OVER(ORDER BY aa.Model ASC), aa.Model
	from 
	(
		select distinct  MeterType_ID= isnull(MeterType_ID,@MeterTypeID_NoData), Model
		from ImportNSI_SourceData_Report18Jur
		where  
		ImportNumber= @LastImportNumber  
		and AllowLoad=1  
		and	TI_ID is not null	
		and MeterModel_ID is null and isnull(Model,'')<>''
	) as aa
	commit;

	--повторно обновляем
	update ImportNSI_SourceData_Report18Jur
	set 
	MeterModel_ID = Dict_Meters_Model.MeterModel_ID,
	MeterType_ID = Dict_Meters_Model.MeterType_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join Dict_Meters_Model on  Dict_Meters_Model.StringName= res.Model 
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1  
	and	res.TI_ID is not null	
	and res.MeterModel_ID is null
	and isnull(res.MeterType_ID, @MeterTypeID_NoData)=Dict_Meters_Model.MeterType_ID
	
	--hard_meters 
	update ImportNSI_SourceData_Report18Jur
	set 
	Meter_ID= hard_meters.Meter_ID  
	from
	ImportNSI_SourceData_Report18Jur res
	join hard_meters on  hard_meters.MeterModel_ID=res.MeterModel_ID and  hard_meters.MeterSerialNumber= res.SerialNumber 
	where
	res.ImportNumber= @LastImportNumber  
	and res.AllowLoad=1  
	and	res.TI_ID is not null	
	and res.Meter_ID is null
	and res.MeterModel_ID is not null
	and isnull( res.SerialNumber,'')<>''
	and len(res.SerialNumber)>3

	--добавляем отутствующие

	begin transaction
	set @maxid=0
	select @maxid=max(Meter_ID) from hard_meters
	select @maxid= isnull(@maxid,0)
	  

	--добавляем 
	insert into hard_meters 
		(Meter_ID,MeterModel_ID, MeterType_ID , MeterSerialNumber, LinkNumber,IsAlien,LoadControl,OutageReport,ReadRequest,RelayCapable,
		AllowTariffWrite,CreateDateTime,CUS_ID, TimeDiffHours )
	select 
		@maxid+ROW_NUMBER() OVER(ORDER BY aa.MeterSerialNumber ASC), MeterModel_ID, MeterType_ID , MeterSerialNumber, LinkNumber,IsAlien,LoadControl,OutageReport,ReadRequest,RelayCapable,
		AllowTariffWrite,CreateDateTime,CUS_ID, TimeDiffHours
	from 
	(
	select distinct MeterModel_ID, MeterType_ID , MeterSerialNumber=SerialNumber, LinkNumber='',IsAlien= 1,LoadControl=0,OutageReport=0,ReadRequest=0,RelayCapable=0,
	AllowTariffWrite=0,CreateDateTime = getdate(),CUS_ID=0, TimeDiffHours=0
	from ImportNSI_SourceData_Report18Jur
	where 
	ImportNumber= @LastImportNumber  
	and AllowLoad=1 
	and TI_ID is not null	
	and Meter_ID is null and MeterModel_ID is not null and MeterType_ID is not null
	 and SerialNumber is not null
	 and len(SerialNumber)>3
	) as aa

	commit;

	--повторно обновляем
	update ImportNSI_SourceData_Report18Jur
	set 
	Meter_ID= hard_meters.Meter_ID , MeterType_ID = hard_meters.MeterType_ID, MeterModel_ID =hard_meters.MeterModel_ID
	from
	ImportNSI_SourceData_Report18Jur res
	join hard_meters on  hard_meters.MeterModel_ID=res.MeterModel_ID and  isnull(hard_meters.MeterSerialNumber,'')= isnull(res.SerialNumber ,'')
	where
	res.ImportNumber= @LastImportNumber   
	and res.AllowLoad=1 
	and	res.TI_ID is not null	
	and res.Meter_ID is null
	and res.MeterModel_ID is not null
	and isnull( res.SerialNumber,'')<>''
	and len(res.SerialNumber)>3

end


set @msg=convert(varchar, getdate(),121)+ ' Добавили/нашли ПУ' 
--print @msg


update ImportNSI_SourceData_Report18Jur
set AllowLoad=0, ErrorMessage = ErrorMessage+' ТИ не создана;'
where 
ImportNumber= @LastImportNumber
and AllowLoad<>0 
and TI_ID is null


--счетчик может быть не указана (тогда это 1 ТИ без ПУ)
--update ImportNSI_SourceData_Report18Jur
--set AllowLoad=0, ErrorMessage = ErrorMessage+' ПУ не создан;'
--where 
--ImportNumber= @LastImportNumber
--and AllowLoad<>0 
--and Meter_ID is null



--==== ЗАМЕНЫ ПУ, ТР

set @msg=convert(varchar, getdate(),121)+ ' =========== ЗАМЕНЫ ПУ, ТР ====================' 
--print @msg


create table #tempImportOEK_Jur18 
(
SheetName nvarchar(200),
MinRowID int, 
MetersCount int,
RowNumber int,
TI_ID int, 
METER_ID int ,
IMportNumber varchar(100),
EventDate datetime, 
TariffName varchar(100), 
ChannelType tinyint, 
TIEPPCode varchar(100), 
SerialNumber  varchar(100), 
TRCoeff float,
IntegralValue_Previous float,
IntegralValue_Current float
) 
create index IX_tempImportOEK_Jur18_1 on #tempImportOEK_Jur18 (ImportnUmber,SheetName, TIEPPCode, TariffName, EventDate)
 


insert into #tempImportOEK_Jur18
(
RowNumber ,
SheetName ,
MinRowID , 
TI_ID , 
METER_ID  ,
IMportNumber ,
EventDate , 
TariffName ,
ChannelType, 
TIEPPCode ,
SerialNumber  ,
TRCoeff,
IntegralValue_Previous ,
IntegralValue_Current 
)

select 
ROW_NUMBER() OVER(PARTITION BY 
IMportNumber,
SheetName,
TIEPPCode,
EventDate, 
TariffName
  ORDER BY IMportNumber,
  SheetName,
TIEPPCode,
EventDate, 
TariffName,
MinRowID ) 
AS PartNumber,
tempGroupResult.*
from
(

select distinct 
SheetName,
MinRowID=min(RowID), 
TI_ID, 
METER_ID,
IMportNumber,
EventDate, 
TariffName, 
ChannelType,
TIEPPCode, 
SerialNumber, 
TRCoeff,
IntegralValue_Previous,
IntegralValue_Current
from ImportNSI_SourceData_Report18Jur
where 
ImportNumber = @LastImportNumber
and AllowLoad=1
group by 
IMportNumber,
SheetName,
EventDate, 
TariffName, 
ChannelType,
TIEPPCode, 
TI_ID, METER_ID,
SerialNumber, 
TRCoeff,
IntegralValue_Previous,
IntegralValue_Current
) as tempGroupResult 

--количество ПУ в периоде по тарифу+зав номер считаем

--считаем количество ПУ
--считаем количество тарифов для этих ПУ....
update #tempImportOEK_Jur18
set MetersCount=mcount.MetersCount
from 
#tempImportOEK_Jur18 
a cross apply (select MetersCount=count (1) from #tempImportOEK_Jur18 b
				 where 
					a.IMportNumber = b.IMportNumber
					and a.SheetName=b.SheetName
					and a.TIEPPCode = b.TIEPPCode
					and a.TariffName = b.TariffName
					and a.EventDate = b.EventDate 					 
					)  as  mCount
	

--select * from #tempImportOEK_Jur18
--where TI_ID in (73513658,73488011,73588172,73491996,73492215,73497511)
--order by TI_ID, RowNumber

--drop table #tempImportOEK_Jur18
 


set @msg=convert(varchar, getdate(),121)+ ' ImportNSI_SourceData_Report18Jur - EventDatePrevious,EventDateCurrent' 
--print @msg
 

 

update ImportNSI_SourceData_Report18Jur
set MetersCount=0
where 
ImportNumber= @LastImportNumber

update ImportNSI_SourceData_Report18Jur
set  
MetersCount= temp.MetersCount,
--дата предыдущих показаний - начало месяца
EventDatePrevious= case 
					when temp.MetersCount=1 then temp.EventDate 
					when temp.RowNumber>=1 and temp.RowNUmber<=temp.MetersCount 
						then DATEADD(d, (temp.RowNumber-1)*(DATEDIFF(d, temp.EventDate, dateadd(m,1,temp.EventDate))/temp.MetersCount), temp.EventDate)
					else temp.EventDate end,
--дата текущих показания (конец месяца)
EventDateCurrent= dateadd(S,-1,  
				  case 
					when temp.MetersCount=1 then dateadd(month,1,temp.EventDate )
				    when temp.RowNumber>=1 and temp.RowNUmber<=temp.MetersCount 
						then DATEADD(d, (temp.RowNumber)*(DATEDIFF(d, temp.EventDate, dateadd(m,1,temp.EventDate))/temp.MetersCount), temp.EventDate)
					else dateadd(month,1,temp.EventDate ) end)
from 
ImportNSI_SourceData_Report18Jur 
, #tempImportOEK_Jur18 temp
where 
ImportNSI_SourceData_Report18Jur.ImportNumber= @LastImportNumber
and ImportNSI_SourceData_Report18Jur.ImportNumber= temp.ImportNumber
and ImportNSI_SourceData_Report18Jur.EventDate= temp.EventDate
and ImportNSI_SourceData_Report18Jur.SheetName = temp.SheetName
and ImportNSI_SourceData_Report18Jur.TIEPPCode= temp.TIEPPCode
and ImportNSI_SourceData_Report18Jur.RowID= temp.MinRowID

 


drop table #tempImportOEK_Jur18


--ошибка если больше 3х замен в месяце
update ImportNSI_SourceData_Report18Jur 
set 
AllowLoad=0, ErrorMessage=ErrorMessage+' больше 3х замен ПУ;'
where 
ImportNumber=@LastImportNumber
and AllowLoad=1 -- берем только оставшиеся корректные
and (MetersCount>3)

--пример - ТИ с кодом 979171705980872 для ЮЗГО - тариф Н - 3 записи, тариф П- 2, тариф П/П  -3 в итоге создаются 3 и 2 замены ПУ соответственно и они конфликтуют друг с другом...
update ImportNSI_SourceData_Report18Jur 
set 
AllowLoad=0, ErrorMessage=ErrorMessage+' некорректное (разное) количество тарифов;'
from ImportNSI_SourceData_Report18Jur a
where 
a.ImportNumber=@LastImportNumber
and a.AllowLoad=1 -- берем только оставшиеся корректные
and exists (select top 1 1 from ImportNSI_SourceData_Report18Jur b where a.ImportNumber=b.ImportNumber and a.RowID<>b.RowID and a.MetersCount<> b.MetersCount and b.AllowLoad=1 and a.TI_ID= b.TI_ID)



   update ImportNSI_SourceData_Report18Jur
   set AllowLoad=0, ErrorMessage= ErrorMessage+' TI_ID='+convert(varchar(200),TI_ID)+' не определена дата начала периода (дубли?);'
   where 
   ImportNumber=@LastImportNumber
	and AllowLoad=1
	and EventDatePrevious is null

	   update ImportNSI_SourceData_Report18Jur
   set AllowLoad=0, ErrorMessage= ErrorMessage+' TI_ID='+convert(varchar(200),TI_ID)+' не определена дата завершения периода (дубли?);'
   where 
   ImportNumber=@LastImportNumber
	and AllowLoad=1
	and EventDateCurrent is null

--счетчики могут бытб не указаны, но все равно если указана ТИ то 
--чистим для нее периоды действия и показания..
--а также коэфф тр..
--Только для ТИ с уровня 18ЮЛ!!!


--строго для дерева 18ЮЛ


--ограничиваем предыдущие замены (если имеются) первым числом
--считаем что замены ПУ оформлены корректно
--показания на момент замены не трогаем


update ImportNSI_SourceData_Report18Jur 
set 
AllowLoad=0, ErrorMessage= ErrorMessage+ ' дубль'
where ImportNumber= @LastImportNumber
				and AllowLoad = 1 
				and TI_ID is not null
				and Meter_ID is not null
				and EventDateCurrent is null
				and EventDatePrevious is null
				and HierLev1_ID = @H1DefaultID 

set @msg=convert(varchar, getdate(),121)+ ' update   Info_Meters_TO_TI set FinishDateTime= DATEADD(s,-1,@EventDateTime)' 
--print @msg
 
update  
Info_Meters_TO_TI
set FinishDateTime= DATEADD(s,-1,@EventDateTime)
where 
TI_ID in (select TI_ID from ImportNSI_SourceData_Report18Jur 
			where ImportNumber= @LastImportNumber
				and AllowLoad = 1 
				and TI_ID is not null
				and HierLev1_ID = @H1DefaultID 
		 )
and StartDateTime<@EventDateTime
and (FinishDateTime is null or FinishDateTime>=@EventDateTime)
 
set @msg=convert(varchar, getdate(),121)+ ' delete from Info_Meters_TO_TI' 
--print @msg





--удаляем (заменяем) замены ПУ в текущем периоде (которые по текущей схеме создаются - на каждый месяц)
delete from Info_Meters_TO_TI
where 
TI_ID in (select TI_ID from ImportNSI_SourceData_Report18Jur 
			where ImportNumber= @LastImportNumber
				and AllowLoad = 1 
				and TI_ID is not null
				and HierLev1_ID = @H1DefaultID 
		 )
and 
	(
	(StartDateTime>=@EventDateTime and (StartDateTime <dateadd(MONTH,1,@EventDateTime)))
	or 
	(FinishDateTime is not null and FinishDateTime>=@EventDateTime and (FinishDateTime <dateadd(MONTH,1,@EventDateTime)))
	)
--историю замен пока создавать не будем? тк там источников.. возможно и без них нормально отрабатыаются замены.. 
--если нет то потом скрипт придумаем..
--еще ведь не понятно какие показания какого ПУ у нас в Ексель.. старого/нового или только нового?

 
set @msg=convert(varchar, getdate(),121)+ ' insert Info_Meters_TO_TI' 
--print @msg

insert into Info_Meters_TO_TI  (TI_ID,
METER_ID,
StartDateTime,
FinishDateTime,
MetersReplaceSession_ID,
CUS_ID,
SourceType)
select TI_ID, METER_ID, EventDatePrevious, EventDateCurrent ,newid() ,0,0 
from 
(
select distinct  TI_ID, METER_ID, EventDatePrevious, EventDateCurrent 
from ImportNSI_SourceData_Report18Jur
where 
ImportNumber= @LastImportNumber
and AllowLoad=1
and Meter_ID is not null
and TI_ID is not null
and HierLev1_ID=@H1DefaultID
) as meters




--не загружаем СУММУ если она равна 0 на этом периоде и есть тарифные показания не равные ноль
--перед коэфф делаем эту проверку..

set @msg=convert(varchar, getdate(),121)+ ' не загружаем СУММУ если она равна 0 на этом периоде и есть тарифные показания не равные ноль' 
--print @msg
update ImportNSI_SourceData_Report18Jur
set AllowLoad=0, ErrorMessage=ErrorMessage+' (предыд) сумма =0, но есть тарифные каналы - не загружаем; '
from ImportNSI_SourceData_Report18Jur a where 
			a.ImportNumber= @LastImportNumber  
			--and a.AllowLoad=1
			and a.HierLev1_ID = @H1DefaultID
			and isnull(a.IntegralValue_Previous,0)=0
			and a.ChannelType=1
			and a.EventDatePrevious is not null
			and exists (select top 1 1 from  ImportNSI_SourceData_Report18Jur b where 
									a.ImportNumber= b.ImportNumber 
									--and b.AllowLoad=1 
									and b.TI_ID = a.TI_ID 
									and b.EventDatePrevious = a.EventDatePrevious									
									and b.ChannelType<>1
									and isnull(b.IntegralValue_Previous,0)<>0									
									)

update ImportNSI_SourceData_Report18Jur
set AllowLoad=0, ErrorMessage=ErrorMessage+' (текущие) сумма =0, но есть тарифные каналы - не загружаем; '
from ImportNSI_SourceData_Report18Jur a where 
			a.ImportNumber= @LastImportNumber  
			--and a.AllowLoad=1
			and a.HierLev1_ID = @H1DefaultID
			and isnull(a.IntegralValue_Current,0)=0
			and a.ChannelType=1
			and a.EventDatePrevious is not null
			and exists (select top 1 1 from  ImportNSI_SourceData_Report18Jur b where 
									a.ImportNumber= b.ImportNumber 
									--and b.AllowLoad=1 
									and b.TI_ID = a.TI_ID 
									and b.EventDatePrevious = a.EventDatePrevious									
									and b.ChannelType<>1
									and isnull(b.IntegralValue_Current,0)<>0									
									)


--=================коэфф ТР!!==============================

-- имопрт плобмб+ТР   будет конфликтовать но...
  
--каждый месяц перезагружаем, если поменялся
if (isnull(@AllowLoadTRCoeff,0) =1)
begin
   

   begin transaction;
   
   --БЫВАЮТ КРИВЫЕ КОЭФФИЦИЕНТЫ - например для тарифных показаний НА ОДНОМ периоде один коэфф, а для суммы другой.. 
   --поэтому берем первый попавшийся.. либо пишем ошибку!!!

   update ImportNSI_SourceData_Report18Jur
   set AllowLoad=0, ErrorMessage= ErrorMessage+' TI_ID='+convert(varchar(200),TI_ID)+' дубли коэфф Трансформации;'
  where ImportNumber= @LastImportNumber
				and AllowLoad = 1 
				and TI_ID in
				(
				   select distinct a.TI_ID from
				   (
				   select TI_ID, EventDatePrevious, EventDateCurrent,trCount= count(distinct TRCOeff) from ImportNSI_SourceData_Report18Jur 
				   where
					ImportNumber=@LastImportNumber
					and AllowLoad=1
					group by TI_ID, EventDatePrevious, EventDateCurrent
					having count(distinct TRCOeff)>1
					) as a
					)
  	
	set @msg=convert(varchar, getdate(),121)+ ' update   Info_Transformators set FinishDateTime= 2100'
	--print @msg

	--ограничиваем предыдущие коэфф первым числом месяца (если не совпадают)
	update  
	Info_Transformators
	set FinishDateTime= DATEADD(s,-1,@EventDateTime)
	where 
	TI_ID in (select ImportNSI_SourceData_Report18Jur.TI_ID
				from ImportNSI_SourceData_Report18Jur 
				where 
				ImportNumber= @LastImportNumber
				and AllowLoad = 1 
				and ImportNSI_SourceData_Report18Jur.TI_ID is not null
				--and HierLev1_ID = @H1DefaultID 
				and ImportNSI_SourceData_Report18Jur.TRCoeff is not null
				and abs((Info_Transformators.COEFU*Info_Transformators.COEFI)-isnull(ImportNSI_SourceData_Report18Jur.TRCoeff,1))>0.000000001 
			 )
	and StartDateTime<@EventDateTime --начало < 01 числа
	and (FinishDateTime is null or FinishDateTime>=@EventDateTime) --но завершение больше 01 числа
	

		 		  	
	set @msg=convert(varchar, getdate(),121)+ ' delete from Info_Transformators'
	--print @msg




	--удаляем оставшиеся (?) коэффициенты текущего месяца (будем заменять)  
	-- (если не совпадают) (т.к. их может быть два если была замена)
	delete from
	Info_Transformators
	where 
	TI_ID in (select ImportNSI_SourceData_Report18Jur.TI_ID
					from ImportNSI_SourceData_Report18Jur 
					where 
					ImportNumber= @LastImportNumber
					and AllowLoad = 1 
					and ImportNSI_SourceData_Report18Jur.TI_ID is not null
					--and HierLev1_ID = @H1DefaultID 
					and ImportNSI_SourceData_Report18Jur.TRCoeff is not null
					and abs((Info_Transformators.COEFU*Info_Transformators.COEFI)-isnull(ImportNSI_SourceData_Report18Jur.TRCoeff,1))>0.000000001 
				 )
	and 
	(
	(StartDateTime>=@EventDateTime and (StartDateTime <dateadd(MONTH,1,@EventDateTime)))
	or 
	(FinishDateTime is not null and FinishDateTime>=@EventDateTime and (FinishDateTime <dateadd(MONTH,1,@EventDateTime)))
	)
	
	 
	set @msg=convert(varchar, getdate(),121)+ ' insert Info_Transformators'
	--print @msg

	 insert into Info_Transformators (TI_ID,
	StartDateTime,
	FinishDateTime,
	COEFU,
	CoefUHigh,
	CoefULow,
	COEFI,
	CoefIHigh,
	CoefILow,
	CUS_ID,
	UseBusSystem)
	
	select distinct  TI_ID,  EventDatePrevious, EventDateCurrent , 
	1,1 ,1,
	case when isnull(TRCoeff,1)<=1 then 1 else TRCoeff end,
	case when isnull(TRCoeff,1)<=1 then 1 else TRCoeff end,1,
	0,0

	from ImportNSI_SourceData_Report18Jur
	where 
	ImportNumber= @LastImportNumber
	and AllowLoad=1 
	and TI_ID is not null 
	and not exists (
					--и нет коэффициентов в этом месяце...			
					select top 1 1 from Info_Transformators tr 
					where tr.TI_ID = ImportNSI_SourceData_Report18Jur.TI_ID 
					and 
						(
							(StartDateTime between @EventDateTime and dateadd(s,-1,dateadd(MONTH,1,@EventDateTime)))
							or 
							@EventDateTime between StartDateTime  and (isnull(FinishDateTime,'01-01-2100') )
						)
					)


	--ограничиваем предыдущие коэфф первым числом месяца (если не совпадают)
	--еще раз так как добавиться мог период впереди.. например SerialNumber like  '15616121'
	update  
	Info_Transformators
	set FinishDateTime= DATEADD(s,-1,@EventDateTime)
	where 
	TI_ID in (select ImportNSI_SourceData_Report18Jur.TI_ID
				from ImportNSI_SourceData_Report18Jur 
				where 
				ImportNumber= @LastImportNumber
				and AllowLoad = 1 
				and ImportNSI_SourceData_Report18Jur.TI_ID is not null
				--and HierLev1_ID = @H1DefaultID 
				and ImportNSI_SourceData_Report18Jur.TRCoeff is not null
				--and abs((Info_Transformators.COEFU*Info_Transformators.COEFI)-isnull(ImportNSI_SourceData_Report18Jur.TRCoeff,1))>0.000000001 
			 )
	and StartDateTime<@EventDateTime --начало < 01 числа
	and (FinishDateTime is null or FinishDateTime>=@EventDateTime) --но завершение больше 01 числа
	


	--расширяем последний для каждой из загружаемых ТИ период до 2100 года.
	update Info_Transformators
	set FinishDateTime= '01-01-2100'
	from
	Info_Transformators a
	where 
	a.TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)
	and not exists (select top 1 1 from Info_Transformators b where b.TI_ID = a.TI_ID and b.StartDateTime>a.StartDateTime )
	and a.FinishDateTime is not null
	and a.FinishDateTime < '01-01-2100'


	--и надо будет сделать скрипт объединяющий одинаковые диапазоны (если между ними 1 сек!!)
	--(выбираем 2 записи min и max у которых даныне одинаковы и между ними нет других записей)
	--далее макс запись удаляем а мин запись расширяем до максимальной!!
	 
	 commit transaction;

end
 


--ДАННЫЕ - загружаем всегда за искл случаев когда грузится только дерево
IF (@FreeHierarchyTypeLoad is  null or  @FreeHierarchyTypeLoad<>2)
BEGIN

	if (isnull(@AllowLoadAddress,0) =1)
	begin
  
	   --исключаем пустые адреса чтобы не плодить привязок к верхнеум уровню в адресной структуре
		 update ImportNSI_SourceData_Report18Jur 
		 set Address ='' where 
		ImportNumber= @LastImportNumber
		and AllowLoad=1   
		and	
		 Address is not null 
		 and ((Address like '%москва%'  and len(Address)<8) or (Address not like '%москва%'  and len(Address)<5))

	  
		select @maxid=max(FullAddress_ID) from FIAS_FullAddressToHierarchy
		select @maxid= isnull(@maxid,0)
	 	
		
		--добавляем  отсутствующие из таблицы с пользовательскими адресами
		insert into FIAS_FullAddressToHierarchy (FullAddress_ID,AOGUID,TI_ID)
		select FullAddress_ID=@maxid+ROW_NUMBER() OVER(ORDER BY addrimported.TI_ID ASC), addrimported.AOGUID, addrimported.TI_ID
		from (
			select distinct 
				AOGUID= ImportNSI_UserAddress_To_FIAS.FIASCode  ,
				TI_ID=res.TI_ID
				from 
				ImportNSI_SourceData_Report18Jur  res
				join ImportNSI_UserAddress_To_FIAS 
					on 
					--пока не используем группировку так как все листы разные res.SheetName = ImportNSI_UserAddress_To_FIAS.GroupName and
					 res.Address = ImportNSI_UserAddress_To_FIAS.UserFullAddress
				where 
				ImportNumber= @LastImportNumber  
				and res.AllowLoad=1
				and		res.TI_ID is not null
				and res.TI_ID not in (select TI_ID from FIAS_FullAddressToHierarchy where TI_ID is not null)
				and res.SheetName is not null
				and isnull(res.Address,'') <>''
				and ImportNSI_UserAddress_To_FIAS.GroupName is not null
				and ImportNSI_UserAddress_To_FIAS.FIASCode is not null
				and ImportNSI_UserAddress_To_FIAS.FIASCode in (Select FIAS_FullAddress.AOGUID from FIAS_FullAddress)
			) as addrimported
		 
	end

	set @msg=convert(varchar, getdate(),121)+ 'удаляем показания за месяц с этого источника для загружаемых ТИ' 
	--print @msg

	--удаляем все источники
	--в 1 и 5 зранилище
	delete from 
	ArchCalcBit_Integrals_Virtual_1
	where 
	TI_ID in (select TI_ID from ImportNSI_SourceData_Report18Jur where 
				ImportNumber= @LastImportNumber  
				and AllowLoad=1
				and HierLev1_ID = @H1DefaultID
				and	TI_ID is not null
				and TIType =11)
	and EventDateTime>= @EventDateTime
	and EventDateTime<DATEADD(month,1,@EventDateTime)
	and DataSource_ID = @DataSourceType

	delete from 
	ArchCalcBit_Integrals_Virtual_5
	where 
	TI_ID in (select TI_ID from ImportNSI_SourceData_Report18Jur where 
				ImportNumber= @LastImportNumber  
				and AllowLoad=1
				and HierLev1_ID = @H1DefaultID
				and	TI_ID is not null
				and TIType =15)
	and EventDateTime>= @EventDateTime
	and EventDateTime<DATEADD(month,1,@EventDateTime)
	and DataSource_ID = @DataSourceType

	set @msg=convert(varchar, getdate(),121)+ ' выборка предыдущих показаний' 
	--print @msg

	--возможны дубли ТИ (ТИ нашлась по коду или номеру ПУ.. а показания разные) ???
	--импорт интегралов по TI_ID!
	declare @ImportedTable ImportedInegralValueTableType
	insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, 
	TI_ID, EventDateTime, ChannelType, DataSourceType, Data)

	select ROW_NUMBER() OVER(ORDER BY rr.TI_ID ASC), 0, null, null, 
	rr.TI_ID, EventDatePrevious, rr.ChannelType, @DataSourceType, null from 
	 (
		select distinct
		 TI_ID, EventDatePrevious, ChannelType 
		from ImportNSI_SourceData_Report18Jur res
		where 
		res.ImportNumber= @LastImportNumber
		and res.AllowLoad=1  
		and	TI_ID is not null
		and res.ChannelType is  not null
		and res.IntegralValue_Previous is not null
		and res.EventDatePrevious is not null
	 
	) as rr

	--теперь update  (так как могут быть дубли и они могут толичаться 0 берем первое попавшееся значение)
	update @ImportedTable
	set Data=IntegralValue_Previous*isnull(Coeff,1)
	from 
	ImportNSI_SourceData_Report18Jur res
	 join @ImportedTable temp on res.ti_ID= temp.TI_ID and res.ChannelType= temp.ChannelType and res.EventDatePrevious = temp.EventDateTime
	where 
	res.ImportNumber= @LastImportNumber
		and res.AllowLoad=1  
		and	res.TI_ID is not null
		and res.ChannelType is  not null
		and res.IntegralValue_Previous is not null
		and res.EventDatePrevious is not null
	
	declare @maxRowNumber int
	select @maxRowNumber = max(RowNumber) from @ImportedTable

	 --а теперь тупо суммируем все тарифные каналы по ТИ если нет суммы
	 --МОЖЕТ БЫТЬ ДОБАВИТЬ ПРОВЕРКУ НА наличие ВСЕХ требуемых каналов?
	 insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, TI_ID, EventDateTime, ChannelType, DataSourceType, Data)
	 select @maxRowNumber+ROW_NUMBER() OVER(ORDER BY summChannel.TI_ID ASC),summChannel.* from 
	 (
	select   ResultStatus,Code=null,Meter_ID= null, TI_ID, EventDateTime,ChannelType= 1, DataSourceType,Data= Sum([data]) 
	from 
	@ImportedTable r0
	where 
	ChannelType <>1 
				and not exists (
				select top 1 1 from 
				@ImportedTable r1
				where 
				r1.TI_ID = r0.TI_ID 
				and r1.ChannelType =1
				and r1.EventDateTime = r0.EventDateTime
				and r1.DataSourceType = r0.DataSourceType 			
				and r1.RowNumber<>r0.RowNumber)
	group by  ResultStatus,Code, Meter_ID, TI_ID, EventDateTime,   DataSourceType 
	) 
	as summChannel

	set @msg=convert(varchar, getdate(),121)+ ' update ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	update ArchCalcBit_Integrals_Virtual_1 
	set Data= Source.Data, DispatchDateTime = getdate()
	from 
	ArchCalcBit_Integrals_Virtual_1 Target join @ImportedTable Source on 
	Target.TI_ID= Source.TI_ID and Target.ChannelType= Source.ChannelType and Target.EventDateTime= Source.EventDateTime and Target.DataSource_ID= Source.DataSourceType
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11

	set @msg=convert(varchar, getdate(),121)+ ' insert ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	insert 
	into ArchCalcBit_Integrals_Virtual_1
	(TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)


	select distinct
	Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSourceType,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0
	from  @ImportedTable Source 
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11
	and not exists
	(select top 1 1 from ArchCalcBit_Integrals_Virtual_1 Target 
	where 
	Target.TI_ID= Source.TI_ID 
	and Target.ChannelType= Source.ChannelType 
	and Target.EventDateTime= Source.EventDateTime 
	and Target.DataSource_ID= Source.DataSourceType
	)
   
	set @msg=convert(varchar, getdate(),121)+ ' update ArchCalcBit_Integrals_Virtual_5' 
	--print @msg

	update ArchCalcBit_Integrals_Virtual_5 
	set Data= Source.Data, DispatchDateTime = getdate()
	from 
	ArchCalcBit_Integrals_Virtual_5 Target 
	join @ImportedTable Source on 
	Target.TI_ID= Source.TI_ID and Target.ChannelType= Source.ChannelType and Target.EventDateTime= Source.EventDateTime and Target.DataSource_ID= Source.DataSourceType
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=15

	set @msg=convert(varchar, getdate(),121)+ ' insert ArchCalcBit_Integrals_Virtual_5' 
	--print @msg
	insert into ArchCalcBit_Integrals_Virtual_5
	(TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	select distinct
	Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSourceType,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0
	from  @ImportedTable Source 
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where		
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=15
	and not exists
	(select top 1 1 from ArchCalcBit_Integrals_Virtual_5 Target 
	where 
	Target.TI_ID= Source.TI_ID 
	and Target.ChannelType= Source.ChannelType 
	and Target.EventDateTime= Source.EventDateTime 
	and Target.DataSource_ID= Source.DataSourceType
	)

	set @msg=convert(varchar, getdate(),121)+ ' Импортировали показания предыдущие' 
	--print @msg

	set @msg=convert(varchar, getdate(),121)+ ' выборка текущих показаний' 
	--print @msg

	delete from @ImportedTable

	--текущие
	insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, 
	TI_ID, EventDateTime, ChannelType, DataSourceType, Data)

	select ROW_NUMBER() OVER(ORDER BY rr.TI_ID ASC), 0, null, null, 
	rr.TI_ID, EventDateCurrent, rr.ChannelType, @DataSourceType, null from 
	 (
		select distinct
		 TI_ID, EventDateCurrent, ChannelType 
		from ImportNSI_SourceData_Report18Jur res
		where 
		res.ImportNumber= @LastImportNumber
		and res.AllowLoad=1  
		and	TI_ID is not null
		and res.ChannelType is  not null
		and res.IntegralValue_Current is not null
		and res.EventDateCurrent is not null
	 
	) as rr

	--теперь update  (так как могут быть дубли и они могут толичаться 0 берем первое попавшееся значение)
	update @ImportedTable
	set Data=IntegralValue_Current*isnull(Coeff,1)
	from 
	ImportNSI_SourceData_Report18Jur res
	 join @ImportedTable temp on res.ti_ID= temp.TI_ID and res.ChannelType= temp.ChannelType and res.EventDateCurrent = temp.EventDateTime
	where 
	res.ImportNumber= @LastImportNumber
		and res.AllowLoad=1  
		and	res.TI_ID is not null
		and res.ChannelType is  not null
		and res.IntegralValue_Current is not null
		and res.EventDateCurrent is not null
		 
	select @maxRowNumber = max(RowNumber) from @ImportedTable

	 --а теперь тупо суммируем все тарифные каналы по ТИ если нет суммы
	 insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, TI_ID, EventDateTime, ChannelType, DataSourceType, Data)
	 select @maxRowNumber+ROW_NUMBER() OVER(ORDER BY summChannel.TI_ID ASC),summChannel.* from 
	 (
	select   ResultStatus,Code=null,Meter_ID= null, TI_ID, EventDateTime,ChannelType= 1, DataSourceType,Data= Sum([data]) 
	from 
	@ImportedTable r0
	where 
	ChannelType <>1 
				and not exists (
				select top 1 1 from 
				@ImportedTable r1
				where 
				r1.TI_ID = r0.TI_ID 
				and r1.ChannelType =1
				and r1.EventDateTime = r0.EventDateTime
				and r1.DataSourceType = r0.DataSourceType 			
				and r1.RowNumber<>r0.RowNumber)
	group by  ResultStatus,Code, Meter_ID, TI_ID, EventDateTime,   DataSourceType 
	) 
	as summChannel


	set @msg=convert(varchar, getdate(),121)+ ' update ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	update ArchCalcBit_Integrals_Virtual_1 
	set Data= Source.Data, DispatchDateTime = getdate()
	from 
	ArchCalcBit_Integrals_Virtual_1 Target join @ImportedTable Source on 
	Target.TI_ID= Source.TI_ID and Target.ChannelType= Source.ChannelType and Target.EventDateTime= Source.EventDateTime and Target.DataSource_ID= Source.DataSourceType
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11

	set @msg=convert(varchar, getdate(),121)+ ' insert ArchCalcBit_Integrals_Virtual_1' 
	--print @msg
	insert 
	into ArchCalcBit_Integrals_Virtual_1
	(TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)


	select distinct
	Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSourceType,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0
	from  @ImportedTable Source 
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11
	and not exists
	(select top 1 1 from ArchCalcBit_Integrals_Virtual_1 Target 
	where 
	Target.TI_ID= Source.TI_ID 
	and Target.ChannelType= Source.ChannelType 
	and Target.EventDateTime= Source.EventDateTime 
	and Target.DataSource_ID= Source.DataSourceType
	)
     
	set @msg=convert(varchar, getdate(),121)+ ' update ArchCalcBit_Integrals_Virtual_5' 
	--print @msg
	update ArchCalcBit_Integrals_Virtual_5 
	set Data= Source.Data, DispatchDateTime = getdate()
	from 
	ArchCalcBit_Integrals_Virtual_5 Target 
	join @ImportedTable Source on 
	Target.TI_ID= Source.TI_ID and Target.ChannelType= Source.ChannelType and Target.EventDateTime= Source.EventDateTime and Target.DataSource_ID= Source.DataSourceType
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=15
   
	set @msg=convert(varchar, getdate(),121)+ ' insert ArchCalcBit_Integrals_Virtual_5' 
	--print @msg

	insert 
	into ArchCalcBit_Integrals_Virtual_5
	(TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)
	select distinct
	Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSourceType,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0
	from  @ImportedTable Source 
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where		
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=15
	and not exists
	(select top 1 1 from ArchCalcBit_Integrals_Virtual_5 Target 
	where 
	Target.TI_ID= Source.TI_ID 
	and Target.ChannelType= Source.ChannelType 
	and Target.EventDateTime= Source.EventDateTime 
	and Target.DataSource_ID= Source.DataSourceType
	)

	set @msg=convert(varchar, getdate(),121)+ ' Импортировали показания текущие' 
	--print @msg

	------------------------------------------------------------
	--ИМПОРТ в основную структуру по заводским номерам ПУ
	------------------------------------------------------------
 
	 declare @lastEventDateTime datetime
	 set @lastEventDateTime =dateadd(S,-1, dateadd(M, 1,@EventDateTime))
 
	 declare @ourSerailNumbers table (SerialNumber varchar(200))

	 insert into @ourSerailNumbers 
	 select distinct
	 ImportNSI_SourceData_Report18Jur.SerialNumber
	 from 
	 ImportNSI_SourceData_Report18Jur
	  where 
	 IMportNumber= @LastImportNumber
	 and ChannelType is not null
	 and SerialNumber in (select MeterserialNumber 
							from Hard_Meters 
								join Info_Meters_TO_TI on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
								join vw_Dict_Hierarchy on vw_Dict_Hierarchy.TI_ID =Info_Meters_TO_TI.TI_ID
							where 
							vw_Dict_Hierarchy.HierLev1_ID<>@H1DefaultID
						
							)
	and len (SerialNumber)>3

	Create table #ourTI  
	(
	ID int identity(1,1),
	TI_ID int, 
	Meter_ID int,
	SerialNumber nvarchar(200),
	StartDateTime datetime,FinishDateTime datetime,

	ChannelType int,

	IntegralValue_Previous float,
	IntegralValue_Current float,
	FileTRCoeff float,
	DBTRCoeff float,
	AllowLoad bit,
	ErrorMessage nvarchar(800),
	Coeff float
	primary key (ID)
	)

	create index IX_TEMP_Import_OurTI_F18Jur_TI_ID on  #ourTI (TI_ID)

	create index IX_TEMP_Import_OurTI_F18Jur_Meter_ID on  #ourTI (Meter_ID)


	 insert into #ourTI
	 (
	 TI_ID,
	 Meter_ID,SerialNumber,
	StartDateTime,
	FinishDateTime,
	 ChannelType,
	 IntegralValue_Previous,
	 IntegralValue_Current,
	 FileTRCoeff,
	 AllowLoad,ErrorMessage,Coeff)


	--по заводскому номеру находим ТИ
	select 
	Info_Meters_TO_TI.TI_ID,
	Hard_meters.Meter_ID,
	ImportNSI_SourceData_Report18Jur.SerialNumber,
	--даты показаний
	--если счетчик был установлен ранее то пишем начало месяца (предыд показания будут на эту дату)
	StartDateTime=case when  Info_Meters_TO_TI.StartDateTime<@EventDateTime then @EventDateTime
				--если дата установки внутри загружаемого месяца, то пишем предыдущие (начальные) показания на эту дату
				else Info_Meters_TO_TI.StartDateTime end,
	--если дата снятия ПУ внутри месяца то берем ее 
	FinishDateTime= case when  Info_Meters_TO_TI.FinishDateTime is not null and Info_Meters_TO_TI.FinishDateTime<=@lastEventDateTime then isnull(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')
				--если за загружаемым месяцем то ставим конец месяца
					else @lastEventDateTime end,
	ImportNSI_SourceData_Report18Jur.ChannelType,
	ImportNSI_SourceData_Report18Jur.IntegralValue_Previous,
	ImportNSI_SourceData_Report18Jur.IntegralValue_Current,
	ImportNSI_SourceData_Report18Jur.TRCoeff,
	AllowLoad=1,
	ErrorMessage='',
	Coeff
	from 
	ImportNSI_SourceData_Report18Jur
	join @ourSerailNumbers ourSerailNumbers on ourSerailNumbers.SerialNumber=ImportNSI_SourceData_Report18Jur.SerialNumber
	join Hard_meters on Hard_meters.MeterSerialNumber = ourSerailNumbers.SerialNumber 
						 and Hard_meters.Meter_ID in (select Meter_ID 
												from  Info_Meters_TO_TI 
													join vw_Dict_Hierarchy on vw_Dict_Hierarchy.TI_ID =Info_Meters_TO_TI.TI_ID
												where 
							vw_Dict_Hierarchy.HierLev1_ID<>@H1DefaultID
							and 
							((Info_Meters_TO_TI.StartDateTime>=@EventDateTime and Info_Meters_TO_TI.StartDateTime<=@lastEventDateTime)
							or 
							(@EventDateTime>=Info_Meters_TO_TI.StartDateTime and @EventDateTime<=isnull(Info_Meters_TO_TI.FinishDateTime ,'01-01-2100'))
							))
	Join Info_Meters_TO_TI on Info_Meters_TO_TI.METER_ID=Hard_meters.Meter_ID
	join vw_Dict_Hierarchy on vw_Dict_Hierarchy.HierLev1_ID<>@H1DefaultID and vw_Dict_Hierarchy.TI_ID =Info_Meters_TO_TI.TI_ID
	where 
	 ImportNumber=@LastImportNumber
  

	   update #ourTI
	   set AllowLoad=0, ErrorMessage=ErrorMessage+'имеется другая запись в этом периоде; '
	   from #ourTI ourTI
	   where 
	   --для ТИ
	   exists (select top 1 11 from #ourTI b 
					where 
					ourTI.TI_ID=b.TI_ID 
					and ourTI.ChannelType=b.ChannelType
					and b.ID<>ourTI.ID 
					and ((ourTI.StartDateTime between b.StartDateTime and b.FinishDateTime)
						or (b.StartDateTime between ourTI.StartDateTime and ourTI.FinishDateTime)
						))
		--для ПУ
	  or exists (select top 1 11 from #ourTI b 
					where 
					ourTI.Meter_ID=b.Meter_ID 
					and ourTI.ChannelType=b.ChannelType
					and b.ID<>ourTI.ID 
					and ((ourTI.StartDateTime between b.StartDateTime and b.FinishDateTime)
						or (b.StartDateTime between ourTI.StartDateTime and ourTI.FinishDateTime)
						))

	   update #ourTI
	   set AllowLoad=0, ErrorMessage=ErrorMessage+'не совпадает коэффициент трансформации; '
	   from #ourTI ourTI
	   where 
	   --для ТИ
	   exists (select top 1 11 from Info_Transformators b 
					where 
					ourTI.TI_ID=b.TI_ID  
					and (b.COEFI*b.COEFU)<>ourTI.FileTRCoeff 
					and ((ourTI.StartDateTime between b.StartDateTime and b.FinishDateTime)
						or (b.StartDateTime between ourTI.StartDateTime and ourTI.FinishDateTime)
						))
 
	set @msg=convert(varchar, getdate(),121)+ 'удаляем показания за месяц с этого источника для загружаемых ТИ' 
	--print @msg
 
	delete from 
	ArchCalcBit_Integrals_Virtual_1
	where 
	TI_ID in (select TI_ID from #ourTI where AllowLoad=1)
	and EventDateTime>= @EventDateTime
	and EventDateTime<DATEADD(month,1,@EventDateTime)
	and DataSource_ID = @DataSourceType

	set @msg=convert(varchar, getdate(),121)+ ' выборка предыдущих показаний' 
	--print @msg
 
	delete from @ImportedTable 

	insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, 
	TI_ID, EventDateTime, ChannelType, DataSourceType, Data)
 
	select distinct
	ID, 0,null,null,
	TI_ID, StartDateTime, ChannelType , @DataSourceType, IntegralValue_Previous*Coeff
	from #ourTI res
	where  
		res.AllowLoad=1   
 
	select @maxRowNumber = max(RowNumber) from @ImportedTable

	 --а теперь тупо суммируем все тарифные каналы по ТИ если нет суммы
	 --МОЖЕТ БЫТЬ ДОБАВИТЬ ПРОВЕРКУ НА наличие ВСЕХ требуемых каналов?
	 insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, TI_ID, EventDateTime, ChannelType, DataSourceType, Data)
	 select @maxRowNumber+ROW_NUMBER() OVER(ORDER BY summChannel.TI_ID ASC),summChannel.* from 
	 (
	select   ResultStatus,Code=null,Meter_ID= null, TI_ID, EventDateTime,ChannelType= 1, DataSourceType,Data= Sum([data]) 
	from 
	@ImportedTable r0
	where 
	ChannelType <>1 
				and not exists (
				select top 1 1 from 
				@ImportedTable r1
				where 
				r1.TI_ID = r0.TI_ID 
				and r1.ChannelType =1
				and r1.EventDateTime = r0.EventDateTime
				and r1.DataSourceType = r0.DataSourceType 			
				and r1.RowNumber<>r0.RowNumber)
	group by  ResultStatus,Code, Meter_ID, TI_ID, EventDateTime,   DataSourceType 
	) 
	as summChannel


	set @msg=convert(varchar, getdate(),121)+ ' update ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	update ArchCalcBit_Integrals_Virtual_1 
	set Data= Source.Data, DispatchDateTime = getdate()
	from 
	ArchCalcBit_Integrals_Virtual_1 Target join @ImportedTable Source on 
	Target.TI_ID= Source.TI_ID and Target.ChannelType= Source.ChannelType and Target.EventDateTime= Source.EventDateTime and Target.DataSource_ID= Source.DataSourceType
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11

	set @msg=convert(varchar, getdate(),121)+ ' insert ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	insert 
	into ArchCalcBit_Integrals_Virtual_1
	(TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)


	select distinct
	Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSourceType,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0
	from  @ImportedTable Source 
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11
	and not exists
	(select top 1 1 from ArchCalcBit_Integrals_Virtual_1 Target 
	where 
	Target.TI_ID= Source.TI_ID 
	and Target.ChannelType= Source.ChannelType 
	and Target.EventDateTime= Source.EventDateTime 
	and Target.DataSource_ID= Source.DataSourceType
	)  

	set @msg=convert(varchar, getdate(),121)+ ' Импортировали показания предыдущие' 
	--print @msg

	set @msg=convert(varchar, getdate(),121)+ ' выборка текущих показаний' 
	--print @msg

	delete from @ImportedTable

	select @maxRowNumber = max(RowNumber) from @ImportedTable
	--текущие
	insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, 
	TI_ID, EventDateTime, ChannelType, DataSourceType, Data)
 
	select distinct 
	ID, 0, null, null,  TI_ID, EventDateCurrent=FinishDateTime, ChannelType ,@DataSourceType,   Data=IntegralValue_Current*Coeff
	from #ourTI res
	where  
	res.AllowLoad=1  
  
	select @maxRowNumber = max(RowNumber) from @ImportedTable

	 --а теперь тупо суммируем все тарифные каналы по ТИ если нет суммы
	 --МОЖЕТ БЫТЬ ДОБАВИТЬ ПРОВЕРКУ НА наличие ВСЕХ требуемых каналов?
	 insert into @ImportedTable
	(RowNumber, Resultstatus, Code, Meter_ID, TI_ID, EventDateTime, ChannelType, DataSourceType, Data)
	 select @maxRowNumber+ROW_NUMBER() OVER(ORDER BY summChannel.TI_ID ASC),summChannel.* from 
	 (
	select   ResultStatus,Code=null,Meter_ID= null, TI_ID, EventDateTime,ChannelType= 1, DataSourceType,Data= Sum([data]) 
	from 
	@ImportedTable r0
	where 
	ChannelType <>1 
				and not exists (
				select top 1 1 from 
				@ImportedTable r1
				where 
				r1.TI_ID = r0.TI_ID 
				and r1.ChannelType =1
				and r1.EventDateTime = r0.EventDateTime
				and r1.DataSourceType = r0.DataSourceType 			
				and r1.RowNumber<>r0.RowNumber)
	group by  ResultStatus,Code, Meter_ID, TI_ID, EventDateTime,   DataSourceType 
	) 
	as summChannel

	set @msg=convert(varchar, getdate(),121)+ ' update ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	update ArchCalcBit_Integrals_Virtual_1 
	set Data= Source.Data, DispatchDateTime = getdate()
	from 
	ArchCalcBit_Integrals_Virtual_1 Target join @ImportedTable Source on 
	Target.TI_ID= Source.TI_ID and Target.ChannelType= Source.ChannelType and Target.EventDateTime= Source.EventDateTime and Target.DataSource_ID= Source.DataSourceType
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11

	set @msg=convert(varchar, getdate(),121)+ ' insert ArchCalcBit_Integrals_Virtual_1' 
	--print @msg

	insert 
	into ArchCalcBit_Integrals_Virtual_1
	(TI_ID,	EventDateTime,	ChannelType,	DataSource_ID,	Data,	ManualEnterData,IntegralType,	DispatchDateTime,	Status,ContrReplaceStatus,	ManualEnterStatus,	MainProfileStatus,	IsUsedForFillHalfHours,	CUS_ID)


	select distinct
	Source.TI_ID,	Source.EventDateTime,	Source.ChannelType,	Source.DataSourceType,	Source.Data, NULL,	0,	 getdate(),0, 0,	0,	0,	NULL,	0
	from  @ImportedTable Source 
	join info_TI on SOURCE.TI_ID = Info_TI.TI_ID 
	where 
	Source.TI_ID is not null
	and Source.ChannelType is not null
	and Source.Data is not null
	and info_TI.TIType=11
	and not exists
	(select top 1 1 from ArchCalcBit_Integrals_Virtual_1 Target 
	where 
	Target.TI_ID= Source.TI_ID 
	and Target.ChannelType= Source.ChannelType 
	and Target.EventDateTime= Source.EventDateTime 
	and Target.DataSource_ID= Source.DataSourceType
	)  
			
	update ImportNSI_SourceData_Report18Jur
	set
	ErrorMessage =ImportNSI_SourceData_Report18Jur.ErrorMessage+' ТИ из общей структуры '+ ourTI.ErrorMessage
	from 
	ImportNSI_SourceData_Report18Jur 
	join #ourTI ourTI on ImportNSI_SourceData_Report18Jur.SerialNumber like ourTI.SerialNumber
	where 
	ImportNumber= @LastImportNumber 

	declare @EventDateTimeEnd datetime 
	set  @EventDateTimeEnd=DATEADD(s,-1,DATEADD(m,  1,@EventDateTime))

	declare @DispDateTime datetime
	set @DispDateTime= getdate()

	--=============================================================================
	--импорт РАСХОДа в акт недоучета 
	--здесь пишем расход и отдельно коэфф потерь

	set @msg=convert(varchar, getdate(),121)+ 'удаляем РАСХОД за месяц с этого источника для загружаемых ТИ' 
	--print @msg

	begin Transaction ArchCalc_Replace_ActUndercount;	
 
		--для всех загружаемых ТИ удаляем источник потребитель за загружаемый период
		--в 1 и 5 зранилище
		delete from 
		ArchCalc_Replace_ActUndercount
		where 
		TI_ID in (select TI_ID from ImportNSI_SourceData_Report18Jur where 			
					ImportNumber= @LastImportNumber and  AllowLoad=1 
							and	TI_ID is not null
							and TIType in (11,15))
		and StartDateTime between  @EventDateTime and @EventDateTimeEnd


		--EnumActMode Замещение = 0, -Вычитание = 2
		--если значение отрицательное то пишем его положительными с признаком "вычитание"

		insert into ArchCalc_Replace_ActUndercount
		(TI_ID,
		ChannelType,
		StartDateTime,
		FinishDateTime,
		AddedValue,
		CommentString,
		User_ID,
		IsInactive,
		CUS_ID,
		ActUndercount_UN,
		ActMode,
		IsFinishDateTimeInclusive,
		IsCoeffTransformationEnabled,
		IsLossesCoefficientEnabled
		)
		select distinct 
		TI_ID,
		ChannelType,
		StartDateTime,
		FinishDateTime,
		AddedValue,
		CommentString,
		User_ID,
		IsInactive,
		CUS_ID,
		ActUndercount_UN=newid(),
		ActMode,
		IsFinishDateTimeInclusive,
		1,
		1
		 from 
		 (
			select distinct
			 TI_ID,
			 ChannelType=1,
		 
			 --пишем строго в пределах импортируемого месяца
			 StartDateTime=case when EventDatePrevious< @EventDateTime then @EventDateTime else EventDatePrevious end,
			 FinishDateTime= case when EventDateCurrent> @EventDateTimeEnd then @EventDateTimeEnd else @EventDateTimeEnd end,

			  --если значение отрицательное то пишем его с признаком вычитание 
			  --делим на коэфф и ставим галку использовать коэфф трансформации
			  --пишем значение из общего расхода.. потом оно будет приведено в зависимости от коэфф потерь..
			 AddedValue= (case when isnull(IntegralValue_Expense ,0)<0 then -1*isnull(IntegralValue_Expense ,0)
							   else IntegralValue_Expense end)
							   *res.Coeff
								/ (case when isnull(res.TRCoeff,1)=0 then 1 else isnull(res.TRCoeff,1) end),

			 CommentString='импорт из файла (форма 18ЮЛ) '+convert(varchar,@DispDateTime,121),
			 User_ID='80004Q4WZ1350KO8NT59RM',
			 IsInactive=0,
			 CUS_ID=0,
			 --вычитание если отрицательное, замещение если положительное
			 Actmode= case when isnull(IntegralValue_CommonExpense ,0)<0 then 2
							   else 0 end,
			 --конец периода вкл
			 IsFinishDateTimeInclusive=1

			from 
				ImportNSI_SourceData_Report18Jur res
			where  
				ImportNumber= @LastImportNumber and  AllowLoad=1 
				and	res.TI_ID is not null
				and res.ChannelType is  not null
				and res.ChannelType=1
				and res.IntegralValue_Expense is not null	---РАСХОД ДОЛЖНЫ БЫТЬ УКАЗАН
				--and res.IntegralValue_Losses is not null	--потери не обязательно
				and res.EventDatePrevious is not null
				and res.EventDateCurrent is not null
				and res.IntegralValue_Current is null		

		) as rr


		set @msg=convert(varchar, getdate(),121)+ ' Импортировали расход' 
		--print @msg
	 
	commit  Transaction ArchCalc_Replace_ActUndercount;	

	--=============================================================================
	----импорт коэфф ПОТЕРЬ Info_TI_LossesCoefficients
	--ПОТЕРИ УЖЕ ПРИВЕДЕНЫ К коэффициенту!! должно быть например 1,02 это значит потери 2% (добавлены)


	update ImportNSI_SourceData_Report18Jur
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' коэффициент потерь должен быть >0;'
	where 
	ImportNumber= @LastImportNumber  
	and AllowLoad=1
	and	TI_ID is not null
		and ChannelType is  not null
		and ChannelType=1
		and IntegralValue_Expense is not null	--РАСХОД И ПОТЕРИ ДОЛЖНЫ БЫТЬ УКАЗАНЫ
		and IntegralValue_Losses is not null
		and isnull(IntegralValue_Losses,0)<=0


	begin Transaction Info_TI_LossesCoefficients;	

	delete from 
	Info_TI_LossesCoefficients
	where 
	TI_ID in (select TI_ID from ImportNSI_SourceData_Report18Jur where 			
				ImportNumber= @LastImportNumber and  AllowLoad=1 
						and	TI_ID is not null and LossesCoefficient is not null)
	and StartDateTime between  @EventDateTime and @EventDateTimeEnd
 
  

	 insert into  Info_TI_LossesCoefficients
		(TI_ID,
		StartDateTime,
		FinishDateTime,
		LossesCoefficient_ID,
		LossesCoefficient,
		User_ID,
		DispatchDateTime)
	select distinct
		TI_ID,
		StartDateTime,
		FinishDateTime,
		LossesCoefficient_ID= newid(),
		LossesCoefficient,
		User_ID  ='80004Q4WZ1350KO8NT59RM',
		DispatchDateTime=@DispDateTime	 
	from
	 (	select distinct
		  TI_ID,
		  StartDateTime=case when EventDatePrevious< @EventDateTime then @EventDateTime else EventDatePrevious end,
		  FinishDateTime= case when EventDateCurrent> @EventDateTimeEnd then @EventDateTimeEnd else @EventDateTimeEnd end,
		  LossesCoefficient = res.IntegralValue_Losses	 
		from 
			ImportNSI_SourceData_Report18Jur res
		where  
			ImportNumber= @LastImportNumber and  AllowLoad=1 
			and	res.TI_ID is not null
			and res.ChannelType is  not null
			and res.ChannelType=1
			and res.IntegralValue_Expense is not null	--РАСХОД И ПОТЕРИ ДОЛЖНЫ БЫТЬ УКАЗАНЫ
			and res.IntegralValue_Losses is not null
			and res.EventDatePrevious is not null
			and res.EventDateCurrent is not null
			and isnull(IntegralValue_Losses,0.0)>0.0		
			and isnull(IntegralValue_Losses,0.0)<>1.0
			--все таки импортируем всегда
			--and res.IntegralValue_Current is null		

	)
	as a

	commit  Transaction Info_TI_LossesCoefficients;	



END--данные (не загружаем если тип - только деревья)


----Обновляем последний период для счетчиков, чтобы поиск работал и тд. 
--объединение одинаковых периодов тут же можно продумать
update Info_Meters_TO_TI
set FinishDateTime= '01-01-2100'
--select * 
from
Info_Meters_TO_TI a
where 
a.TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)
and not exists (select top 1 1 from Info_Meters_TO_TI b where b.TI_ID = a.TI_ID and b.StartDateTime>a.StartDateTime )
and a.FinishDateTime is not null
and a.FinishDateTime < '01-01-2100'


--====================================================================
--КОРРЕКТИРУЕМ  
--====================================================================
--для таблицы ImportNSI_SourceData_Report18Jur и соотв @LastImportNumber

--дату завершения младшего периода при пересечениях
update Info_Meters_TO_TI
set 
FinishDateTime=DATEADD(s,-1,b.StartDateTime)
--select distinct a.*, b.StartDateTime, b.FinishDateTime, DATEADD(s,-1,b.StartDateTime)
from 
Info_Meters_TO_TI a 
	cross apply
	(select top 1 * from  
	Info_Meters_TO_TI b
	where a.TI_ID = b.TI_ID and a.MetersReplaceSession_ID<>b.MetersReplaceSession_ID
	and
	a.StartDateTime<b.StartDateTime
	and isnull(a.FinishDateTime,'01-01-2100')>b.StartDateTime
	order by StartDateTime asc

	) as b
where 
a.TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)

update Info_Transformators
set 
FinishDateTime=DATEADD(s,-1,b.StartDateTime)
--select distinct a.*, b.StartDateTime, b.FinishDateTime, DATEADD(s,-1,b.StartDateTime)
from 
Info_Transformators a 
	cross apply
	(select top 1 * from  
	Info_Transformators b
	where a.TI_ID = b.TI_ID
	and
	a.StartDateTime<b.StartDateTime
	and isnull(a.FinishDateTime,'01-01-2100')>b.StartDateTime
	order by StartDateTime asc

	) as b
where 
a.TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)




--на всякий случай ставим дату завершения = конец месяца для ТИ у которых они некорректны
--при заменах почему то перепутаны были даты завершения 
--первый период (заканчивался на конец месяца) - корректируется выше
--второй (новый ПУ) - ставим конец месяца
update Info_Meters_TO_TI
set FinishDateTime=dateadd(s,-1,DATEADD(month, DATEDIFF(month, 1, StartDateTime)+1, 0))  
from Info_Meters_TO_TI 
where 
TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)
and isnull(FinishDateTime,'01-01-2100')<StartDateTime

update Info_Transformators
set FinishDateTime=dateadd(s,-1,DATEADD(month, DATEDIFF(month, 1, StartDateTime)+1, 0))  
from Info_Transformators 
where 
TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)
and isnull(FinishDateTime,'01-01-2100')<StartDateTime


--НЕПРЕРЫВНЫЕ диапазоны
--ДЛЯ ВСЕХ ТИ выбираем ВСЕ диапазоны подключения ПУ, при этом объединяем непрерывные (дата завершения отличается на 1 секунду)
--для которых еще не указаны показания на момент замены
--и объединяем
declare @tempDates table (TI_ID int , Meter_ID int, MinStartDateTime datetime, MaxFinishDatetime datetime  , MetersReplaceSession_ID uniqueidentifier)


;with 
cte as (
    select TI_ID, Meter_ID, StartDateTime,FinishDateTime=dateadd(s,1,isnull(FinishDateTime,'01-01-2100')), MetersReplaceSession_ID
    from Info_meters_to_TI 
	where 
	MetersReplaceSession_ID not in (select MetersReplaceSession_ID from Info_Meters_ReplaceHistory_Channels)
	--and TI_ID = 73606666
	and TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)
    union all
    select t.TI_ID, t.METER_ID, cte.StartDateTime, dateadd(s,1,isnull(t.FinishDateTime,'01-01-2100')), cte.MetersReplaceSession_ID
    from cte
    join Info_meters_to_TI t on cte.TI_ID = t.TI_ID and t.METER_ID=cte.METER_ID and cte.FinishDateTime = t.StartDateTime
	where 
	t.MetersReplaceSession_ID not in (select MetersReplaceSession_ID from Info_Meters_ReplaceHistory_Channels)
	 --and t.TI_ID = 73606666
	and  t.TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)
	
), 
cte2 as (
    select *, rn = row_number() over (partition by TI_ID,Meter_ID , FinishDateTime order by StartDateTime)
    from cte
)

insert into @tempDates
(TI_ID,Meter_ID,MinStartDateTime,MaxFinishDatetime,MetersReplaceSession_ID)
select distinct  TI_ID,Meter_ID, StartDateTime, dateadd(s,-1,max(FinishDateTime)) FinishDateTime,MetersReplaceSession_ID
from cte2
where rn=1
group by  TI_ID,StartDateTime,Meter_ID,MetersReplaceSession_ID
order by TI_ID, StartDateTime, Meter_ID;


--удаляем 
delete
from Info_Meters_TO_TI
where 
TI_ID in (select TI_ID from @tempDates)
and TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)

--добавляем
insert into Info_Meters_TO_TI (TI_ID,METER_ID,StartDateTime,FinishDateTime,MetersReplaceSession_ID,CUS_ID,SourceType)
select distinct TI_ID,Meter_ID,MinStartDateTime,MaxFinishDatetime,MetersReplaceSession_ID,0,0
from
	@tempDates
where 
	TI_ID In  (select TI_ID from ImportNSI_SourceData_Report18Jur where ImportNumber= @LastImportNumber and	TI_ID is not null)

--по ТР непрерывные диапазоны тое можно сделать со строгой проверкой ИДентификаторов и коэффициентов







---=====РЕЗУЛЬТАТ=========================================


select distinct 
SheetNumber,
SheetName, 
RowID,
Message,
ErrorMessage
from ImportNSI_SourceData_Report18Jur
where 
ImportNumber= @LastImportNumber
and (AllowLoad=0 or isnull(ErrorMessage,'') <>'')


drop table #ourTI  




BEGIN TRANSACTION;  
  
BEGIN TRY  

 if  (@FreeHierarchyTypeLoad = 1 or @FreeHierarchyTypeLoad = 2 )
 begin    	
	--своб иерархии
	exec usp2_ImportNSI_Report_OEK_Integrals_FreeHierarchy @LastImportNumber

	--юл и сечения
	exec usp2_ImportNSI_Report_OEK_Section1 @LastImportNumber, @EventDateTime, @UserID
end

END TRY  
BEGIN CATCH     
        print ERROR_MESSAGE()   

		 IF @@TRANCOUNT > 0  
        ROLLBACK TRANSACTION;  
END CATCH;  

IF @@TRANCOUNT > 0  
    COMMIT TRANSACTION;  


delete from ImportNSI_SourceData_Report18Jur 
where ImportNumber= @LastImportNumber

 

set @msg=convert(varchar, getdate(),121)+ ' Готово' 
--print @msg

end
go


grant execute on usp2_ImportNSI_Report_OEK_Integrals to UserCalcService
go
grant execute on usp2_ImportNSI_Report_OEK_Integrals to UserDeclarator
go
grant execute on usp2_ImportNSI_Report_OEK_Integrals to UserImportService
go
grant execute on usp2_ImportNSI_Report_OEK_Integrals to UserExportService
go



 