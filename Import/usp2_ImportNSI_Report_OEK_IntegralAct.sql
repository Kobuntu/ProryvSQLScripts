--очищаем (при перезапусках службы) если по каким-то причинам осталась наполнена
if exists (select 1
          from sysobjects
          where  id = object_id('dbo.TEMP_ImportNSI_Report_OEK_IntegralAct')
          and type in ('U'))
   truncate table dbo.TEMP_ImportNSI_Report_OEK_IntegralAct
go


if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp2_ImportNSI_Report_OEK_IntegralAct')
          and type in ('P','PC'))
   drop procedure dbo.usp2_ImportNSI_Report_OEK_IntegralAct
go


IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ImportNSI_Report_OEK_IntegralAct_TableType' AND ss.name = N'dbo')
DROP TYPE [dbo].ImportNSI_Report_OEK_IntegralAct_TableType
-- Пересоздаем заново
CREATE TYPE [dbo].ImportNSI_Report_OEK_IntegralAct_TableType AS TABLE 
(
 SheetNumber int,
 RowNumber int,
 ColNumber int,
 SortNumber nvarchar(200), -- порядковый номер строки в ИнтАкте
 HierLev1Name nvarchar(400),

HierLev1_ID int,
HierLev2Name nvarchar(400),
HierLev2_ID int,
HierLev3Name nvarchar(400),
HierLev3_ID int,
PSName nvarchar(400),
PS_ID int,
TIName nvarchar(800), --оба столбца = название
TIAddName nvarchar(800),
TIEPPCode nvarchar(200),
TIATSCode nvarchar(200),
TIType int,
TI_ID int,

Model	nvarchar(200),
Manufacturer	nvarchar(200),
SerialNumber	nvarchar(200),
Meter_ID	int,
MeterModel_ID	int,
MeterType_ID	int,

ChannelType int,
Coeff	float,
IntegralValue_Previous float,
IntegralValue_Current float,
TRCoeff	float,
TRCoeffDataBase float,
IntegralValue_LossesPercent	float,
IntegralValue_CommonExpense float,
EventDatePrevious datetime,
EventDateCurrent  datetime,
Comment nvarchar(200),
IsNewMeter bit,
User_ID	nvarchar(200),
DispatchDateTime datetime,
AllowLoad bit,
ErrorMessage	nvarchar(max)
)

go
grant execute on type::dbo.ImportNSI_Report_OEK_IntegralAct_TableType to UserCalcService
go
grant execute on type::dbo.ImportNSI_Report_OEK_IntegralAct_TableType to UserDeclarator
go

create procedure  usp2_ImportNSI_Report_OEK_IntegralAct

 @items	ImportNSI_Report_OEK_IntegralAct_TableType readonly,
 @UserID nvarchar(200),
 @EventDateTime datetime,
 @DataSourceType int 

AS
BEGIN


set dateformat dmy
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare @EventDateTimeEnd datetime 
declare @msg nvarchar(max)

 


--на всякий случай удаляем старые записи
delete from TEMP_ImportNSI_Report_OEK_IntegralAct
where 
  DispatchDateTime<=dateadd(d,-1,GETDATE())

--приводим дату к первому числу если накосячат
set  @EventDateTime=DATEADD(m, DATEDIFF(m, 0,@EventDateTime), 0)
set  @EventDateTimeEnd=DATEADD(s,-1,DATEADD(m,  1,@EventDateTime))
  

declare  @ImportNumber uniqueidentifier
set @ImportNumber = newid()

declare @DispDateTime datetime
set @DispDateTime= getdate()

 


--умножаем на кол - во ТИ найденных по заводскому номеру (так как для ОЭК дубли счетчиков в структуре 18ЮЛ)
insert into TEMP_ImportNSI_Report_OEK_IntegralAct
	(
	ImportNumber,
	SheetNumber,
	RowNumber,
	ColNumber,
	SortNumber, 
	HierLev1Name,
	HierLev1_ID,
	HierLev2Name,
	HierLev2_ID,
	HierLev3Name,
	HierLev3_ID,
	PSName,
	PS_ID,
	TIName, 
	TIAddName,
	TIEPPCode,
	TIATSCode,
	TIType,
	TI_ID,
	Model,
	Manufacturer,
	SerialNumber,
	Meter_ID	,
	MeterModel_ID	,
	MeterType_ID	,

	ChannelType,
	Coeff	,
	IntegralValue_Previous,
	IntegralValue_Current,
	TRCoeff	,
	TRCoeffDataBase,
	IntegralValue_LossesPercent	,
	IntegralValue_CommonExpense,
	EventDatePrevious,
	EventDateCurrent,
	Comment,
	IsNewMeter,
	User_ID,
	DispatchDateTime,
	AllowLoad,
	ErrorMessage) 

select distinct
	@ImportNumber,
	SheetNumber,
	RowNumber,
	ColNumber,
	items.SortNumber,
	items.HierLev1Name,
	null,
	items.HierLev2Name,
	null,
	items.HierLev3Name,
	null,
	items.PSName,
	null,
	items.TIName,
	TIAddName,
	TIEPPCode=isnull(Info_TI.EPP_ID,items.TIEPPCode),
	Info_TI.TIATSCode,
	Info_TI.TIType,
	vw_Dict_Hierarchy.TI_ID,
	Model,
	Manufacturer,
	SerialNumber,
	Hard_Meters.Meter_ID	,
	Hard_Meters.MeterModel_ID	,
	Hard_Meters.MeterType_ID	,
	
	ChannelType,
	Coeff	,
	IntegralValue_Previous,
	IntegralValue_Current,
	TRCoeff	,
	TRCoeffDataBase,
	IntegralValue_LossesPercent	,
	IntegralValue_CommonExpense,
	EventDatePrevious,
	EventDateCurrent,
	Comment,
	IsNewMeter = case when items.Comment like '%установлен%' then 1 else 0 end ,	
	User_ID,
	@DispDateTime,
	--не загружаем если не указан номер сортировки (ненужные строки типа итогов)
	AllowLoad= case when isnull(AllowLoad,0)=0 or items.SortNumber  LIKE '%[^0-9]%' then 0 
		else 1 end,
	ErrorMessage=case when  isnull(AllowLoad,0)=0 or items.SortNumber  LIKE '%[^0-9]%' then 'нет данных' else isnull(ErrorMessage,'') end
from 
@items items
		left Join Hard_Meters on isnull(Hard_Meters.MeterSerialNumber ,'') = isnull(items.SerialNumber,'')
		left join Info_Meters_TO_TI on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
		and ( (StartDateTime between dateadd(s,-10,@EventDateTime) and @EventDateTimeEnd)
			or
			(dateadd(s,-10,@EventDateTime)  between StartDateTime and isnull(FinishDateTime,'2100-01-01'))
			)
		left join vw_Dict_Hierarchy on vw_Dict_Hierarchy.TI_ID= Info_Meters_TO_TI.TI_ID
		left join Info_TI on Info_Meters_TO_TI.TI_ID= Info_TI.TI_ID

--проверки (например дубли..)
 

 
  
  
 update TEMP_ImportNSI_Report_OEK_IntegralAct
 set HierLev2Name='нет данных'
  where 
 ImportNumber= @ImportNumber
 and AllowLoad=1
 and ltrim(isnull(HierLev2Name,'')) like ''
  
 update TEMP_ImportNSI_Report_OEK_IntegralAct
 set HierLev3Name='нет данных'
  where 
 ImportNumber= @ImportNumber
 and AllowLoad=1
 and ltrim(isnull(HierLev3Name,'')) like ''
 
 update TEMP_ImportNSI_Report_OEK_IntegralAct
 set PSName='нет данных'
  where 
 ImportNumber= @ImportNumber
 and AllowLoad=1
 and ltrim(isnull(PSName,'')) like ''

 
 update TEMP_ImportNSI_Report_OEK_IntegralAct
 set 
 AllowLoad=0, ErrorMessage= isnull(ErrorMessage,'')+' резерв;'
 where 
 ImportNumber= @ImportNumber
 and AllowLoad=1
 and SerialNumber like '%резерв%'

 
--не будем ничего выдумывать, не загружаем ТИ где не указана ПС 
--т.к. много вовторений типа "АХП" одна это ТИ или нет потом не поймешь
update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,ErrorMessage= ErrorMessage+' не указана ПС; '
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 and ltrim(isnull(psname,'')) like ''



--Создаем структуру  
--добавлять будем также в 18ЮЛ!!!!
declare @H1Defaultname nvarchar(200)= '18_ЮЛ'
declare @H1DefaultID tinyint = 9


--физлиц в другую структуру
if (exists ( select top 1 1 from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and  AllowLoad=1 and isnull(HierLev1Name,'') like 'Физ. лица'))
begin
	set @H1Defaultname = 'Физ. лица'
	select @H1DefaultID = HierLev1_ID from Dict_HierLev1 where isnull(StringName,'') like 'Физ. лица'
end

--====================================================================
--уровень 1 с

if (@H1DefaultID is not null)
begin
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set HierLev1_ID = @H1DefaultID
	where 
	ImportNumber= @ImportNumber 
end

if (@H1DefaultID is  null)
begin

	declare @maxidH1 int =-1

	select @maxidH1=max(HierLev1_ID) from Dict_HierLev1
	select @maxidH1= isnull(@maxidH1,0)+1
		
	if (not exists (select top 1 1 from Dict_HierLev1 where isnull(StringName,'')=@H1Defaultname))
	begin
		INSERT INTO Dict_HierLev1
		(HierLev1_ID,
StringName,
SortNumber,
KPOCode,
Description)
		values(@maxidH1,@H1Defaultname,null,null,null)
		set @H1DefaultID=@maxidH1
	end

	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set HierLev1_ID = @H1DefaultID
	where 
	ImportNumber= @ImportNumber 
end
 
--====================================================================
--поиск, добавление уровня 2  по названию листа, 

	--находим имеющиеся
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	HierLev2_ID = Dict_HierLev2.HierLev2_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_HierLev2  on Dict_HierLev2.HierLev1_ID= res.HierLev1_ID and  Dict_HierLev2.StringName = ltrim(rtrim(res.HierLev2Name))
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.HierLev1_ID is not null 
	and res.HierLev2_ID is null
	
	declare @maxidH2 int =0

	begin Transaction H2;	
	 

		select @maxidH2=max(HierLev2_ID) from Dict_HierLev2
		select @maxidH2= isnull(@maxidH2,0)
		
		--добавляем 
		insert into Dict_HierLev2 (HierLev1_ID, HierLev2_ID, StringName)
		select aa.HierLev1_ID, @maxidH2+ROW_NUMBER() OVER(ORDER BY aa.HierLev2Name ASC),aa.HierLev2Name
		from 
		(
		select distinct  HierLev2Name=ltrim(rtrim(HierLev2Name)), HierLev1_ID
		from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where 
		ImportNumber= @ImportNumber and  AllowLoad=1
		and HierLev1_ID is not null
		and HierLev2_ID is null
		) as aa

	 
	commit Transaction H2;

	--повторно обновляем
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	HierLev2_ID = Dict_HierLev2.HierLev2_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_HierLev2  on Dict_HierLev2.HierLev1_ID= res.HierLev1_ID and  Dict_HierLev2.StringName = ltrim(rtrim(res.HierLev2Name))
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.HierLev1_ID is not null 
	and res.HierLev2_ID is null

	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' не удалось создать уровень 2;'
	where 
	ImportNumber= @ImportNumber and  AllowLoad=1
	and HierLev2_ID is null

	
	set @msg=convert(varchar, getdate(),121)+ ' Добавляем родительские объекты - ур3' 
--print @msg

--====================================================================
--поиск, добавление уровня 3 по  названию ЮЛ

	declare @maxidH3 int =0

	--находим имеющиеся
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	HierLev3_ID = Dict_HierLev3.HierLev3_ID   
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_HierLev3  on Dict_HierLev3.HierLev2_ID= res.HierLev2_ID and  Dict_HierLev3.StringName = ltrim(rtrim(res.HierLev3Name))
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.HierLev2_ID is not null and res.HierLev3_ID is null
	
	begin Transaction H3;	
	begin try
		
		select @maxidH3=max(HierLev3_ID) from Dict_HierLev3
		select @maxidH3= isnull(@maxidH3,0)


		--добавляем 
		insert into Dict_HierLev3 (HierLev2_ID, HierLev3_ID, StringName)
		select aa.HierLev2_ID, @maxidH3+ROW_NUMBER() OVER(ORDER BY aa.HierLev3Name ASC),aa.HierLev3Name
		from 
		(
		select distinct  HierLev3Name=ltrim(rtrim(HierLev3Name)), HierLev2_ID
		from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where 
		 ImportNumber= @ImportNumber and  AllowLoad=1
		and HierLev2_ID is not null
		and HierLev3_ID is null
		) as aa

	end try
	begin catch
		set @maxidH3 =0
	end catch
	commit Transaction H3;

	--повторно обновляем
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	HierLev3_ID = Dict_HierLev3.HierLev3_ID   
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_HierLev3  on Dict_HierLev3.HierLev2_ID= res.HierLev2_ID and  Dict_HierLev3.StringName = ltrim(rtrim(res.HierLev3Name))
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.HierLev2_ID is not null and res.HierLev3_ID is null

	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' не удалось создать уровень 3 (ЮЛ);'
	where 
	ImportNumber= @ImportNumber and  AllowLoad=1
	and HierLev3_ID is null
 
--====================================================================
--поиск, добавление PS 

	declare @maxidPS int =0

	--находим имеющиеся
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	PS_ID = Dict_PS.PS_ID   
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_PS  on Dict_PS.HierLev3_ID= res.HierLev3_ID and  Dict_PS.StringName = ltrim(rtrim(res.PSName))
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
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
		from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where 
		ImportNumber= @ImportNumber and  AllowLoad=1
		and HierLev3_ID is not null
		and PS_ID is null
		) as aa

	end try
	begin catch
		set @maxidPS =0
	end catch
	commit Transaction PS;
	

	--повторно обновляем
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	PS_ID = Dict_PS.PS_ID   
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_PS  on Dict_PS.HierLev3_ID= res.HierLev3_ID and  Dict_PS.StringName = ltrim(rtrim(res.PSName))
	where
	 ImportNumber= @ImportNumber and  AllowLoad=1
	and res.HierLev3_ID is not null and res.PS_ID is null

	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set AllowLoad=0 , ErrorMessage= ErrorMessage+ ' не удалось создать уровень ПС;'
	where 
	ImportNumber= @ImportNumber and  AllowLoad=1
	and PS_ID is null


set @msg=convert(varchar, getdate(),121)+ ' Нашли/добавили уровни 1-4' 
--print @msg



--===========================================================================================================

declare @maxid int 




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
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	MeterType_ID = Dict_Meters_Types.MeterType_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_Meters_Types  on isnull(Dict_Meters_Types.MeterTypeName,'') like isnull(res.Manufacturer ,'')
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.MeterType_ID is null
	and res.Manufacturer is not null


	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	MeterType_ID = @MeterTypeID_NoData
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res	
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.MeterType_ID is null 

	update TEMP_ImportNSI_Report_OEK_IntegralAct set Model='нет данных'
	where 
	ImportNumber= @ImportNumber and isnull(Model,'') like ''

 
 	--находим имеющеся модели и по производителю
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	MeterModel_ID = Dict_Meters_Model.MeterModel_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_Meters_Model on  res.MeterType_ID=Dict_Meters_Model.MeterType_ID and  Dict_Meters_Model.StringName= res.Model 
	where
	ImportNumber= @ImportNumber and  AllowLoad=1
	and res.MeterModel_ID is null
	and res.MeterType_ID is not null
 
  	--находим просто по модели первый попавшийся
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	MeterModel_ID = Dict_Meters_Model.MeterModel_ID,
	MeterType_ID = Dict_Meters_Model.MeterType_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_Meters_Model on  Dict_Meters_Model.StringName = isnull(res.Model ,'')
	where
	 ImportNumber= @ImportNumber and  AllowLoad=1  	
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
		from TEMP_ImportNSI_Report_OEK_IntegralAct
		where  
		ImportNumber= @ImportNumber and  AllowLoad=1
		and MeterModel_ID is null and isnull(Model,'')<>''
	) as aa
	commit;

	--повторно обновляем
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	MeterModel_ID = Dict_Meters_Model.MeterModel_ID,
	MeterType_ID = Dict_Meters_Model.MeterType_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join Dict_Meters_Model on  Dict_Meters_Model.StringName= res.Model 
	where
	ImportNumber= @ImportNumber and  AllowLoad=1 
	and res.MeterModel_ID is null
	and isnull(res.MeterType_ID, @MeterTypeID_NoData)=Dict_Meters_Model.MeterType_ID
	
	--hard_meters 
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	Meter_ID= hard_meters.Meter_ID  
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join hard_meters on  hard_meters.MeterModel_ID=res.MeterModel_ID and  hard_meters.MeterSerialNumber= res.SerialNumber 
	where
	 ImportNumber= @ImportNumber and  AllowLoad=1 
	and res.Meter_ID is null
	and res.MeterModel_ID is not null
	and isnull( res.SerialNumber,'')<>''
	and isnull( res.SerialNumber,'')not like '%расч%'
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
	from TEMP_ImportNSI_Report_OEK_IntegralAct
	where 
	ImportNumber= @ImportNumber and  AllowLoad=1
	and Meter_ID is null and MeterModel_ID is not null and MeterType_ID is not null
	 and SerialNumber is not null
	and isnull( SerialNumber,'')not like '%расч%'
	 and len(SerialNumber)>3
	) as aa

	commit;

	--повторно обновляем
	update TEMP_ImportNSI_Report_OEK_IntegralAct
	set 
	Meter_ID= hard_meters.Meter_ID , MeterType_ID = hard_meters.MeterType_ID, MeterModel_ID =hard_meters.MeterModel_ID
	from
	TEMP_ImportNSI_Report_OEK_IntegralAct res
	join hard_meters on  hard_meters.MeterModel_ID=res.MeterModel_ID and  isnull(hard_meters.MeterSerialNumber,'')= isnull(res.SerialNumber ,'')
	where
	 ImportNumber= @ImportNumber and  AllowLoad=1 	
	and res.Meter_ID is null
	and res.MeterModel_ID is not null
	and isnull( res.SerialNumber,'')<>''
	and isnull( res.SerialNumber,'')not like '%расч%'
	and len(res.SerialNumber)>3

end


set @msg=convert(varchar, getdate(),121)+ ' Добавили/нашли ПУ' 
--print @msg





--=============================================================================================


--пробуем искать ТИ по коду ЕПП (номер абонента для ФЛ)
update 
TEMP_ImportNSI_Report_OEK_IntegralAct
set 
	TI_ID = Info_TI.TI_ID,
	--PS_ID= info_TI.PS_ID,
	TIType= info_TI.TIType 
from 
	TEMP_ImportNSI_Report_OEK_IntegralAct result, 
	Info_TI 
where  
	ImportNumber= @ImportNumber and  AllowLoad=1
	and	result.PS_ID is not null
	and result.TI_ID is null  
	and isnull(Info_TI.EPP_ID,'')<>''
	and isnull(result.TIEPPCode,'')=isnull(Info_TI.EPP_ID,'')
	
	 

--ДОБАВЛЕНЕ ТИ 
BEGIN TRANSACTION; 
 
	select @maxid=max(TI_ID) from Info_TI
	select @maxid= isnull(@maxid,0)
 
	insert into Info_TI (PS_ID, TI_ID, TIType, TIName, EPP_ID, TIATSCode, Commercial, Voltage, 
						AccountType, Deleted, IsCoeffTransformationDisabled, CreateDateTime, CUS_ID) 	
	 select 
		PS_ID, @maxid+ROW_NUMBER() OVER(ORDER BY aa.TIName ASC), TIType, TIName, TIEPPCode, TIATSCode,
		 Commercial=0, Voltage= 0, Accounttype = 0, Deleted= 0, IsCoeffTransformationDisabled=0, CreateDateTime= GETDATE(), CUS_ID=0
	from 
	(
		 select distinct
			PS_ID, TIType=isnull(TIType,15), 
			TIName=TIName+case when isnull(TIAddName,'')<>'' then ' ('+TIAddName+')' else '' end, 
			 TIATSCode, 
			 TIEPPCode
		from TEMP_ImportNSI_Report_OEK_IntegralAct result
		where    
			ImportNumber= @ImportNumber and  AllowLoad=1 
			and	result.PS_ID is not null
			and result.TI_ID is null			
			and result.IsNewMeter = 0
			and isnull(result.TIName,'') <> '' 	
			and isnull(result.TIEPPCode,'')<>''	
		)
	as aa
COMMIT;


--из только что добавленных!
--обновляем TI_ID по названию на ПС
-- 
update 
TEMP_ImportNSI_Report_OEK_IntegralAct
set 
	TI_ID = Info_TI.TI_ID,
	--PS_ID= info_TI.PS_ID,
	TIType= info_TI.TIType 
from 
	TEMP_ImportNSI_Report_OEK_IntegralAct result, 
	Info_TI 
where  
	ImportNumber= @ImportNumber and  AllowLoad=1
	and	result.PS_ID is not null
	and result.TI_ID is null  
	and isnull(Info_TI.EPP_ID,'')<>''
	and isnull(result.TIEPPCode,'')=isnull(Info_TI.EPP_ID,'')

 ---------------------------------------------------------------------------------

  


--====ЗАВНОМЕР=========================================================================================


--пробуем искать ТИ по названию по зав номер в ЕПП
update 
TEMP_ImportNSI_Report_OEK_IntegralAct
set 
	TI_ID = Info_TI.TI_ID,
	--PS_ID= info_TI.PS_ID,
	TIType= info_TI.TIType 
from 
	TEMP_ImportNSI_Report_OEK_IntegralAct result, 
	Info_TI 
where  
	ImportNumber= @ImportNumber and  AllowLoad=1
	and	result.PS_ID is not null
	and result.TI_ID is null 
	and 'ПУ№'+isnull(result.SerialNumber,'') =isnull(INfo_TI.EPP_ID,'')

	 


--ДОБАВЛЕНЕ ТИ 
BEGIN TRANSACTION; 
 
	select @maxid=max(TI_ID) from Info_TI
	select @maxid= isnull(@maxid,0)
 
	insert into Info_TI (PS_ID, TI_ID, TIType, TIName, EPP_ID, TIATSCode, Commercial, Voltage, 
						AccountType, Deleted, IsCoeffTransformationDisabled, CreateDateTime, CUS_ID) 	
	 select 
		PS_ID, @maxid+ROW_NUMBER() OVER(ORDER BY aa.TIName ASC), TIType, TIName, TIEPPCode, TIATSCode,
		 Commercial=0, Voltage= 0, Accounttype = 0, Deleted= 0, IsCoeffTransformationDisabled=0, CreateDateTime= GETDATE(), CUS_ID=0
	from 
	(
		 select distinct
			PS_ID, TIType=isnull(TIType,15), 
			TIName=TIName+rtrim(' '+TIAddName), 
			 TIATSCode, 
			 TIEPPCode= case when result.SerialNumber like '%расч%' then '' else 'ПУ№'+isnull(result.SerialNumber,'') end
		from TEMP_ImportNSI_Report_OEK_IntegralAct result
		where    
			ImportNumber= @ImportNumber and  AllowLoad=1 
			and	result.PS_ID is not null
			and result.TI_ID is null
			--and result.Meter_ID is not null
			--считаем что в файле обязательно будет новый и старый ПУ (иначе один только новый не загрузится)
			--добавляем ТИ для старых ПУ, для новых ниже обновим ИД (типа равны ПС, названия ТИ и строки +-1 и OldNew)				
			and result.IsNewMeter = 0
			and isnull(result.TIName,'') <> '' 		
		)
	as aa
COMMIT;


--из только что добавленных!
--обновляем TI_ID по названию на ПС
-- 
update 
TEMP_ImportNSI_Report_OEK_IntegralAct
set 
	TI_ID = Info_TI.TI_ID,
	--PS_ID= info_TI.PS_ID,
	TIType= info_TI.TIType 
from 
	TEMP_ImportNSI_Report_OEK_IntegralAct result, 
	Info_TI 
where  
	ImportNumber= @ImportNumber and  AllowLoad=1
	and	result.PS_ID is not null
	and result.TI_ID is null 	
	and 'ПУ№'+isnull(result.SerialNumber,'') =isnull(INfo_TI.EPP_ID,'')

 ---------------------------------------------------------------------------------


set @msg=convert(varchar, getdate(),121)+ ' Добавили ненайденные ТИ' 
--print @msg


--ОСТАЛЬНЫЕ ТИ- не найденные
--обновляем ИД ТИ для новых ПУ  по названию ПУ и ТИ

update TEMP_ImportNSI_Report_OEK_IntegralAct
set 
PS_ID= ro.ps_ID,
TI_ID=ro.TI_ID
from TEMP_ImportNSI_Report_OEK_IntegralAct resNew 
cross apply (select top 1 resOld.PS_ID, resOld.TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct resOld 
			where 
			resOld.ImportNumber= @ImportNumber
			-- and  resOld.AllowLoad=1
			and isnull(resOld.IsNewMeter,0)=0
			and resOld.TI_ID is not null
			--На этой же ПС в Файле
			and isnull(resOld.PS_ID,0)=isnull(resNew.PS_ID,-1)
			--расположены рядом согласно шаблона.. поэтому этих условий достаточно!!!
			and (isnull(resnew.RowNumber,-1)= isnull(resOld.RowNumber,-2)+1 )
						
			) ro
where  
 resNew.ImportNumber= @ImportNumber 
 --and  resNew.AllowLoad=1
--ИД нет
and resNew.TI_ID is null
--явно указано что это новый ПУ
and isnull(resnew.IsNewMeter,0)=1





--добавляем для старых (заменяемых, есл иесть замена) ПУ строки (там где не определились) 

insert into TEMP_ImportNSI_Report_OEK_IntegralAct
(SheetNumber,
	RowNumber,
	ColNumber,
	SortNumber, 
	HierLev1Name,
	HierLev1_ID,
	HierLev2Name,
	HierLev2_ID,
	HierLev3Name,
	HierLev3_ID,
	PSName,
	PS_ID,
	TIName, 
	TIAddName,
	TIEPPCode,
	TIATSCode,
	TIType,
	TI_ID,
	Model,
	Manufacturer,
	SerialNumber,
	Meter_ID	,
	MeterModel_ID	,
	MeterType_ID	,

	ChannelType,
	Coeff	,
	IntegralValue_Previous,
	IntegralValue_Current,
	TRCoeff	,
	TRCoeffDataBase,
	IntegralValue_LossesPercent	,
	EventDatePrevious,
	EventDateCurrent,
	Comment,
	IsNewMeter,
	User_ID,
	DispatchDateTime,
	AllowLoad,
	ErrorMessage)

select
 distinct 
 SheetNumber,
	RowNumber,
	ColNumber,
	SortNumber, 
	HierLev1Name,
	HierLev1_ID,
	HierLev2Name,
	HierLev2_ID,
	HierLev3Name,
	HierLev3_ID,
	PSName,
	PS_ID,
	TIName, 
	TIAddName,
	TIEPPCode,
	TIATSCode,
	TIType,
	TI_ID,
	Model,
	Manufacturer,
	SerialNumber,
	dupp.Meter_ID	,
	dupp.MeterModel_ID	,
	dupp.MeterType_ID	,

	ChannelType,
	Coeff	,
	IntegralValue_Previous,
	IntegralValue_Current,
	TRCoeff	,
	TRCoeffDataBase,
	IntegralValue_LossesPercent	,
	EventDatePrevious,
	EventDateCurrent,
	Comment,
	IsNewMeter,
	User_ID,
	DispatchDateTime,
	AllowLoad,
	ErrorMessage
from 
 TEMP_ImportNSI_Report_OEK_IntegralAct resOld 
 --
 cross apply (select top 1 resnewDupp.Meter_ID ,resnewDupp.MeterModel_ID	,	resnewDupp.MeterType_ID	
			from TEMP_ImportNSI_Report_OEK_IntegralAct resnewDupp
			  where  
			  resnewDupp.ImportNumber= @ImportNumber and  resnewDupp.AllowLoad=1
				 --И ЕСТЬ след строка (такой формат файла)
				 and (isnull(resnewDupp.RowNumber,-1)= isnull(resOld.RowNumber,-2)+1 ) 
			     and isnull(resnewDupp.IsNewMeter,0)=1
				 --но, на другой ПС 
				 and isnull(resnewDupp.PS_ID,0) <> isnull(resOld.PS_ID,-1)
				 )  as dupp
where 
 resOld.ImportNumber= @ImportNumber and  resOld.AllowLoad=1
and resold.PS_ID is not null
and isnull(resOld.IsNewMeter,0)=0
--и НЕТ след строки с порядковым номером +1 и Новым ПУ для этой ПС
and not exists 
	(select top 1 1 from TEMP_ImportNSI_Report_OEK_IntegralAct resnew
	 where 	 
 resnew.ImportNumber= @ImportNumber and  resnew.AllowLoad=1
	 and (isnull(resnew.RowNumber,-1)= isnull(resOld.RowNumber,-2)+1 ) 
 and isnull(resnew.IsNewMeter,0)=1
	 and isnull(resnew.PS_ID,0) = isnull(resOld.PS_ID,-1))
	 and isnull(resOld.SerialNumber,'') not like '%расч%'
--добавляем для всех, чтобы корректно потом добавить данные, но замены только для структуры 18 ЮЛ (в основной структуре не создаем замены так ак недостаточно данных)

 


---=====================================================================


 --меняем структуру в соотв с последними файлами (ТОЛЬКО для ветки 18ЮЛ)
 --переносим ТИ (чужие) на ПС указанные в листе 
 update Info_TI
 set PS_ID = res.PS_ID
 from TEMP_ImportNSI_Report_OEK_IntegralAct res join Info_TI on res.TI_ID= info_TI.TI_ID
 where
	res.ImportNumber= @ImportNumber and  res.AllowLoad=1
 and res.PS_ID<>Info_TI.PS_ID
	and	 res.TI_ID is not null
 and res.TI_ID in (select TI_ID from vw_Dict_Hierarchy where HierLev1_ID=@H1DefaultID)

				
set @msg=convert(varchar, getdate(),121)+ ' Перенесли ТИ' 
--print @msg	
		
--удаляем пустые родительские объекты без связей после переноса (ТОЛЬКО в ветке 18 ЮЛ? )
delete from Dict_PS 
where
PS_ID in (select distinct PS_ID from vw_Dict_HierarchyPS where PS_ID is not null and vw_Dict_HierarchyPS.HierLev1_ID=@H1DefaultID  ) 
and PS_ID not in (select PS_ID from Info_TI)
and PS_ID not in (select PS_ID from Hard_CommChannels where PS_ID is not null)
and PS_ID not in (select PS_ID from Info_Section_List where PS_ID is not null)
and PS_ID not in (select PS_ID from Dict_PS_PowerSupply_PS_List where PS_ID is not null)
and PS_ID not in (select PS_ID from Dict_JuridicalPersons_To_HierLevels where PS_ID is not null)
and PS_ID not in (select PS_ID from Info_Balance_FreeHierarchy_Objects where PS_ID is not null)
 

delete  from Dict_HierLev3 
where 
HierLev2_ID in (select   HierLev2_ID from Dict_HierLev2 where  Dict_HierLev2.HierLev1_ID=@H1DefaultID  )
and HierLev3_ID not in  (select HierLev3_ID from Dict_PS where HierLev3_ID is not null)
and HierLev3_ID not in  (select HierLev3_ID from Info_Section_List where HierLev3_ID is not null)
and HierLev3_ID not in  (select HierLev3_ID from Dict_JuridicalPersons_To_HierLevels where HierLev3_ID is not null)
and HierLev3_ID not in  (select HierLev3_ID from Info_Balance_FreeHierarchy_Objects where HierLev3_ID is not null)


delete  from Dict_HierLev2
where 
HierLev1_ID in (select   HierLev1_ID from Dict_HierLev1 where  Dict_HierLev1.HierLev1_ID=@H1DefaultID  )
and HierLev2_ID not in  (select HierLev2_ID from Dict_HierLev3 where HierLev2_ID is not null)
and HierLev2_ID not in  (select HierLev2_ID from Info_Section_List where HierLev2_ID is not null)
and HierLev2_ID not in  (select HierLev2_ID from Dict_JuridicalPersons_To_HierLevels where HierLev2_ID is not null)
and HierLev2_ID not in  (select HierLev2_ID from Info_Balance_FreeHierarchy_Objects where HierLev2_ID is not null)

 
set @msg=convert(varchar, getdate(),121)+ ' Удалили пустые уровни 1-4' 
--print @msg



update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,ErrorMessage= ErrorMessage+' не найдена ТИ; '
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 and TI_ID is null

update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,ErrorMessage= ErrorMessage+' не найдена ПС; '
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 and PS_ID is null


update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,ErrorMessage= ErrorMessage+' не найден ПУ; '
where
	ImportNumber= @ImportNumber and  AllowLoad=1 and  SerialNumber not like '%расч%' and Meter_ID is null



----не загружаем дубли ПУ на одной ПС++
update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,
ErrorMessage= ErrorMessage+' один и тот же ПУ указан несколько раз на ПС; '
from TEMP_ImportNSI_Report_OEK_IntegralAct res 
where 
	ImportNumber= @ImportNumber and  AllowLoad=1   
and PS_ID is not null

and PS_ID in
(
	select distinct a.PS_ID from
	(
		select PS_ID,SerialNumber, MeterCount= count( SerialNumber) from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where
	ImportNumber= @ImportNumber and  AllowLoad=1 
		and isnull(SerialNumber ,'')not like '%расч%'
		group by PS_ID ,SerialNumber
		having count(SerialNumber)>1
	) as a
)
 
update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,
ErrorMessage= ErrorMessage+' больше двух ПУ на одной ТИ; '
from TEMP_ImportNSI_Report_OEK_IntegralAct res 
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 
and PS_ID is not null

and PS_ID in
(
	select distinct a.PS_ID from
	(
		select PS_ID, TI_ID, MeterCount= count( Meter_ID) from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where
	ImportNumber= @ImportNumber and  AllowLoad=1 
		and isnull(SerialNumber ,'')not like '%расч%'
		group by PS_ID,  TI_ID
		having count(Meter_ID)>2
	) as a
)


--считаем кол-во ПУ На ТИ для замен (не должно быть больше 1 замены)


update TEMP_ImportNSI_Report_OEK_IntegralAct
set 
EventDatePrevious= case 
					when tn.Meter_ID is null then    @EventDateTime
					else  DATEADD(d, ((case when IsNewMeter=1 then 2 else 1 end)-1)*(DATEDIFF(d, @EventDateTime, dateadd(m,1,@EventDateTime))/2), @EventDateTime) end,
--дата текущих показания (конец месяца)
EventDateCurrent= dateadd(S,-1,  
				  case 
					when tn.Meter_ID is null then dateadd(month,1,@EventDateTime )
				    else DATEADD(d, ((case when IsNewMeter=1 then 2 else 1 end))*(DATEDIFF(d,@EventDateTime, dateadd(m,1,@EventDateTime))/2), @EventDateTime)  end)

from TEMP_ImportNSI_Report_OEK_IntegralAct res 
 outer apply ( select top 1 resNew.Meter_ID from TEMP_ImportNSI_Report_OEK_IntegralAct resNew  
				 where  
					resNew.ImportNumber= @ImportNumber
					and  resNew.AllowLoad=1  
					and resNew.TI_ID= res.TI_ID 
					and isnull(resNew.IsNewMeter,0)=1 
					and isnull(resnew.RowNumber,0)<>isnull(res.RowNumber,0)			 
					) as tn
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 


--привязки ПУ ТИ обновляем только для структуры 18ЮЛ
--для обычной тсруктуры не трогаем.. (пусть вручную там заполняют ибо вручную достовернее)


update TEMP_ImportNSI_Report_OEK_IntegralAct
set ChannelType=1




--можно сделать проверки на внешние ключи..



--========================================================================
--привязки ПУ

--удаляем только для 18ЮЛ (заменяем) замены ПУ в текущем периоде (которые по текущей схеме создаются - на каждый месяц)
delete from Info_Meters_TO_TI
where 
TI_ID in (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct 
			where  
	ImportNumber= @ImportNumber and  AllowLoad=1 
				and HierLev1_ID = @H1DefaultID 
				and TI_ID is not null
		 )
and 
	(
	(StartDateTime>=@EventDateTime and (StartDateTime <dateadd(MONTH,1,@EventDateTime)))
	or 
	(FinishDateTime is not null and FinishDateTime>=@EventDateTime and (FinishDateTime <dateadd(MONTH,1,@EventDateTime)))
	)

--добавляем только для 18ЮЛ
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
from TEMP_ImportNSI_Report_OEK_IntegralAct
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 
and HierLev1_ID=@H1DefaultID
and Meter_ID is not null
and TI_ID is not null
) as meters











set @msg=convert(varchar, getdate(),121)+ 'удаляем показания за месяц с этого источника для загружаемых ТИ' 
--print @msg

--удаляем все источники
--в 1 и 5 зранилище
delete from 
ArchCalcBit_Integrals_Virtual_1
where 
TI_ID in (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where 
			
	ImportNumber= @ImportNumber and  AllowLoad=1 
			 --для всех ТИ!!!
			--and HierLev1_ID = @H1DefaultID
			and	TI_ID is not null
			and TIType =11)
and EventDateTime>= @EventDateTime
and EventDateTime<DATEADD(month,1,@EventDateTime)
and DataSource_ID = @DataSourceType

delete from 
ArchCalcBit_Integrals_Virtual_5
where 
TI_ID in (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where 
			 
	ImportNumber= @ImportNumber and  AllowLoad=1 
			 --для всех ТИ!!!
			--and HierLev1_ID = @H1DefaultID
			and	TI_ID is not null
			and TIType =15)
and EventDateTime>= @EventDateTime
and EventDateTime<DATEADD(month,1,@EventDateTime)
and DataSource_ID = @DataSourceType





update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,
ErrorMessage= ErrorMessage+' больше двух значений для одной ТИ на указанную дату; '
from TEMP_ImportNSI_Report_OEK_IntegralAct res 
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 
and PS_ID is not null

and PS_ID in
(
	select distinct a.PS_ID from
	(
		select PS_ID, TI_ID, EventDatePrevious, dataCount= count( IntegralValue_Previous) from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where
	ImportNumber= @ImportNumber and  AllowLoad=1 
		group by PS_ID,  TI_ID,EventDatePrevious 
		having count(IntegralValue_Previous)>1
	) as a
)


update TEMP_ImportNSI_Report_OEK_IntegralAct
set AllowLoad=0,
ErrorMessage= ErrorMessage+' больше двух значений для одной ТИ на указанную дату; '
from TEMP_ImportNSI_Report_OEK_IntegralAct res 
where 
	ImportNumber= @ImportNumber and  AllowLoad=1 
and PS_ID is not null

and PS_ID in
(
	select distinct a.PS_ID from
	(
		select PS_ID, TI_ID, EventDateCurrent, dataCount= count( IntegralValue_Current) from TEMP_ImportNSI_Report_OEK_IntegralAct 
		where
	ImportNumber= @ImportNumber and  AllowLoad=1 
		group by PS_ID,  TI_ID,EventDateCurrent 
		having count(IntegralValue_Current)>1
	) as a
)




declare @ImportedTable ImportedInegralValueTableType


set @msg=convert(varchar, getdate(),121)+ ' выборка предыдущих показаний' 
--print @msg

insert into @ImportedTable
(RowNumber, Resultstatus, Code, Meter_ID, 
TI_ID, EventDateTime, ChannelType, DataSourceType, Data)

select ROW_NUMBER() OVER(ORDER BY rr.TI_ID ASC), 0, null, null, 
rr.TI_ID, EventDatePrevious, rr.ChannelType, @DataSourceType, IntegralValue_Previous from 
 (
	select distinct
	 TI_ID, EventDatePrevious, ChannelType , IntegralValue_Previous=IntegralValue_Previous*Isnull(COeff,1)
	from TEMP_ImportNSI_Report_OEK_IntegralAct res
	where  
	ImportNumber= @ImportNumber and  AllowLoad=1 
	and	TI_ID is not null
	and res.ChannelType is  not null
	and res.IntegralValue_Previous is not null
	and res.EventDatePrevious is not null
	 
) as rr



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




set @msg=convert(varchar, getdate(),121)+ ' Импортировали показания предыдущие' 
--print @msg




set @msg=convert(varchar, getdate(),121)+ ' выборка текущих показаний' 
--print @msg

delete from @ImportedTable


insert into @ImportedTable
(RowNumber, Resultstatus, Code, Meter_ID, 
TI_ID, EventDateTime, ChannelType, DataSourceType, Data)

select ROW_NUMBER() OVER(ORDER BY rr.TI_ID ASC), 0, null, null, 
rr.TI_ID, EventDateCurrent, rr.ChannelType, @DataSourceType, IntegralValue_Current from 
 (
	select distinct
	 TI_ID, EventDateCurrent, ChannelType , IntegralValue_Current=IntegralValue_Current*isnull(coeff,1)
	from TEMP_ImportNSI_Report_OEK_IntegralAct res
	where  
	res.ImportNumber= @ImportNumber and  res.AllowLoad=1 
	and	TI_ID is not null
	and res.ChannelType is  not null
	and res.IntegralValue_Current is not null
	and res.EventDateCurrent is not null
	 
) as rr



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

 




 
--=================коэфф ТР!!==============================

-- имопрт плобмб+ТР   будет конфликтовать но...
  
--каждый месяц перезагружаем, если поменялся
begin
   

   begin transaction;
    
					 
	set @msg=convert(varchar, getdate(),121)+ ' update   Info_Transformators set FinishDateTime= 2100'
	--print @msg

	--ограничиваем предыдущие коэфф первым числом месяца (если не совпадают)
	update  
	Info_Transformators
	set FinishDateTime= DATEADD(s,-1,@EventDateTime)
	where 
	TI_ID in (select TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID
				from TEMP_ImportNSI_Report_OEK_IntegralAct 
				where 
				ImportNumber= @ImportNumber
				and AllowLoad = 1 
				and TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID is not null
				--and HierLev1_ID = @H1DefaultID 
				and TEMP_ImportNSI_Report_OEK_IntegralAct.TRCoeff is not null
				and abs((Info_Transformators.COEFU*Info_Transformators.COEFI)-isnull(TEMP_ImportNSI_Report_OEK_IntegralAct.TRCoeff,1))>0.000000001 
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
	TI_ID in (select TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID
					from TEMP_ImportNSI_Report_OEK_IntegralAct 
					where 
					ImportNumber= @ImportNumber
					and AllowLoad = 1 
					and TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID is not null
					--and HierLev1_ID = @H1DefaultID 
					and TEMP_ImportNSI_Report_OEK_IntegralAct.TRCoeff is not null
					and abs((Info_Transformators.COEFU*Info_Transformators.COEFI)-isnull(TEMP_ImportNSI_Report_OEK_IntegralAct.TRCoeff,1))>0.000000001 
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

	from TEMP_ImportNSI_Report_OEK_IntegralAct
	where 
	ImportNumber= @ImportNumber
	and AllowLoad=1 
	and TI_ID is not null 
	and not exists (
					--и нет коэффициентов в этом месяце...			
					select top 1 1 from Info_Transformators tr 
					where tr.TI_ID = TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID 
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
	TI_ID in (select TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID
				from TEMP_ImportNSI_Report_OEK_IntegralAct 
				where 
				ImportNumber= @ImportNumber
				and AllowLoad = 1 
				and TEMP_ImportNSI_Report_OEK_IntegralAct.TI_ID is not null
				--and HierLev1_ID = @H1DefaultID 
				and TEMP_ImportNSI_Report_OEK_IntegralAct.TRCoeff is not null
				--and abs((Info_Transformators.COEFU*Info_Transformators.COEFI)-isnull(TEMP_ImportNSI_Report_OEK_IntegralAct.TRCoeff,1))>0.000000001 
			 )
	and StartDateTime<@EventDateTime --начало < 01 числа
	and (FinishDateTime is null or FinishDateTime>=@EventDateTime) --но завершение больше 01 числа
	


	--расширяем последний для каждой из загружаемых ТИ период до 2100 года.
	update Info_Transformators
	set FinishDateTime= '01-01-2100'
	from
	Info_Transformators a
	where 
	a.TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
	and not exists (select top 1 1 from Info_Transformators b where b.TI_ID = a.TI_ID and b.StartDateTime>a.StartDateTime )
	and a.FinishDateTime is not null
	and a.FinishDateTime < '01-01-2100'


	--и надо будет сделать скрипт объединяющий одинаковые диапазоны (если между ними 1 сек!!)
	--(выбираем 2 записи min и max у которых даныне одинаковы и между ними нет других записей)
	--далее макс запись удаляем а мин запись расширяем до максимальной!!
	 
	 commit transaction;

end
 



--=============================================================================
--импорт РАСХОДа в акт недоучета 
--здесь пишем сразу итоговый расход так как коэфф потреь не отсутствует

set @msg=convert(varchar, getdate(),121)+ 'удаляем РАСХОД за месяц с этого источника для загружаемых ТИ' 
--print @msg


declare @commentString nvarchar(200)='(форма интегральный акт)'

if(@H1Defaultname = 'Физ. лица')
	set @commentString = '(форма физ.лица)'

begin Transaction ArchCalc_Replace_ActUndercount;	
 
	--для всех загружаемых ТИ удаляем источник потребитель за загружаемый период
	--в 1 и 5 зранилище
	delete from 
	ArchCalc_Replace_ActUndercount
	where 
	TI_ID in (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where 			
				ImportNumber= @ImportNumber and  AllowLoad=1 
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
	IsLossesCoefficientEnabled)
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
		  AddedValue= (case when isnull(IntegralValue_CommonExpense ,0)<0 then -1*isnull(IntegralValue_CommonExpense ,0)
						   else IntegralValue_CommonExpense end)
						    *res.Coeff
						   /  (case when isnull(res.TRCoeff,1)=0 then 1 else isnull(res.TRCoeff,1) end),


		  CommentString='импорт из файла '+@commentString+' '+convert(varchar,@DispDateTime,121),
		  User_ID='80004Q4WZ1350KO8NT59RM',
		  IsInactive=0,
		  CUS_ID=0,
		  --вычитание если отрицательное, замещение если положительное
		  Actmode= case when isnull(IntegralValue_CommonExpense ,0)<0 then 2
						   else 0 end,
			--конец периода вкл
		 IsFinishDateTimeInclusive=1

		from 
			TEMP_ImportNSI_Report_OEK_IntegralAct res
		where  
		ImportNumber= @ImportNumber and  AllowLoad=1 
		and	TI_ID is not null
		and res.ChannelType is  not null
		and res.IntegralValue_CommonExpense is not null
		and res.EventDatePrevious is not null
		and res.EventDateCurrent is not null
		--показаний нет!!
		and res.IntegralValue_Current is null
		--коэфф не импортируем здесь, поэтому берем только те строки где он не указан 
		and TRCoeff is null 

	) as rr


	set @msg=convert(varchar, getdate(),121)+ ' Импортировали расход' 
	--print @msg
	 

commit  Transaction ArchCalc_Replace_ActUndercount;	
 

--=============================================================================
----импорт коэфф ПОТЕРЬ Info_TI_LossesCoefficients
--ПОТЕРИ УЖЕ ПРИВЕДЕНЫ К коэффициенту!! должно быть например 1,02 это значит потери 2% (добавлены)

begin Transaction Info_TI_LossesCoefficients;	

delete from 
Info_TI_LossesCoefficients
where 
TI_ID in (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where 			
			ImportNumber= @ImportNumber and  AllowLoad=1 
					and	TI_ID is not null and IntegralValue_LossesPercent is not null)
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
	  LossesCoefficient = IntegralValue_LossesPercent	 
	from 
		TEMP_ImportNSI_Report_OEK_IntegralAct res
	where  
	ImportNumber= @ImportNumber and  AllowLoad=1 
	and	TI_ID is not null
	and ChannelType=1
	and res.IntegralValue_LossesPercent is not null
	and res.EventDatePrevious is not null
	and res.EventDateCurrent is not null
)
as a

commit  Transaction Info_TI_LossesCoefficients;	




--====================================================================
--КОРРЕКТИРУЕМ 
--====================================================================

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
a.TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)

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
a.TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)



----ПОСЛЕДНИЙ ПЕРИОД расширяем до 2100 года
update Info_Meters_TO_TI
set FinishDateTime= '01-01-2100'
--select * 
from
Info_Meters_TO_TI a
where 
a.TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
and not exists (select top 1 1 from Info_Meters_TO_TI b where b.TI_ID = a.TI_ID and b.StartDateTime>a.StartDateTime )
and a.FinishDateTime is not null
and a.FinishDateTime < '01-01-2100'

update Info_Transformators
set FinishDateTime= '01-01-2100'
from
Info_Transformators a
where 
a.TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
and not exists (select top 1 1 from Info_Transformators b where b.TI_ID = a.TI_ID and b.StartDateTime>a.StartDateTime )
and a.FinishDateTime is not null
and a.FinishDateTime < '01-01-2100'



--на всякий случай ставим дату завершения = конец месяца для ТИ у которых они некорректны
--при заменах почему то перепутаны были даты завершения 
--первый период (заканчивался на конец месяца) - корректируется выше
--второй (новый ПУ) - ставим конец месяца
update Info_Meters_TO_TI
set FinishDateTime=dateadd(s,-1,DATEADD(month, DATEDIFF(month, 1, StartDateTime)+1, 0))  
from Info_Meters_TO_TI 
where 
TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
and isnull(FinishDateTime,'01-01-2100')<StartDateTime

update Info_Transformators
set FinishDateTime=dateadd(s,-1,DATEADD(month, DATEDIFF(month, 1, StartDateTime)+1, 0))  
from Info_Transformators 
where 
TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
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
	and TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
    union all
    select t.TI_ID, t.METER_ID, cte.StartDateTime, dateadd(s,1,isnull(t.FinishDateTime,'01-01-2100')), cte.MetersReplaceSession_ID
    from cte
    join Info_meters_to_TI t on cte.TI_ID = t.TI_ID and t.METER_ID=cte.METER_ID and cte.FinishDateTime = t.StartDateTime
	where 
	t.MetersReplaceSession_ID not in (select MetersReplaceSession_ID from Info_Meters_ReplaceHistory_Channels)
	 --and t.TI_ID = 73606666
	and  t.TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)
	
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
and TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)

--добавляем
insert into Info_Meters_TO_TI (TI_ID,METER_ID,StartDateTime,FinishDateTime,MetersReplaceSession_ID,CUS_ID,SourceType)
select distinct TI_ID,Meter_ID,MinStartDateTime,MaxFinishDatetime,MetersReplaceSession_ID,0,0
from
	@tempDates
where 
	TI_ID In  (select TI_ID from TEMP_ImportNSI_Report_OEK_IntegralAct where ImportNumber= @ImportNumber and	TI_ID is not null)

--по ТР непрерывные диапазоны тое можно сделать со строгой проверкой ИДентификаторов и коэффициентов





--=========================================================================
--РЕЗУЛЬТАТ
 
select distinct 
	SheetNumber,
	SheetName=convert(varchar(200),SheetNumber), 
	RowID= RowNumber,
	Message='',
	ErrorMessage
from 
	TEMP_ImportNSI_Report_OEK_IntegralAct
where 
	Importnumber=@ImportNumber
	and
	  (AllowLoad=0 or isnull(ErrorMessage,'') <>'')


--очищаем таблицу
delete from TEMP_ImportNSI_Report_OEK_IntegralAct
where 
Importnumber=@ImportNumber


END

GO


grant execute on dbo.usp2_ImportNSI_Report_OEK_IntegralAct to UserCalcService
go

grant execute on dbo.usp2_ImportNSI_Report_OEK_IntegralAct to UserDeclarator
go







