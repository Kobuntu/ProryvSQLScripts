IF  not EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'Expl_User_Journal_TableType' AND ss.name = N'dbo')
Create TYPE [dbo].Expl_User_Journal_TableType 
AS TABLE
(
User_ID	ABS_NUMBER_TYPE_2,
UserFullName nvarchar(255),

EventDateTime	datetime,

EventString	nvarchar(510),
CommentString	nvarchar(510),

ApplicationType	smallint,
ApplicationTypeStringName nvarchar(255),

EventCategory	tinyint,
EventType	tinyint,
EventTypeStringName nvarchar(255),

CUS_ID	CUS_ID_TYPE,

ObjectStringID	varchar(255),
ObjectStringType	nvarchar(510),
ObjectStringTypeRus nvarchar(255),
ObjectStringName nvarchar(255),

ParentObjectStringID	varchar(255),
ParentObjectStringType	nvarchar(510),
ParentObjectStringTypeRus nvarchar(255),
ParentObjectStringName nvarchar(255)
PRIMARY KEY  
(
	User_ID ASC,
	EventDateTime ASC
)
)
GO

grant EXECUTE on TYPE::Expl_User_Journal_TableType to [UserCalcService]
go
grant EXECUTE on TYPE::Expl_User_Journal_TableType to [UserDeclarator]
go
grant EXECUTE on TYPE::Expl_User_Journal_TableType to [UserExportService]
go


 if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Get_Expl_User_Journal')
          and type in ('P','PC'))
   drop procedure usp2_Get_Expl_User_Journal
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].usp2_Get_Expl_User_Journal
				 @isEasySelect bit,
				 @startdatetime datetime,
				 @enddatetime datetime,
				 @applicationType int,
				 @userId varchar(200),
				 @objectType nvarchar(255),
				 @objectStringId nvarchar(255),
				 @eventstring nvarchar(255)

AS BEGIN
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
  
--declare @startdatetime datetime= '01-01-2012'
--declare @enddatetime datetime= '01-01-2017'
--declare @applicationType int=null
--declare @objectType varchar(200)  ='Expl_XML_ExportServiceJob'
--declare @objectStringId varchar(200) ='1'
--declare @isEasySelect bit=0

set dateformat dmy

if @applicationType=255
set @applicationType=null

if (isnull(@userId,'') like '')
set @userId=null


declare @tempObjTypeTable table (objType varchar(200))
delete from @tempObjTypeTable

insert into @tempObjTypeTable
values (@objectType)

--добавляем варианты описания событий для счетчика
if (@objectType like 'Hard_meter')
	begin
	insert into @tempObjTypeTable
		values ('Hard_Meters'),('Hard_Meters_Passports'),('Hard_MetersE422_Link'),( 'Hard_MetersUSPD_Link'), ('Info_Meters_TO_TI'), ('Metrostandart.Askue.Data.Hard_Meter'),('Metrostandart.Askue.Data.Hard_MetersUSPD_Link'), ('Proryv.Askue.Data.Hard_Meter') , ('Proryv.Askue.Data.Hard_MetersUSPD_Link')
	end
--аналогично для тарифа, ТИ, ПС


	--упрощенный вариант (надо добавить название объекта отдельно)
	select top 100000
	users.[User_ID],
	users.UserFullName,
	Expl_User_Journal.EventDateTime,

	EventString=replace(replace(isnull(Expl_User_Journal.EventString,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),
	CommentString=replace(replace(isnull(Expl_User_Journal.CommentString,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),

	Expl_User_Journal.ApplicationType,
	ApplicationTypeStringName= 
	case 
	when Expl_User_Journal.ApplicationType =0 then 'Служба расчетов'
	when Expl_User_Journal.ApplicationType =1 then 'АРМ энергетика'
	when Expl_User_Journal.ApplicationType =2 then 'Описатель'
	when Expl_User_Journal.ApplicationType =3 then 'Обработка данных'
	when Expl_User_Journal.ApplicationType =4 then 'Служба рассылки'
	when Expl_User_Journal.ApplicationType =5 then 'Служба импорта почты'
	when Expl_User_Journal.ApplicationType =6 then 'Мгновенные значения'
	when Expl_User_Journal.ApplicationType =7 then 'Подсистема мониторинга'
	when Expl_User_Journal.ApplicationType =8 then 'Репликация'
	when Expl_User_Journal.ApplicationType =9 then 'Сбор данных'
	when Expl_User_Journal.ApplicationType =10 then 'Служба уведомления пользователей'
	when Expl_User_Journal.ApplicationType =13 then 'Служба очистки таблиц'
	when Expl_User_Journal.ApplicationType =20 then 'Репликация справочников'
	else 'нет данных' end,

	Expl_User_Journal.EventCategory,
	Expl_User_Journal.EventType,
	EventTypeStringName= 
	case 
	when Expl_User_Journal.EventType =0 then 'добавление'
	when Expl_User_Journal.EventType =1 then 'редактирование'
	when Expl_User_Journal.EventType =2 then 'удаление'
	when Expl_User_Journal.EventType =4 then 'изменение статуса'
	when Expl_User_Journal.EventType =5 then 'просмотр'
	when Expl_User_Journal.EventType =255 then 'ошибка'
	when Expl_User_Journal.EventType =254 then 'предупреждение'
	else 'нет данных' end,
		
	Expl_User_Journal.CUS_ID,

	ObjectStringID=Expl_User_Journal.ObjectID,
	ObjectStringType=replace(replace(isnull(Expl_User_Journal.ObjectName,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),	
	ObjectStringTypeRus  =replace(replace(isnull(Expl_User_Journal.ObjectName,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),
	ObjectStringName  = replace(replace(isnull(Expl_User_Journal.ObjectName,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),

	
	ParentObjectStringID=Expl_User_Journal.ParentObjectID,
	ParentObjectStringType=replace(replace(isnull(Expl_User_Journal.ParentObjectName,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),
	ParentObjectStringTypeRus =replace(replace(isnull(Expl_User_Journal.ParentObjectName,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.',''),
	ParentObjectStringName =replace(replace(isnull(Expl_User_Journal.ParentObjectName,''),'Metrostandart','Proryv'),'Proryv.Askue.Data.','')

	from 
	Expl_User_Journal

	 join Expl_Users users on Expl_User_Journal.[User_ID]= users.[User_ID]

	where 
	Expl_User_Journal.EventDateTime>=@startdatetime
	and 
	Expl_User_Journal.EventDateTime<=@enddatetime
	and (@applicationType is null or Expl_User_Journal.ApplicationType=@applicationType)
	and (@userId is null or Expl_User_Journal.[User_ID]=@userId)
	and 
	(
		isnull(@objectType,'') like '' 
		or   
		(
			(isnull(isnull(Expl_User_Journal.ObjectName,''),'') in (select * from @tempObjTypeTable)and isnull(Expl_User_Journal.ObjectID,'') like @objectStringId)
			or
			(isnull(isnull(Expl_User_Journal.ParentObjectName,''),'') in (select * from @tempObjTypeTable) and isnull(Expl_User_Journal.ParentObjectID,'') like @objectStringId)
		)
		or
		(
			isnull(Expl_User_Journal.CommentString,'') like '%'+@objectType+'_~'+@objectStringId
		)
	)
	and (isnull(@eventstring,'') like ''  
		or (isnull(Expl_User_Journal.CommentString,'') like '%'+@eventstring+'%')
		or (isnull(Expl_User_Journal.EventString,'') like '%'+@eventstring+'%'))
	order by EventDateTime desc


END
GO

grant EXECUTE on usp2_Get_Expl_User_Journal to [UserCalcService]
go
grant EXECUTE on usp2_Get_Expl_User_Journal to [UserDeclarator]
go
grant EXECUTE on usp2_Get_Expl_User_Journal to [UserExportService]
go



