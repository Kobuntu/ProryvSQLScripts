IF   EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ExplDoc_Journal_Document_Exchange_TableType' AND ss.name = N'dbo')
drop type dbo.ExplDoc_Journal_Document_Exchange_TableType 
go

Create TYPE [dbo].ExplDoc_Journal_Document_Exchange_TableType 
AS TABLE
(
ExchangeDocument_ID	EXCHANGEDOCUMENT_ID_TYPE	,
CreateDateTime	datetime	,
DirectionExchange	tinyint	,
DataFormat	tinyint	,
FileNumber	int	,
FileName	varchar (200)	 null,
FileType	tinyint	,
StartDateTime	datetime	,
FinishDateTime	datetime	 ,
User_ID	ABS_NUMBER_TYPE_2	 null,
HostName	varchar (200)	 null,
Status	tinyint	,
MessageString	varchar	(400),
CUS_ID	CUS_ID_TYPE	,
ParentExchangeDocument_ID	EXCHANGEDOCUMENT_ID_TYPE	 null,
ExportServiceJob_ID	EXPORTSERVICEJOB_ID_TYPE	 null,
TICount	int	 null,
TIWithErrorCount	int	 null,
TINonCommertialCount	int	 null,

DirectionExchangeStringName varchar (200)	 null,
DataFormatStringName varchar (200)	 null,
FileTypeStringName varchar (200)	 null,
        
StatusBegin bit null,
StatusFileCreatedOrReceived bit null,
StatusFileSendOrLoaded bit null,
StatusAnswerLadedOrCreated bit null,
StatusAnswerCorrect bit null,
StatusIsNonCommerceInfoExists bit null,
StatusIsErorExists bit null,
StatusEnd bit null,
ParentExchangeDocumentStringName varchar(200)	 null,
ExportServiceJobStringName varchar (200)	 null,
UserFullName varchar (200)	 null


PRIMARY KEY  
(
	ExchangeDocument_ID
)
)
GO

  

grant EXECUTE on TYPE::ExplDoc_Journal_Document_Exchange_TableType to [UserCalcService]
go
grant EXECUTE on TYPE::ExplDoc_Journal_Document_Exchange_TableType to [UserDeclarator]
go
grant EXECUTE on TYPE::ExplDoc_Journal_Document_Exchange_TableType to [UserExportService]
go


 if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Get_ExplDoc_Journal_Document_Exchange')
          and type in ('P','PC'))
   drop procedure usp2_Get_ExplDoc_Journal_Document_Exchange
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].usp2_Get_ExplDoc_Journal_Document_Exchange
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
  
set dateformat dmy

if @applicationType=255
set @applicationType=null

if (isnull(@userId,'') like '')
set @userId=null

	select distinct top 10000
	item.ExchangeDocument_ID,
	item.CreateDateTime,
	item.DirectionExchange,
	item.DataFormat,
	item.FileNumber,
	item.FileName,
	item.FileType,
	item.StartDateTime,
	FinishDateTime= case when item.FinishDateTime is not null then item.FinishDateTime else item.StartDateTime end,
	item.User_ID,
	item.HostName,
	item.Status,
	item.MessageString,
	item.CUS_ID,
	item.ParentExchangeDocument_ID,
	item.ExportServiceJob_ID,
	item.TICount,
	item.TIWithErrorCount,
	item.TINonCommertialCount,
	
	DirectionExchangeStringName = case when isnull(item.DirectionExchange,0)=0 then 'Экспорт' 
									when isnull(item.DirectionExchange,0)=1 then 'Импорт' 
									else 'нет данных' end,
 								
	DataFormatStringName = case when isnull(item.DataFormat,0)=0 then '80020' 
									when isnull(item.DataFormat,0)=1 then 'показания' 
									when isnull(item.DataFormat,0)=2 then '51070' 

									when isnull(item.DataFormat,0)=4 then '80040' 
									when isnull(item.DataFormat,0)=5 then 'АСКП' 
									when isnull(item.DataFormat,0)=6 then '80030' 
									when isnull(item.DataFormat,0)=7 then '80021' 
									when isnull(item.DataFormat,0)=8 then '80050' 
									when isnull(item.DataFormat,0)=50 then 'НСИ' 
									when isnull(item.DataFormat,0)=51 then 'своб иерархия' 
									when isnull(item.DataFormat,0)=255 then 'другое' 
									else 'нет данных' end,
									
	FileTypeStringName = case when isnull(item.FileType,0)=0 then 'XML' 
									when isnull(item.FileType,0)=1 then 'Excel' 
									when isnull(item.FileType,0)=2 then 'TXT' 
									when isnull(item.FileType,0)=3 then 'ZIP' 
									when isnull(item.FileType,0)=255 then 'другое' 
									else 'нет данных' end,

	--начало - всегда 1
	StatusBegin = cast( 1 as bit),
	StatusFileCreatedOrReceived = cast((case when isnull(item.status,0)&1 = 1 then 	 1  else 0 end) as bit),
	StatusFileSendOrLoaded = cast((case when isnull(item.status,0)&2 = 2 then 	 1  else 0 end) as bit),
	StatusAnswerLadedOrCreated = cast((case when isnull(item.status,0)&4 = 4 then 	 1  else 0 end) as bit),
	StatusAnswerCorrect = cast((case when isnull(item.status,0)&8 = 8 then 	 1  else 0 end) as bit),
	StatusIsNonCommerceInfoExists = cast((case when isnull(item.status,0)&32 = 32 then 	 1  else 0 end) as bit),
	StatusIsErorExists = cast((case when isnull(item.status,0)&64 = 64 then 	 1  else 0 end) as bit),
	StatusEnd = cast((case when isnull(item.status,0)&128 = 128 then 	 1  else 0 end) as bit),

	UserFullName= case when users.User_ID is not null then users.UserFullName else '' end,
	ParentExchangeDocumentStringName= case when parent.FileName is null then '' else parent.FileName end,
	ExportServiceJobStringName= isnull(job.StringName,'')


	from 
	ExplDoc_Journal_Document_Exchange item
	left join Expl_Users users on item.[User_ID]= users.[User_ID]
	left join ExplDoc_Journal_Document_Exchange parent on parent.ExchangeDocument_ID= Item.ParentExchangeDocument_ID
	left join Expl_XML_ExportServiceJob job on job.ExportServiceJob_ID= Item.ExportServiceJob_ID
	where 
	item.CreateDateTime>=@startdatetime
	and 
	item.CreateDateTime<=@enddatetime	
	and ((isnull(@userId ,'') like '' or item.[User_ID] = @userId ))
	
	and ((isnull(@eventstring,'') like ''  
		or (isnull(item.[FileName],'')+isnull(parent.FileName,'')) like '%'+@eventstring+'%'))

	order by item.CreateDateTime desc


END
GO

grant EXECUTE on usp2_Get_ExplDoc_Journal_Document_Exchange to [UserCalcService]
go
grant EXECUTE on usp2_Get_ExplDoc_Journal_Document_Exchange to [UserDeclarator]
go
grant EXECUTE on usp2_Get_ExplDoc_Journal_Document_Exchange to [UserExportService]
go








--удаляем (не используем более)
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Get_ExplDoc_Journal_JobFiles')
          and type in ('P','PC'))
   drop procedure [usp2_Get_ExplDoc_Journal_JobFiles]
go


if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Get_ExportJob_Emails')
          and type in ('P','PC'))
   drop procedure usp2_Get_ExportJob_Emails
go

create procedure [dbo].usp2_Get_ExportJob_Emails

 @JobId int, @StartPeriod datetime, @EndPeriod datetime, @UseSendDatetime bit=1

 as 
 begin
  
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
 
if (@UseSendDatetime=1 and @EndPeriod= @StartPeriod)
set @EndPeriod= dateadd(d,1, @StartPeriod)



select 
JobName=Expl_XML_ExportServiceJob.StringName,
Expl_XML_ExportServiceJob.ExportServiceJob_ID,
ExplDoc_Journal_Email_Exchange.GroupNumber,
ExplDoc_Journal_Email_Exchange.EmailNumber,
ExplDoc_Journal_Email_Exchange.EmailSubject,
ExplDoc_Journal_Email_Exchange.EmailTo,
ExplDoc_Journal_Email_Exchange.ResultMessageString,
ExplDoc_Journal_Email_Exchange.DispatchDateTime,
ExplDoc_Journal_Email_Exchange.ExchangeEmail_ID,
 
 --переворачиваем для файлов неправильные статусы в 1,а правильные статусы в 0
 --суммируем, если хотя бы один файл некорректне (есть 1 и более единичек (Сумма>0)) то ошибку на все письмо



		FileStatus_IsCreated	=  
		cast(case when
			sum(
				case when 
						isnull((
								case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&1 = 1 
										then 	1  else 0 end) 
								,0) =0 
						then 1 else 0 end  			
				) 
				>0
			then 0 else 1 end
			as bit),


		FileStatus_IsSent			=  cast(case when
		sum(cast(case when isnull( cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&2 = 2 	 then 	1  else 0 end)as bit),0) =0 then 1 else 0 end  as int))>0
			then 0 else 1 end
			as bit),
			 
		FileStatus_AnswerLoaded		= 
		 cast(case when
				sum(cast(case when isnull(cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&4 = 4 then 	1 else 0 end)as bit),0) =0 then 1 else 0 end  as int))
					>0
			then 0 else 1 end
			as bit),


		FileStatus_AnswerCorrect		= cast(case when
		 sum(cast(case when isnull(cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&8 = 8 then 	1  else 0 end)as bit),0) =0 then 1 else 0 end  as int))	>0
			then 0 else 1 end
			as bit),

		FileStatus_IsNonCommerceNotExists	= cast(case when
		 sum(cast(case when isnull(cast ((case when isnull(ExplDoc_Journal_Document_Exchange.TINonCommertialCount,0)>0 
	or isnull(ExplDoc_Journal_Document_Exchange.status,0)&32 = 32 then 0  else 1 end)as bit),1) =0 then 1 else 0 end  as int))	>0
			then 0 else 1 end
			as bit),

		FileStatus_IsErorNotExists		=  
		cast(case when
			sum(cast(case when isnull(
				cast ((case when
					isnull(ExplDoc_Journal_Document_Exchange.Status,0)&64 = 64  -- если есть ошибка (текст)
					or 
					(
					(case when isnull(ExplDoc_Journal_Document_Exchange.TINonCommertialCount,0)>0 or isnull(ExplDoc_Journal_Document_Exchange.status,0)&32 = 32 then 0  else 1 end) =0
					or
					 isnull(ExplDoc_Journal_Document_Exchange.status,0)&1 = 0 
					or isnull(ExplDoc_Journal_Document_Exchange.status,0)&2 = 0
					or isnull(ExplDoc_Journal_Document_Exchange.status,0)&4 = 0 
					or isnull(ExplDoc_Journal_Document_Exchange.status,0)&8 = 0	)
					then 0 else 1 end)	as bit)		
			,0) =0 then 1 else 0 end  as int))		>0
			then 0 else 1 end
			as bit)

from Expl_XML_ExportServiceJob
join ExplDoc_Journal_Email_Exchange on Expl_XML_ExportServiceJob.ExportServiceJob_ID=ExplDoc_Journal_Email_Exchange.ExportServiceJob_ID
join ExplDoc_Journal_Document_Exchange on ExplDoc_Journal_Email_Exchange.ExchangeEmail_ID=ExplDoc_Journal_Document_Exchange.ExchangeEmail_ID
where
Expl_XML_ExportServiceJob.ExportServiceJob_ID= @JobId
and 
(
	(
		@UseSendDatetime = 1
		and (	ExplDoc_Journal_Email_Exchange.DispatchDateTime >=@StartPeriod	and  ExplDoc_Journal_Email_Exchange.DispatchDateTime <@EndPeriod	)
	)
	or 
	 (
		@UseSendDatetime=0
		
		and 
		(
		(ExplDoc_Journal_Document_Exchange.StartDateTime >=@StartPeriod	and  ExplDoc_Journal_Document_Exchange.StartDateTime<=@EndPeriod	)
		or
		(@StartPeriod >=ExplDoc_Journal_Document_Exchange.StartDateTime	and  @StartPeriod<=ExplDoc_Journal_Document_Exchange.FinishDateTime	)
		)

	 )
)
group by

Expl_XML_ExportServiceJob.StringName,
Expl_XML_ExportServiceJob.ExportServiceJob_ID,
ExplDoc_Journal_Email_Exchange.GroupNumber,
ExplDoc_Journal_Email_Exchange.EmailNumber,
ExplDoc_Journal_Email_Exchange.EmailSubject,
ExplDoc_Journal_Email_Exchange.EmailTo,
ExplDoc_Journal_Email_Exchange.ResultMessageString,
ExplDoc_Journal_Email_Exchange.DispatchDateTime,
ExplDoc_Journal_Email_Exchange.ExchangeEmail_ID 


order by ExplDoc_Journal_Email_Exchange.DispatchDateTime

END
GO

grant EXECUTE on usp2_Get_ExportJob_Emails to UserCalcService
go
grant EXECUTE on usp2_Get_ExportJob_Emails to UserDeclarator
go



if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Get_ExportJob_Files')
          and type in ('P','PC'))
   drop procedure usp2_Get_ExportJob_Files
go

create procedure [dbo].usp2_Get_ExportJob_Files
 @ParentType int, --0-Email, 1-File
 @ParentID int

 as 
 begin
  
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
   

select 

ExplDoc_Journal_Document_Exchange.FileName,
ExplDoc_Journal_Document_Exchange.StartDateTime,
ExplDoc_Journal_Document_Exchange.FinishDateTime,
ExplDoc_Journal_Document_Exchange.TICount,
ExplDoc_Journal_Document_Exchange.TINonCommertialCount,
FileSize=round(ExplDoc_Journal_Document_Exchange.FileSize,1),
ExplDoc_Journal_Document_Exchange.IsSigned,
ExplDoc_Journal_Document_Exchange.Status,
ExplDoc_Journal_Document_Exchange.MessageString,
ExplDoc_Journal_Document_Exchange.FullFileName,
ExplDoc_Journal_Document_Exchange.CreateDateTime ,
ExplDoc_Journal_Document_Exchange.ExchangeDocument_ID,
	DirectionExchangeStringName = case when isnull(ExplDoc_Journal_Document_Exchange.DirectionExchange,0)=0 then 'Экспорт' 
									when isnull(ExplDoc_Journal_Document_Exchange.DirectionExchange,0)=1 then 'Импорт' 
									else 'нет данных' end,
 								
	DataFormatStringName = case when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=0 then '80020' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=1 then 'показания' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=2 then '51070' 

									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=4 then '80040' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=5 then 'АСКП' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=6 then '80030' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=7 then '80021' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=8 then '80050' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=50 then 'НСИ' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=51 then 'своб иерархия' 
									when isnull(ExplDoc_Journal_Document_Exchange.DataFormat,0)=255 then 'другое' 
									else 'нет данных' end,
									
	FileTypeStringName = case when isnull(ExplDoc_Journal_Document_Exchange.FileType,0)=0 then 'XML' 
									when isnull(ExplDoc_Journal_Document_Exchange.FileType,0)=1 then 'Excel' 
									when isnull(ExplDoc_Journal_Document_Exchange.FileType,0)=2 then 'TXT' 
									when isnull(ExplDoc_Journal_Document_Exchange.FileType,0)=3 then 'ZIP' 
									when isnull(ExplDoc_Journal_Document_Exchange.FileType,0)=255 then 'другое' 
									else 'нет данных' end,
 
	StatusFileCreatedOrReceived	= cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&1 = 1 then 	1  else 0 end) as bit),
	StatusFileSendOrLoaded			= cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&2 = 2 	 then 	1  else 0 end)as bit),
	StatusAnswerLadedOrCreated		= cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&4 = 4 then 	1 else 0 end)as bit),	
	StatusAnswerCorrect	= cast ((case when isnull(ExplDoc_Journal_Document_Exchange.status,0)&8 = 8 then 	1  else 0 end)as bit),
	--если кривой файл то ExplDoc_Journal_Document_Exchange.TINonCommertialCount может быть Null тогда тоже выводим как ошибку FileStatus_IsNonCommerceNotExists=0
	--isnull(ExplDoc_Journal_Document_Exchange.TINonCommertialCount,0)>0
	StatusIsNonCommerceInfoExists	= cast ((case when isnull(ExplDoc_Journal_Document_Exchange.TINonCommertialCount,0)>0 or isnull(ExplDoc_Journal_Document_Exchange.status,0)&32 = 32 then 0  else 1 end)as bit),
	StatusIsErorExists		= cast ((case when
	isnull(ExplDoc_Journal_Document_Exchange.status,0)&64 = 64  -- если есть ошибка (текст)
	or 
	(
	(case when isnull(ExplDoc_Journal_Document_Exchange.TINonCommertialCount,0)>0 or isnull(ExplDoc_Journal_Document_Exchange.status,0)&32 = 32 then 0  else 1 end) =0
	or
	 isnull(ExplDoc_Journal_Document_Exchange.status,0)&1 = 0 
	or isnull(ExplDoc_Journal_Document_Exchange.status,0)&2 = 0
	or isnull(ExplDoc_Journal_Document_Exchange.status,0)&4 = 0 
	or isnull(ExplDoc_Journal_Document_Exchange.status,0)&8 = 0	)
	then 0 else 1 end)	as bit)


from 
 ExplDoc_Journal_Document_Exchange 
where
--0-Email, 1-File
(
	(@Parenttype = 0 and isnull(ExplDoc_Journal_Document_Exchange.ExchangeEmail_ID,0)=@ParentID and ExplDoc_Journal_Document_Exchange.DirectionExchange=0 )
	or 
	(@Parenttype = 1 and isnull(ExplDoc_Journal_Document_Exchange.ParentExchangeDocument_ID,0)=@ParentID)
)

----только отправленные..
order by ExplDoc_Journal_Document_Exchange.CreateDateTime

END
GO


grant EXECUTE on usp2_Get_ExportJob_Files to UserCalcService
go
grant EXECUTE on usp2_Get_ExportJob_Files to UserDeclarator
go

