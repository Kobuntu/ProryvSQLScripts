
if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usp2_Get_ExplDoc_Journal_Exchange_Root]')
          and type in ('P','PC'))
   drop procedure dbo.usp2_Get_ExplDoc_Journal_Exchange_Root
go


create procedure dbo.usp2_Get_ExplDoc_Journal_Exchange_Root
  @userId varchar(200)='',
  @startdatetime datetime = null,
  @enddatetime datetime = null,
  @DeadlineHourLocal int =10, 
  @DeadlineMinLocal int= 00,
  @FilterJobDataFormat tinyint= null

AS
BEGIN
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

 Declare @res Table 
 (ExportServiceJob_ID EXPORTSERVICEJOB_ID_TYPE, 
 JobDataFormat tinyint, 
 FileDataFormat tinyint, 
 StartDataPeriod datetime, 
 FinishDataPeriod_Exclude datetime, 
 CorrectStartEmailSending Datetime, 
 DeadlineEmailSending datetime, 
 EmailGroupNumber int, 
 JobStringName nvarchar(200), 
 Email_IsDeadlineCorrected int, 
 RealSendDateTime datetime, 
 FileId EXCHANGEDOCUMENT_ID_TYPE, 
 FileCreateDateTime datetime,
 FileType tinyint, 
 FileNumber int, 
 FileName nvarchar(200), 
 FileTICount int, 
 FileTINonCommertialCount int, 
 FileStatus bigint,
 FileMessageString nvarchar(400), 
 FileFullName nvarchar(800), 
 FileCheckSum  nvarchar(200), 
 FileSize float, 
 FileIsSigned tinyint,
 FileIsArchive tinyint,
 FileStatus_IsCreated int, 
 FileStatus_IsSent int, 
 FileStatus_AnswerLoaded int, 
 FileStatus_AnswerCorrect int, 
 FileStatus_IsNonCommerceNotExists int, 
 FileStatus_IsErorNotExists int, 
 Answer_FileId EXCHANGEDOCUMENT_ID_TYPE, 
 Answer_CreateDateTime datetime,
 Answer_FileName  nvarchar(200), 
 Answer_FullFileName nvarchar(800),  
 Answer_MessageString nvarchar(400) ,
 ParentTICount int,
 ParentTINonCommertialCount int)
Insert into @res 
 exec usp2_Get_ExplDoc_Journal_Exchange_Ext @userId, @startDateTime,@enddatetime, @DeadlineHourLocal ,  @DeadlineMinLocal 


 
select 
JobDataFormat,	
FileStatus_IsCreated		= case when FileStatus_IsCreated_ErrorCount=0 then 1 else 0 end,
FileStatus_IsSent		= case when FileStatus_IsSent_ErrorCount=0 then 1 else 0 end,
Email_IsDeadlineCorrected		= case when IsDeadlineViolated_ErrorCount=0 then 1 else 0 end,		
FileStatus_AnswerLoaded				= case when FileStatus_AnswerLoaded_ErrorCount=0 then 1 else 0 end,
FileStatus_AnswerCorrect			= case when FileStatus_AnswerCorrect_ErrorCount=0 then 1 else 0 end,
--Все данные коммерческие
FileStatus_IsNonCommerceNotExists	= case when FileStatus_IsCreated_ErrorCount=0 and FileStatus_IsNonCommerceNotExists_ErrorCount=0 
												then 1 else 0 end,
FileStatus_IsErorNotExists			=  case when 
												isnull(FileStatus_IsCreated_ErrorCount,0)=0 
												and  isnull(FileStatus_IsSent_ErrorCount,0)=0 
												and  isnull(FileStatus_AnswerLoaded_ErrorCount,0)=0 
												and  isnull(FileStatus_AnswerCorrect_ErrorCount,0)=0 
												and  isnull(FileStatus_IsNonCommerceNotExists_ErrorCount,0)=0 
												and  isnull(FileStatus_IsErorNotExists_ErrorCount,0)=0 
												and isnull(IsDeadlineViolated_ErrorCount,0)=0
												then 1
									else 0 end, 
									FileTICount
from 
(
	select distinct 
	JobDataFormat,	
	IsDeadlineViolated_ErrorCount	=  sum(cast(case when isnull(Email_IsDeadlineCorrected,0) =0 then 1 else 0 end as int)),
	FileStatus_IsCreated_ErrorCount	=  sum(cast(case when isnull(FileStatus_IsCreated,0) =0 then 1 else 0 end  as int)),
	FileStatus_IsSent_ErrorCount			=  sum(cast(case when isnull(FileStatus_IsSent,0) =0 then 1 else 0 end  as int)),
	FileStatus_AnswerLoaded_ErrorCount		=  sum(cast(case when isnull(FileStatus_AnswerLoaded,0) =0 then 1 else 0 end  as int)),
	FileStatus_AnswerCorrect_ErrorCount		=  sum(cast(case when isnull(FileStatus_AnswerCorrect,0) =0 then 1 else 0 end  as int)),
	FileStatus_IsNonCommerceNotExists_ErrorCount	=  sum(cast(case when isnull(FileStatus_IsNonCommerceNotExists,1) =0 then 1 else 0 end  as int)),
	FileStatus_IsErorNotExists_ErrorCount		=  sum(cast(case when isnull(FileStatus_IsErorNotExists,0) =0 then 1 else 0 end  as int)), 	
	FileTICount=sum(FileTICount)
	from
	(
	select * from @res
	where @FilterJobDataFormat is null or JobDataFormat=@FilterJobDataFormat
)
as FileResult
group by 
	JobDataFormat
)
as GroupResult

order by JobDataFormat


END
go

grant EXECUTE on dbo.usp2_Get_ExplDoc_Journal_Exchange_Root to UserDeclarator
go
grant EXECUTE on dbo.usp2_Get_ExplDoc_Journal_Exchange_Root to UserCalcService
go

 
		
