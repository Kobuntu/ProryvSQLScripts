if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usp2_Get_ExplDoc_Journal_Exchange_Ext]')
          and type in ('P','PC'))
   drop procedure dbo.usp2_Get_ExplDoc_Journal_Exchange_Ext
go


create procedure dbo.usp2_Get_ExplDoc_Journal_Exchange_Ext 
  @userId varchar(200)='',
  @startdatetime datetime = null,
  @enddatetime datetime = null,
  @DeadlineHourLocal int =10, 
  @DeadlineMinLocal int= 00,
  @FilterJobID int = null,
  @FilterEmailGroupNumber int = null

AS
BEGIN
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED


print convert(varchar,getdate(),121)+ ': ������'

--������ �� ������� ����������� (��� ������ ������������ - ����� ������ ��������� - �������� �� �����)
set @startdatetime = CAST(FLOOR(CAST( isnull(@startdatetime ,getdate()) as FLOAT))as datetime) 
set @enddatetime = CAST(FLOOR(CAST( isnull(@enddatetime ,getdate()) as FLOAT))as datetime)  

declare @tempStartDatetime datetime =@startdatetime
declare @tempEndDatetime datetime = DATEADD(d,1,@tempStartDatetime)

 
create table #tempPeriodTable  (JobDataFormat tinyint,FileDataFormat tinyint, StartDataPeriod datetime,FinishDataPeriod_Exclude datetime, CorrectStartEmailSending datetime, DeadlineEmailSending datetime)
create index #tempPeriodTable_1 on #tempPeriodTable
(JobDataFormat)
create index #tempPeriodTable_2 on #tempPeriodTable
(StartDataPeriod)


while @tempStartDatetime<=@enddatetime
begin
	--� ��� ����� �������� � ������������ ������� ��������� �������
	--��� 80020 
	--������ ������ ������ � ���������� ������, 
	--� ������ ������ ���� ���������� �� ���� �����, �� ����������� ��������� �������� � ������� 3 �����.. ������� ������ ������� �� 3 ��� ������ �� ������������ �������
	insert into #tempPeriodTable (JobDataFormat,FileDataFormat, StartDataPeriod,FinishDataPeriod_Exclude,CorrectStartEmailSending,DeadlineEmailSending)
	values(0,0, @tempStartDatetime, @tempEndDatetime , @tempEndDatetime,
	 dateadd(mi,3*24*60+@DeadlineHourLocal*60+@DeadlineMinLocal, 
	 @tempEndDatetime)) 
	 	
	--80040 
	--������� � ��� ��� ������� ��������� 3 ����� ���� ������ � @DeadlineHourLocal+���??
	--� ���������� ������ ����� ���� �� ���������� ��� �� ��������, ���� � ������ ���� ������.. 
	--�.�. ��������� ������ �� ���� ��� @tempEndDatetime �� �������
	insert into #tempPeriodTable (JobDataFormat,FileDataFormat, StartDataPeriod,FinishDataPeriod_Exclude,CorrectStartEmailSending,DeadlineEmailSending)
	values(1,4, @tempStartDatetime, @tempEndDatetime , @tempEndDatetime, 
			 dateadd(m,1, DATEADD(mi,3*24*60+@DeadlineHourLocal*60+@DeadlineMinLocal,
						CAST(CAST(year(@tempStartDatetime) AS VARCHAR(4)) +
							RIGHT('0' + CAST( MONTH(@tempStartDatetime) AS VARCHAR(2)), 2) +
							RIGHT('0' + CAST(1 AS VARCHAR(2)), 2) AS DATETIME
							)
					))
			) 


	set @tempStartDatetime=@tempEndDatetime
	set @tempEndDatetime = DATEADD(d,1,@tempStartDatetime)
end








print convert(varchar,getdate(),121)+ ': ������� � ������ - �����'


print convert(varchar,getdate(),121)+ ': #tempJobPeriodTable - ������'


create table #tempJobPeriodTable  
(ExportServiceJob_ID int,
 JobDataFormat tinyint,FileDataFormat tinyint, StartDataPeriod datetime,FinishDataPeriod_Exclude datetime, CorrectStartEmailSending datetime, DeadlineEmailSending datetime)

 
create index #tempJobPeriodTable_1 on #tempJobPeriodTable
(ExportServiceJob_ID)



insert into #tempJobPeriodTable 
(ExportServiceJob_ID,JobDataFormat ,FileDataFormat , StartDataPeriod,FinishDataPeriod_Exclude,CorrectStartEmailSending,DeadlineEmailSending)
select 
ExportServiceJob_ID,tempperiod.JobDataFormat, tempperiod.FileDataFormat, StartDataPeriod,FinishDataPeriod_Exclude,CorrectStartEmailSending,DeadlineEmailSending
from  
Expl_XML_ExportServiceJob, #tempPeriodTable  tempperiod
where 
(@FilterJobID is null or @FilterJobID=ExportServiceJob_ID )
and Expl_XML_ExportServiceJob.DataFormat = tempperiod.JobDataFormat
and ParentExportServiceJob_ID is null


print convert(varchar,getdate(),121)+ ': #tempJobPeriodTable - �����'


print convert(varchar,getdate(),121)+ ': #tempLastEmailTable - ������'

--������� ��������� ������ ����� �� ������� ������� �� ������ ����������� ����
create table #tempLastEmailTable  (
	ExportServiceJob_ID int,
	JobDataFormat tinyint,FileDataFormat tinyint,StartDataPeriod datetime,FinishDataPeriod_Exclude datetime, CorrectStartEmailSending datetime, DeadlineEmailSending datetime,
	EmailGroupNumber int)

insert into #tempLastEmailTable(
	ExportServiceJob_ID ,
	JobDataFormat ,FileDataFormat ,StartDataPeriod ,FinishDataPeriod_Exclude , CorrectStartEmailSending , DeadlineEmailSending ,
	EmailGroupNumber )
select distinct
	job.ExportServiceJob_ID ,
	job.JobDataFormat,job.FileDataFormat,job.StartDataPeriod,job.FinishDataPeriod_Exclude , job.CorrectStartEmailSending , job.DeadlineEmailSending,
	EmailGroupNumber=max(email.GroupNumber)
from 
#tempJobPeriodTable job
left join  ExplDoc_Journal_Email_Exchange   email
	on
		job.ExportServiceJob_ID=email.ExportServiceJob_ID
		and email.DirectionExchange=0		 
		and (@FilterEmailGroupNumber is null or email.GroupNumber= @FilterEmailGroupNumber	)

		--����� ��� ������ �� ���������� ������, ���� ������ ������� ���� ����� (��� ��� ������ ������� � ��������� �����)
		and	(email.DispatchDateTime>=job.CorrectStartEmailSending)
			

		
		and (
			--� ���� xml-����� ��������������� � ����� ������  �� ��� ����
			exists (select top 1 1 from ExplDoc_Journal_Document_Exchange emailDoc
					where 
					emailDoc.ExchangeEmail_ID=email.ExchangeEmail_ID 	 
					and emailDoc.DirectionExchange=0
					and emailDoc.DataFormat = job.FileDataFormat 
					and emailDoc.StartDateTime= job.StartDataPeriod 
					and emailDoc.FileType=0
					)
			--��� ������, ���������� xml-����� �� ��������� ����
			or exists (select top 1 1 from 
						ExplDoc_Journal_Document_Exchange emailDoc 
						join ExplDoc_Journal_Document_Exchange zipDoc on zipDoc.ExchangeDocument_ID= emailDoc.ParentExchangeDocument_ID
					where 
					zipDoc.ExchangeEmail_ID=email.ExchangeEmail_ID 	 
					and zipDoc.DirectionExchange=0
					and zipDoc.DataFormat = job.FileDataFormat 
					and zipDoc.FileType=3
					and emailDoc.DirectionExchange=0
					and emailDoc.DataFormat = job.FileDataFormat 
					and emailDoc.StartDateTime= job.StartDataPeriod 
					and emailDoc.FileType=0
					)
			)
where
 @FilterEmailGroupNumber is null 
or email.GroupNumber= @FilterEmailGroupNumber		
group by 
job.ExportServiceJob_ID,job.JobDataFormat, job.FileDataFormat, StartDataPeriod,FinishDataPeriod_Exclude,CorrectStartEmailSending,DeadlineEmailSending

print convert(varchar,getdate(),121)+ ': #tempLastEmailTable - �����'






	--���-�� �� ������� �� �������������, ���� �� ����
	--������� ���� �� ������
	select distinct 
	groupEmailNumber.ExportServiceJob_ID , 
	groupEmailNumber.JobDataFormat , 
	groupEmailNumber.FileDataFormat, 
	StartDataPeriod , 
	FinishDataPeriod_Exclude , 
	CorrectStartEmailSending , 
	DeadlineEmailSending , 
	EmailGroupNumber , 
	JobStringName=job.StringName,
	Email_IsDeadlineCorrected = case when (email.DispatchDateTime<=groupEmailNumber.DeadlineEmailSending) then 1 else 0 end,
	RealSendDateTime= email.DispatchDateTime,
	FileId=emailDoc.ExchangeDocument_ID,
	FileCreateDateTime=emailDoc.CreateDateTime,
	emailDoc.FileType,
	emailDoc.FileNumber, 
	emailDoc.FileName,
	FileTICount=emailDoc.TICount,
	FileTINonCommertialCount=isnull(emailDoc.TINonCommertialCount,0),
	FileStatus=cast(emailDoc.Status as bigint), 
	FileMessageString= emailDoc.MessageString, 
	FileFullName=isnull(emailDoc.FullFileName,parentEmailDoc.FullFileName), 
	FileCheckSum=isnull(emailDoc.FileCheckSum,parentEmailDoc.FileCheckSum), 
	FileSize=round(isnull(emailDoc.FileSize,parentEmailDoc.FileSize),1), 
	FileIsSigned= cast (isnull(emailDoc.IsSigned,parentEmailDoc.IsSigned) as int),
	FileIsArchive = case when isnull(emailDoc.FileType,0)=3 or isnull(parentEmailDoc.FileType,0)=3 then 1 else 0 end,
 
	--������� - 
	FileStatus_IsCreated	= (case when isnull(emailDoc.status,0)&1 = 1 then 	1  else 0 end),
	FileStatus_IsSent			= (case when isnull(emailDoc.status,0)&2 = 2 or isnull(parentEmailDoc.status,0)&2 = 2  then 	1  else 0 end), 
	FileStatus_AnswerLoaded		= (case when isnull(emailDoc.status,0)&4 = 4 then 	1 else 0 end),		
	FileStatus_AnswerCorrect	= (case when isnull(emailDoc.status,0)&8 = 8 then 	1  else 0 end),	
	--���� ������ ���� �� emailDoc.TINonCommertialCount ����� ���� Null ����� ���� ������� ��� ������ FileStatus_IsNonCommerceNotExists=0
	--isnull(emailDoc.TINonCommertialCount,0)>0
	FileStatus_IsNonCommerceNotExists	= (case when isnull(emailDoc.TINonCommertialCount,0)>0 or isnull(emailDoc.status,0)&32 = 32 then 0  else 1 end),
	FileStatus_IsErorNotExists		= (case when
	isnull(emailDoc.status,0)&64 = 64  -- ���� ���� ������ (�����)
	or 
	(
	(case when isnull(emailDoc.TINonCommertialCount,0)>0 or isnull(emailDoc.status,0)&32 = 32 then 0  else 1 end) =0
	or
	 isnull(emailDoc.status,0)&1 = 0 
	or isnull(emailDoc.status,0)&2 = 0
	or isnull(emailDoc.status,0)&4 = 0 
	or isnull(emailDoc.status,0)&8 = 0	)
	then 0 else 1 end),				

	Answer_FileId=answerEmailDoc.ExchangeDocument_ID,
	Answer_CreateDateTime=answerEmailDoc.CreateDateTime,
	Answer_FileName=answerEmailDoc.FileName,
	Answer_FullFileName=answerEmailDoc.FullFileName,
	Answer_MessageString=answerEmailDoc.MessageString,

	ParentTICount =parentEmailDoc.TICount,
	ParentTINonCommertialCount =parentEmailDoc.TINonCommertialCount


	from 
	#tempLastEmailTable  groupEmailNumber
	left join Expl_XML_ExportServiceJob job on job.ExportServiceJob_ID=groupEmailNumber.ExportServiceJob_ID and job.DataFormat= groupEmailNumber.JobDataFormat
	left join ExplDoc_Journal_Email_Exchange email on email.ExportServiceJob_ID= groupEmailNumber.ExportServiceJob_ID	and email.DirectionExchange=0 																	
																		and email.GroupNumber= groupEmailNumber.EmailGroupNumber
																			
	left join ExplDoc_Journal_Document_Exchange emailDoc
								on 
								emailDoc.DirectionExchange=0
								and emailDoc.DataFormat = groupEmailNumber.FileDataFormat 
								
								and (
									--�������� xml � ������
									emailDoc.ExchangeDocument_ID in 
												 (select xmlDoc.ExchangeDocument_ID from ExplDoc_Journal_Document_Exchange xmlDoc
														where 
														xmlDoc.ExchangeEmail_ID=email.ExchangeEmail_ID 	 
														and xmlDoc.DirectionExchange=0
														and xmlDoc.DataFormat = groupEmailNumber.FileDataFormat 
														and xmlDoc.StartDateTime= groupEmailNumber.StartDataPeriod 
														and xmlDoc.FileType=0
												 )
										or 
										--��� � ������, ������� � ������
									emailDoc.ExchangeDocument_ID in 
												 (select xmlDoc.ExchangeDocument_ID from 
														ExplDoc_Journal_Document_Exchange xmlDoc 
														join ExplDoc_Journal_Document_Exchange zipDoc on zipDoc.ExchangeDocument_ID= xmlDoc.ParentExchangeDocument_ID
													where 
													zipDoc.ExchangeEmail_ID=email.ExchangeEmail_ID 	 
													and zipDoc.DirectionExchange=0
													and zipDoc.DataFormat = groupEmailNumber.FileDataFormat 
													and zipDoc.FileType=3
													and xmlDoc.DirectionExchange=0
													and xmlDoc.DataFormat = groupEmailNumber.FileDataFormat 
													and xmlDoc.StartDateTime= groupEmailNumber.StartDataPeriod 
													and xmlDoc.FileType=0
													)
									)
								
	--������������ ��������
	left join ExplDoc_Journal_Document_Exchange parentEmailDoc
			on parentEmailDoc.ExchangeDocument_ID= emailDoc.ParentExchangeDocument_ID
	

	--����� �� ��������
	left join ExplDoc_Journal_Document_Exchange answerEmailDoc
			on answerEmailDoc.ParentExchangeDocument_ID= emailDoc.ExchangeDocument_ID
			and answerEmailDoc.DirectionExchange=1 
 
order by JobDataFormat , FileDataFormat , JobStringName, StartDataPeriod, FileCreateDateTime


drop table  #tempJobPeriodTable 
drop table  #tempLastEmailTable 
drop table #tempPeriodTable

	 

END
go

grant EXECUTE on dbo.usp2_Get_ExplDoc_Journal_Exchange_Ext to UserDeclarator
go
grant EXECUTE on dbo.usp2_Get_ExplDoc_Journal_Exchange_Ext to UserCalcService
go

 
 