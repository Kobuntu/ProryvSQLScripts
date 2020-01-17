
if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usp2_Get_ExplDoc_Journal_Exchange_Summary]')
          and type in ('P','PC'))
   drop procedure dbo.usp2_Get_ExplDoc_Journal_Exchange_Summary
go


if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usf2_Get_TI_ChangesStatistic_By_ExportServiceJob]')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.[usf2_Get_TI_ChangesStatistic_By_ExportServiceJob]
go

if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usf2_Get_TI_By_ExportServiceJob]')
          and type in ('IF', 'FN', 'TF'))
drop  function [dbo].[usf2_Get_TI_By_ExportServiceJob]
go

if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usf2_Get_TI_By_OurFormula]')
          and type in ('IF', 'FN', 'TF'))
drop function  [dbo].[usf2_Get_TI_By_OurFormula]
go

if exists (select 1
          from sysobjects
          where  id = object_id('dbo.[usf2_Get_OurFormula_By_FormulaUN_Recursive]')
          and type in ('IF', 'FN', 'TF'))
drop function  [dbo].[usf2_Get_OurFormula_By_FormulaUN_Recursive]
go





-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		Сентябрь, 2018
--
-- Описание:
--
--		Возвращаяет список ТИ по ИД формулы, на указанную дату
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Get_TI_By_OurFormula]
(
	@Formula_UN ABS_NUMBER_TYPE_2,
	@ReportDateTime datetime 
)
RETURNS @ret TABLE
(
	TI_ID int	
)
as
begin

	insert into @ret ( TI_ID)
	select Distinct fd.TI_ID
	from 
	Info_TP2_OurSide_Formula_List fl 
		join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN 
		join Info_TI on Info_TI.TI_ID = fd.TI_ID
	where
	fl.Formula_UN= @Formula_UN
	and @ReportDateTime between fl.StartDateTime and isnull(fl.FinishDateTime, '21000101')
	and fd.TI_ID is not null
		 
	 return;
end
go

-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		Сентябрь, 2018
--
-- Описание:
--
--		Возвращаяет список Формул входящих в указанную формулу, на указанную дату
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Get_OurFormula_By_FormulaUN_Recursive]
(
	
	@Formula_UN ABS_NUMBER_TYPE_2, --родительская (корневая) формула
	@ReportDateTime datetime ,	   --время, на которое запрашиваем (может быть нужен период?)		
	-- доп параметры фильтрации для ТП 
	@SelectionType tinyint, -- 0 - PPIMain, 1 - PPIReserve, 2 - PSIMain, 3  PSIReserve , null - All	--(ППИ и ПСИ одновременно не может быть)
	@AllowExclusdedTP bit 
)
RETURNS @ret TABLE
(
	Formula_UN ABS_NUMBER_TYPE_2
)
as
begin

with innerFormula (Formula_UN, UsedFormula_UN, UsedTP_ID,UsedSection_ID, Lvl)
as
	(
		--саму себя, далее смотрим в ней объекты содержащие формулы
		select  fd.Formula_UN, fd.UsedFormula_UN, fd.TP_ID, fd.Section_ID,Lvl=0
		from 
		Info_TP2_OurSide_Formula_List fl 
			join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN 
		where
		fl.Formula_UN =@Formula_UN
		and @ReportDateTime between fl.StartDateTime and isnull(fl.FinishDateTime, '21000101')
		 

		union all
		
		--формулы привязанные к формуле
		select  fd.Formula_UN, fd.UsedFormula_UN, fd.TP_ID, fd.Section_ID,Lvl=i.lvl+1
		from 
		innerFormula i
		join Info_TP2_OurSide_Formula_List fl  on fl.Formula_UN = i.UsedFormula_UN
		join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN 
		where 
		i.UsedFormula_UN is not null
		and @ReportDateTime between fl.StartDateTime and isnull(fl.FinishDateTime, '21000101')
		and fd.Formula_UN <> i.Formula_UN
		and i.Lvl<100
	
		union all

		--ТП привязанные к формуле
		select  fd.Formula_UN, fd.UsedFormula_UN, fd.TP_ID, fd.Section_ID,Lvl=i.lvl+1
		from 
		innerFormula i
		join Info_TP2_OurSide_Formula_List fl on fl.TP_ID = i.UsedTP_ID
		join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN	
		join Info_TP2 on 	Info_TP2.TP_ID=i.UsedTP_ID
		where
		i.UsedTP_ID is not null
		and @ReportDateTime between fl.StartDateTime and isnull(fl.FinishDateTime, '21000101')
		and fd.Formula_UN <> i.Formula_UN
		and (
				@AllowExclusdedTP is null
				or (@AllowExclusdedTP = 0 and Info_TP2.ExcludeFromXMLExport =0 )
				or (@AllowExclusdedTP = 1 and Info_TP2.ExcludeFromXMLExport =1 )
			)
		and 
			(
			--для формул КА будет по другому...
			  @SelectionType is  null
			  or ( @SelectionType= 0 and Info_TP2.IsMoneyOurSide=1)
			  or ( @SelectionType= 1 and Info_TP2.IsMoneyOurSide=0)
			  or ( @SelectionType= 2 and Info_TP2.IsMoneyOurSideMode2=1)
			  or ( @SelectionType= 3 and Info_TP2.IsMoneyOurSideMode2=0)		 
			)
		and i.Lvl<100
		
		

		union all
		 
		--формулы через ТП сечений, привязанных к найденным формулам
		select  fd.Formula_UN, fd.UsedFormula_UN, fd.TP_ID, fd.Section_ID,Lvl=i.lvl+1
		from 
		innerFormula i 
		join Info_Section_Description2  on Info_Section_Description2.Section_ID = i.UsedSection_ID
		join Info_TP2_OurSide_Formula_List fl on Info_Section_Description2.TP_ID=fl.TP_ID
		join Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
		join Info_TP2 on 	Info_TP2.TP_ID=Info_Section_Description2.TP_ID
		join Info_Section_List on Info_Section_Description2.Section_ID = Info_Section_List.Section_ID
		where
		i.UsedSection_ID is not null
		and @ReportDateTime between fl.StartDateTime and isnull(fl.FinishDateTime, '21000101')
		and @ReportDateTime between Info_Section_Description2.StartDateTime and isnull(Info_Section_Description2.FinishDateTime, '21000101')
		and fd.Formula_UN <> i.Formula_UN
		and (
				@AllowExclusdedTP is null
				or (@AllowExclusdedTP = 0 and Info_TP2.ExcludeFromXMLExport =0 )
				or (@AllowExclusdedTP = 1 and Info_TP2.ExcludeFromXMLExport =1 )
			)		
		and 
		(
		--для формул КА будет по другому...
		  @SelectionType is null
		  or ( @SelectionType= 0 and Info_TP2.IsMoneyOurSide=1)
		  or ( @SelectionType= 1 and Info_TP2.IsMoneyOurSide=0)
		  or ( @SelectionType= 2 and Info_TP2.IsMoneyOurSideMode2=1)
		  or ( @SelectionType= 3 and Info_TP2.IsMoneyOurSideMode2=0)			 
		)
		and i.Lvl<100	
	)

	 insert into @ret
	 select  distinct Formula_UN from innerFormula;
	   
	 
	 return;
end
go

  
-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		Сентябрь, 2018
--
-- Описание:
--
--		Возвращаяет список ТИ используемых в указанном задании отправки макетов
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Get_TI_By_ExportServiceJob]
(
	@ExportServiceJob_ID	EXPORTSERVICEJOB_ID_TYPE,
	@ReportDateTime datetime 
)
RETURNS @ret TABLE
(
	TI_ID int
)
as
begin

declare 
	@TiType tinyint, --null, 0- все, 1 - кроме малых, 2 - малые
	@AISFilterType	tinyint, 
	@ATSAIS_ID	ATS_AIS_ID_TYPE,
	@ATSArea_ID ATS_AREA_ID_TYPE,
	@UseMainAIS bit,
	@AISUseAlternative	bit,
	@DenyEmptyTIATSCode	bit ,
	@SelectionType tinyint, -- 0 - PPIMain, 1 - PPIReserve, 2 - PSIMain, 3  PSIReserve , null - All	 
	@AllowExclusdedTP bit


	select @TiType =case when Expl_XML_ExportServiceJob.DataFormat=0 then 1
	when Expl_XML_ExportServiceJob.DataFormat=1 then 2
	else 0 end,
	@AISFilterType=AISFilterType,
	@ATSAIS_ID	=ATSAIS_ID,
	@ATSArea_ID =ATSArea_ID,
	@UseMainAIS =UseMainAIS,
	@AISUseAlternative	=AISUseAlternative,
	@DenyEmptyTIATSCode	=DenyEmptyTIATSCode ,
	@SelectionType =SectionMode, --?? 0 - PPIMain, 1 - PPIReserve, 2 - PSIMain, 3  PSIReserve , null - All	--(ППИ и ПСИ одновременно не может быть)
	@AllowExclusdedTP =1			
	from Expl_XML_ExportServiceJob
	where ExportServiceJob_ID=@ExportServiceJob_ID
	 


 if (@ATSAIS_ID='')
	set @ATSAIS_ID = null
	
 if (@ATSArea_ID='')
	set @ATSArea_ID = null

	

 declare @tempRet table (TI_ID int)

		insert into @tempRet (TI_ID)
		
		--Добавляем ТИ, прикрепленные		
		select distinct
		[dbo].vw_Dict_Hierarchy.TI_ID
		from 
		[dbo].Expl_XML_ExportServiceJob_Object
		 join [dbo].vw_Dict_Hierarchy
		on
		(Expl_XML_ExportServiceJob_Object.HierLev1_ID is not null and Expl_XML_ExportServiceJob_Object.HierLev1_ID= vw_Dict_Hierarchy.HierLev1_ID)
		or
		(Expl_XML_ExportServiceJob_Object.HierLev2_ID is not null and Expl_XML_ExportServiceJob_Object.HierLev2_ID= vw_Dict_Hierarchy.HierLev2_ID)
		or
		(Expl_XML_ExportServiceJob_Object.HierLev3_ID is not null and Expl_XML_ExportServiceJob_Object.HierLev3_ID= vw_Dict_Hierarchy.HierLev3_ID)
		or
		(Expl_XML_ExportServiceJob_Object.PS_ID is not null and Expl_XML_ExportServiceJob_Object.PS_ID= vw_Dict_Hierarchy.PS_ID)
		or
		(Expl_XML_ExportServiceJob_Object.TI_ID is not null and Expl_XML_ExportServiceJob_Object.TI_ID= vw_Dict_Hierarchy.TI_ID)

		where 
		Expl_XML_ExportServiceJob_Object.ExportServiceJob_ID=@ExportServiceJob_ID
		and vw_Dict_Hierarchy.TI_ID is not null

				
		declare @tempSection table (Section_ID int)
		insert into @tempSection (Section_ID)
		select distinct 
		Section_ID 
		from
		[dbo].Expl_XML_ExportServiceJob_Object 
		where
		Expl_XML_ExportServiceJob_Object.ExportServiceJob_ID=@ExportServiceJob_ID
		and Expl_XML_ExportServiceJob_Object.Section_ID is not null
		
		insert into @tempSection (Section_ID)
		select distinct 
		[Expl_XML_Export_ConfigContent].Section_ID 
		from
		Expl_XML_ExportServiceJob_Object
		join[dbo].[Expl_XML_Export_ConfigContent] on Expl_XML_Export_ConfigContent.XMLExportConfig_ID=Expl_XML_ExportServiceJob_Object.XMLExportConfig_ID
		where
		Expl_XML_ExportServiceJob_Object.ExportServiceJob_ID=@ExportServiceJob_ID
		and Expl_XML_ExportServiceJob_Object.XMLExportConfig_ID is not null
		and [Expl_XML_Export_ConfigContent].Section_ID is not null
		and [Expl_XML_Export_ConfigContent].Section_ID not in (select Section_ID from @tempSection)


		
		declare @tempFormulasRet table (Formula_UN ABS_NUMBER_TYPE_2, TI_ID int)

		--добавляем ТИ из сечений задания
		--insert into @tempFormulasRet (Formula_UN,TI_ID)	
		insert into @tempRet (TI_ID)		
		select distinct 
		--tis.formula_UN,
		tis.TI_ID
		from 
		@tempSection sect
		join [dbo].Info_Section_Description2 on sect.Section_ID = Info_Section_Description2.Section_ID
			join [dbo].Info_TP2_OurSide_Formula_List on Info_TP2_OurSide_Formula_List.TP_ID=Info_Section_Description2.TP_ID
			cross apply(select * from [dbo].[usf2_Get_OurFormula_By_FormulaUN_Recursive] 
										(	 Info_TP2_OurSide_Formula_List.Formula_UN, 
											@ReportDateTime,
											@SelectionType,
											@AllowExclusdedTP)
					) as fn
			cross apply (select * from [dbo].[usf2_Get_TI_By_OurFormula]
													(fn.Formula_UN,  
													@ReportDateTime)) as tis
		where 
		 @ReportDateTime between Info_Section_Description2.StartDateTime and isnull(Info_Section_Description2.FinishDateTime, '21000101')
		and tis.TI_ID not in (select TI_ID from @ret)

		
		--добавляем ТИ из АИС в задании
		insert into @tempRet (TI_ID)		
		select distinct 
		Info_TI.TI_ID 
		from

		Expl_XML_ExportServiceJob_Object
		join Info_TI on (isnull(@UseMainAIS,0)=1 and Info_TI.ATSAIS_ID is not null and  Info_TI.ATSAIS_ID =Expl_XML_ExportServiceJob_Object.ATSAIS_ID 
						or 
						isnull(@AISUseAlternative,0)=1 and Info_TI.ATSAIS_ID2 is not null and  Info_TI.ATSAIS_ID2=Expl_XML_ExportServiceJob_Object.ATSAIS_ID 
						)
		where 
		Expl_XML_ExportServiceJob_Object.ExportServiceJob_ID=@ExportServiceJob_ID
		and Expl_XML_ExportServiceJob_Object.ATSAIS_ID is not null
		and Info_TI.TI_ID not in (select TI_ID from @ret)

		--добавляем ТИ из конфигурации в задании
		insert into @tempRet (TI_ID)
		select distinct 
		[Expl_XML_Export_ConfigContent].TI_ID 
		from
		Expl_XML_ExportServiceJob_Object
		join[dbo].[Expl_XML_Export_ConfigContent] on Expl_XML_Export_ConfigContent.XMLExportConfig_ID=Expl_XML_ExportServiceJob_Object.XMLExportConfig_ID
		where
		Expl_XML_ExportServiceJob_Object.ExportServiceJob_ID=@ExportServiceJob_ID
		and Expl_XML_ExportServiceJob_Object.XMLExportConfig_ID is not null
		and [Expl_XML_Export_ConfigContent].TI_ID is not null
		and [Expl_XML_Export_ConfigContent].TI_ID not in (select TI_ID from @tempRet)
		

		insert into @ret
		select distinct Info_TI.TI_ID
		from 
		@tempRet temp 
		join Info_TI on  Info_TI.TI_ID = temp.TI_ID
		where
		(
			--null, 0- все, 1 - кроме малых, 2 - малые
			@TiType is null
			or
			@TiType not in (1,2)
			or
			(
				--все кроме малых галка и тип
				(@TiType=1 and Info_TI.TIType !=2 and isnull (Info_TI.IsSmallTI,0)=0)
				or
				--малые галка или тип
				(@TiType=2 and (Info_TI.TIType =2 or isnull (Info_TI.IsSmallTI,0)=1))		
			)
		)

		--фильтры по АИС
		and 
		(
			--0 все ТИ, 1- указанная АИС, 2 ТИ с заполненной АИС
			@AISFilterType is null
			or @AISFilterType = 0
			or (@AISFilterType= 1 
				and (
						@UseMainAIS=1 and Info_TI.ATSAIS_ID= @ATSAIS_ID
						or 
						@AISUseAlternative=1 and Info_TI.ATSAIS_ID2= @ATSAIS_ID
					)
				)
			or (@AISFilterType= 2 
				and (
						@UseMainAIS=1 and Info_TI.ATSAIS_ID is not null
						or 
						@AISUseAlternative=1 and Info_TI.ATSAIS_ID2 is not null
					)
				)
		)

		--если фильтр по Area
		and 
		(
			@ATSArea_ID is null 
			or 		
			(
				@UseMainAIS=1 and  isnull(Info_TI.ATSArea_ID,'')=@ATSArea_ID
				or 
				@AISUseAlternative=1 and  isnull(Info_TI.ATSArea_ID2,'')=@ATSArea_ID
			)	
		)

		--запрет пустых кодов АТС
		and 
		(
			@DenyEmptyTIATSCode is null 
			or @DenyEmptyTIATSCode =0
			or (
				@DenyEmptyTIATSCode =1 
				and isnull(Info_TI.TIATSCode,'')<>''
				)
		)
return;
end
go




-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		Сентябрь, 2018
--
-- Описание:
--
--		Статистика по ТИ для задания отправки 80020 с указанием - изменились данные после последней выгрузки или нет
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Get_TI_ChangesStatistic_By_ExportServiceJob]
(
	@ExportServiceJob_ID	EXPORTSERVICEJOB_ID_TYPE,
	@EventDate datetime,
	@LastRunDateTime datetime
)

RETURNS @ret TABLE
(
	ExportServiceJob_ID int,
	TI_ID int,
	EventDate datetime,
	FileCreateDateTime datetime,
	IsChanged bit,
	DispatchDateTime datetime
)
as
begin

--для  указанной строки статистики (задание, дата, дата последнего выполнения.. находим ТИ и статистику)

declare @DataSourceType tinyint

	
select @DataSourceType = case when Expl_XML_ExportServiceJob.DataSourceType	=255 then null else Expl_XML_ExportServiceJob.DataSourceType end
from Expl_XML_ExportServiceJob
where ExportServiceJob_ID=@ExportServiceJob_ID
		

set @EventDate = CAST(FLOOR(CAST( isnull(@EventDate ,getdate()) as FLOAT))as datetime) 


	 
insert into @ret
(
ExportServiceJob_ID,
TI_ID,
EventDate,
FileCreateDateTime,
IsChanged,
DispatchDateTime
)
select distinct
@ExportServiceJob_ID,
tis.TI_ID,
@EventDate,
@LastRunDateTime,
IsChanged=isnull(Arch_30_Values.IsChanged,0),  
DispatchDateTime=Arch_30_Values.DispatchDateTime 
from 
(select * from  dbo.[usf2_Get_TI_By_ExportServiceJob]
									( 
									@ExportServiceJob_ID,
									@EventDate	--список ТИ на начало периода							
									)) Tis
outer apply (
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalc_30_Virtual where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalc_30_Month_Values where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) and DispatchDateTime>@LastRunDateTime			
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_1 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_2 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_2 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_4 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_5 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_6 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_7 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_8 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_9 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_10 where TI_ID = Tis.TI_ID and EventDate= @EventDate and ChannelType in (1,2,3,4) 
			and(@DataSourceType is null or Datasource_ID=@DataSourceType) and DispatchDateTime>@LastRunDateTime			
			) 
			as Arch_30_Values


return;

end
go




grant select on [usf2_Get_TI_ChangesStatistic_By_ExportServiceJob] to UserCalcService
go
grant select on [usf2_Get_TI_ChangesStatistic_By_ExportServiceJob] to UserDeclarator
go
grant select on [usf2_Get_TI_By_ExportServiceJob] to UserCalcService
go
grant select on [usf2_Get_TI_By_ExportServiceJob] to UserDeclarator
go
grant select on [usf2_Get_OurFormula_By_FormulaUN_Recursive] to UserCalcService
go
grant select on [usf2_Get_OurFormula_By_FormulaUN_Recursive] to UserDeclarator
go 
grant select on [usf2_Get_TI_By_OurFormula] to UserCalcService
go
grant select on [usf2_Get_TI_By_OurFormula] to UserDeclarator
go




create procedure dbo.usp2_Get_ExplDoc_Journal_Exchange_Summary
  @userId varchar(200)='',
  @startdatetime datetime = null,
  @enddatetime datetime = null,
  @DeadlineHourLocal int =10, 
  @DeadlineMinLocal int= 00,
  @FilterJobDataFormat tinyint= null,
  @ReturnOnlyErrors bit = 0

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
 ParentTINonCommertialCount int
 )
Insert into @res 
 exec usp2_Get_ExplDoc_Journal_Exchange_Ext @userId, @startDateTime,@enddatetime, @DeadlineHourLocal ,  @DeadlineMinLocal 


 
select 
JobDataFormat,	
FileDataFormat,
EmailGroupNumber,
GroupResult.ExportServiceJob_ID,
JobStringName,
StartDataPeriod,
FinishDataPeriod_Exclude,
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
FileTICount,
JobResult =  
 case when MaxFileCreateDateTime is null then 0 
				 when MaxFileCreateDateTime is not null and isnull(ChangedSumm,0)=0 then 1
				 when MaxFileCreateDateTime is not null and isnull(ChangedSumm,0)=1 then 2
				 else 0 end,
JobResultString = 
case when MaxFileCreateDateTime is null then 'не выполнено' 
				 when  MaxFileCreateDateTime is not null and isnull(ChangedSumm,0)=0 then 'выполнено, отсутствуют изменения данных'
				 when  MaxFileCreateDateTime is not null and isnull(ChangedSumm,0)=1 then 'выполнено, имеются изменения данных'
				 else '' end 
,MaxFileCreateDateTime
from 
(
	select distinct 
		JobDataFormat,	
		FileDataFormat,
		EmailGroupNumber,
		ExportServiceJob_ID,
		JobStringName,
		StartDataPeriod,  
		FinishDataPeriod_Exclude,
		IsDeadlineViolated_ErrorCount	=  sum(cast(case when isnull(Email_IsDeadlineCorrected,0) =0 then 1 else 0 end as int)),
		FileStatus_IsCreated_ErrorCount	=  sum(cast(case when isnull(FileStatus_IsCreated,0) =0 then 1 else 0 end  as int)),
		FileStatus_IsSent_ErrorCount			=  sum(cast(case when isnull(FileStatus_IsSent,0) =0 then 1 else 0 end  as int)),
		FileStatus_AnswerLoaded_ErrorCount		=  sum(cast(case when isnull(FileStatus_AnswerLoaded,0) =0 then 1 else 0 end  as int)),
		FileStatus_AnswerCorrect_ErrorCount		=  sum(cast(case when isnull(FileStatus_AnswerCorrect,0) =0 then 1 else 0 end  as int)),
		FileStatus_IsNonCommerceNotExists_ErrorCount	=  sum(cast(case when isnull(FileStatus_IsNonCommerceNotExists,1) =0 then 1 else 0 end  as int)),
		FileStatus_IsErorNotExists_ErrorCount		=  sum(cast(case when isnull(FileStatus_IsErorNotExists,0) =0 then 1 else 0 end  as int)), 	
		FileTICount=sum(FileTICount),
		MaxFileCreateDateTime=max(FileCreateDateTime) 
	from
		(		 
		select * from @res
		where @FilterJobDataFormat is null or JobDataFormat=@FilterJobDataFormat
		)as FileResult
	group by 
		JobDataFormat,	
		FileDataFormat,
		EmailGroupNumber,
		ExportServiceJob_ID,
		JobStringName,
		StartDataPeriod,
		FinishDataPeriod_Exclude
)
as GroupResult
cross apply (select top 1 * from Expl_XML_ExportServiceJob where Expl_XML_ExportServiceJob.ExportServiceJob_ID=  GroupResult.ExportServiceJob_ID) as Job
outer apply 
(
select top 1 ChangedSumm=1
 from  dbo.[usf2_Get_TI_By_ExportServiceJob] (GroupResult.ExportServiceJob_ID,StartDataPeriod) tis --список ТИ на начало каждого периода (может отличаться)
 left join info_TI on Info_TI.TI_ID=tis.TI_ID
 where exists 
 (
  

			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalc_30_Virtual arch where info_TI.TIType=0 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalc_30_Month_Values arch where info_TI.TIType=2 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and DispatchDateTime>GroupResult.MaxFileCreateDateTime			
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_1 arch where info_TI.TIType=11 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_2 arch where info_TI.TIType=12 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_2 arch where info_TI.TIType=13 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_4 arch where info_TI.TIType=14 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_5 arch where info_TI.TIType=15 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_6 arch where info_TI.TIType=16 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_7 arch where info_TI.TIType=17 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_8 arch where info_TI.TIType=18 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_9 arch where info_TI.TIType=19 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime
			union
			select top 1  IsChanged =cast(1 as bit),DispatchDateTime from ArchCalcBit_30_Virtual_10 arch where info_TI.TIType=20 and arch.TI_ID = info_TI.TI_ID and ChannelType in (1,2,3,4) and EventDate= GroupResult.StartDataPeriod 
			and(isnull(job.DataSourceType,255)=255 or arch.DataSource_ID=job.DataSourceType ) and DispatchDateTime>GroupResult.MaxFileCreateDateTime	



			--и любые изменнеия по актам недоучета
			union
			select top 1  IsChanged =cast(1 as bit),EventDateTime  from [dbo].[Expl_User_Journal_Replace_30_Virtual]
			where 
			
			Expl_User_Journal_Replace_30_Virtual.EventDateTime>GroupResult.MaxFileCreateDateTime	
			and Expl_User_Journal_Replace_30_Virtual.TI_ID = info_TI.TI_ID 			
			and Expl_User_Journal_Replace_30_Virtual.ChannelType in (1,2,3,4) 
			and ((GroupResult.StartDataPeriod between Expl_User_Journal_Replace_30_Virtual.EventDate and Expl_User_Journal_Replace_30_Virtual.ZamerDateTime)
					or (Expl_User_Journal_Replace_30_Virtual.EventDate between GroupResult.StartDataPeriod and GroupResult.FinishDataPeriod_Exclude)
				)
			and Expl_User_Journal_Replace_30_Virtual.EventParam<>0
 )
)
as ChangesStatistic
where 
@ReturnOnlyErrors =0
or (@ReturnOnlyErrors=1 and isnull(ChangedSumm,0)=1)
order by JobStringName, StartDataPeriod
 
END
go

grant EXECUTE on dbo.usp2_Get_ExplDoc_Journal_Exchange_Summary to UserDeclarator
go
grant EXECUTE on dbo.usp2_Get_ExplDoc_Journal_Exchange_Summary to UserCalcService
go




