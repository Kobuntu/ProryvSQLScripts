
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InterrogatoryList_InsertInfoTI')
          and type in ('P','PC'))
   drop procedure usp2_InterrogatoryList_InsertInfoTI
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

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
--
--		опросный лист 
--
-- ======================================================================================


create  proc [dbo].usp2_InterrogatoryList_InsertInfoTI
@InterrogatoryList_ID int -- ИД опросного листа, по нему находим к какому типу объекта относится и добавляем ТИ
, @FilterTIType int --1- наши ТИ, 2- чужие
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


---=========================================================
---ДОБАВЛЯЕМ НЕДОСТАЮЩИЕ ТИ В ОПРОСНЫЙ ЛИСТ 
---=========================================================


declare @ObjectLevel int, @ObjectID int

select @ObjectLevel= case 
when HierLev1_ID is not  null  and HierLev2_ID is null then 1
when HierLev2_ID is not  null  and HierLev3_ID is null then 2
when HierLev3_ID is not  null  and PS_ID is null then 3
when PS_ID is not  null   then 4
when Section_ID is not  null   then 5
else 0 end,
@ObjectID=case 
when HierLev1_ID is not  null  and HierLev2_ID is null then HierLev1_ID
when HierLev2_ID is not  null  and HierLev3_ID is null then HierLev2_ID
when HierLev3_ID is not  null  and PS_ID is null then HierLev3_ID
when PS_ID is not  null   then PS_ID
when Section_ID is not  null   then Section_ID
else 0 end
from Expl_InterrogatoryList
where Expl_InterrogatoryList.InterrogatoryList_ID=@InterrogatoryList_ID


declare @TI_table table 
        (
        Num int identity(1,1), 
        ShemNumber varchar(200),
        
        TIName nvarchar(200),
        TIVoltage float,
        
        IncludedToAIIS varchar(200),
        IsSmallTI varchar(200),
        
        CounterType nvarchar(200),
        CounterSerialNumber nvarchar(200),
        CounterChannels varchar(200),
        
        MethodCalculacionSmallTI varchar(200),
        TimeIntervals varchar(200), 

        H1Name nvarchar(200),
        H2Name nvarchar(200),
        H3Name nvarchar(200),
        PSName nvarchar(200),
        PSVoltage float,
        ParentName nvarchar(400),
        Place nvarchar(800),
        TICode nvarchar(200),
        PS_ID int,      
        TI_ID int, 
        TNDesignation_ID int
        )
      
      
IF (isnull(@ObjectID,0)<>0)
BEGIN

  

if @ObjectLevel=5
begin

	--НАШИ ТИ
	if (@FilterTIType=1)
	BEGIN
		insert into @TI_table
			(
			H1Name,
			H2Name,
			H3Name,
			PSName,
			PSVoltage,
			ParentName,
			Place,
			TIName,
			TIVoltage,
			IsSmallTI,
			TICode,
			TI_ID,
			IncludedToAIIS,
			ShemNumber,
			MethodCalculacionSmallTI,
			TimeIntervals,
			PS_ID,
			TNDesignation_ID,
			CounterChannels,
			CounterType,
			CounterSerialNumber
			)       
	        
	select distinct
	'' H1Name,
	'' H2Name,
	'' H3Name,
	Info_Section_List.SectionName PSName,
	0 PSVoltage,
	--название родителя - выводим в документе (если формируется по уровню 2 например)
	'' ParentName,
	--полное навзвание родителя (для сортировки?)                           
	Place= ltrim(isnull(Dict_HierLev1.StringName,'')+', '+isnull(Dict_HierLev2.StringName,'')+', '+isnull(Dict_HierLev3.StringName,'')+', '+isnull(Dict_PS.StringName,'')),

	Info_TI.TIName,
	Info_TI.Voltage,
	IsSmallTI= case when isnull(Info_TI.IsSmallTI,0)=0 then 'нет' else 'да' end,
	TIATSCode=isnull(Info_TI.TIATSCode,''),
	Info_TI.TI_ID,
	IncludedToAIIS='Да',
	ShemNumber= '-',
	MethodCalculacionSmallTI='-',
	TimeIntervals='-',
	Info_Section_List.Section_ID,
	Info_Transformators.TNDesignation_ID,
	CounterChannels=  
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 1)=0  then '01, ' else ''  end+ 
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 2)=0  then '02, ' else ''  end+ 
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 4)=0  then '03, ' else ''  end+ 
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 8)=0  then '04' 
	else ''  end
	,
	isnull(Dict_Meters_Model.StringName,'н/д'),
	isnull(Hard_Meters.MeterSerialNumber,'')
	from 
	Info_Section_List
	join Info_Section_Description2 on Info_Section_Description2.Section_ID= Info_Section_List.Section_ID
	join Info_TP2 on Info_TP2.TP_ID = Info_Section_Description2.TP_ID
	join Info_TP2_OurSide_Formula_List on Info_TP2.TP_ID= Info_TP2_OurSide_Formula_List.TP_ID
	join Info_TP2_OurSide_Formula_Description 
			on Info_TP2_OurSide_Formula_Description.Formula_UN=Info_TP2_OurSide_Formula_List.Formula_UN
			and Info_TP2_OurSide_Formula_Description.TI_ID is not null
	join Info_TI on Info_TI.TI_ID= Info_TP2_OurSide_Formula_Description.TI_ID
	outer apply
			(
			  select top 1 * from Info_Transformators where TI_ID=Info_TI.TI_ID 
			  and (StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())
			  order by StartDateTime desc
			) Info_Transformators
			outer apply
			(
			 select top 1 * from Info_Meters_TO_TI where TI_ID=Info_TI.TI_ID 
			 and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
			 order by StartDateTime desc
			)Info_Meters_TO_TI
	left join Hard_Meters on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
	left join Dict_Meters_Model on Hard_Meters.MeterModel_ID=Dict_Meters_Model.MeterModel_ID

	join Dict_PS on Dict_PS.PS_ID= Info_TI.PS_ID
	join Dict_HierLev3 on Dict_HierLev3.HierLev3_ID= dict_Ps.HierLev3_ID
	join Dict_HierLev2 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID
	join Dict_HierLev1 on Dict_HierLev1.HierLev1_ID= Dict_HierLev2.HierLev1_ID


	where
	Info_Section_List.Section_ID= @ObjectID
	and Info_TI.Deleted=0
	and Info_TI.Commercial=1
	and Info_TI.TI_ID not in (select TI_ID from Expl_InterrogatoryList_Content where InterrogatoryList_ID=@InterrogatoryList_ID)

	order by
	ParentName,
	Voltage desc,
	Place,
	TIName
	END
	
	--ТИ оформленные через КОНТРАГЕНТОВ
	ELSE
	BEGIN
		insert into @TI_table
			(
			H1Name,
			H2Name,
			H3Name,
			PSName,
			PSVoltage,
			ParentName,
			Place,
			TIName,
			TIVoltage,
			IsSmallTI,
			TICode,
			TI_ID,
			IncludedToAIIS,
			ShemNumber,
			MethodCalculacionSmallTI,
			TimeIntervals,
			PS_ID,
			TNDesignation_ID,
			CounterChannels,
			CounterType,
			CounterSerialNumber
			)       
	        
	select distinct
	'' H1Name,
	'' H2Name,
	'' H3Name,
	Info_Section_List.SectionName PSName,
	0 PSVoltage,
	--название родителя - выводим в документе (если формируется по уровню 2 например)
	'' ParentName,
	--полное навзвание родителя (для сортировки?)                           
	Place= ltrim(isnull(Dict_HierLev1.StringName,'')+', '+isnull(Dict_HierLev2.StringName,'')+', '+isnull(Dict_HierLev3.StringName,'')+', '+isnull(Dict_PS.StringName,'')),

	Info_TI.TIName,
	Info_TI.Voltage,
	IsSmallTI= case when isnull(Info_TI.IsSmallTI,0)=0 then 'нет' else 'да' end,
	TIATSCode=isnull(Info_TI.TIATSCode,''),
	Info_TI.TI_ID,
	IncludedToAIIS='Да',
	ShemNumber= '-',
	MethodCalculacionSmallTI='-',
	TimeIntervals='-',
	Info_Section_List.Section_ID,
	Info_Transformators.TNDesignation_ID,
	CounterChannels=  
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 1)=0  then '01, ' else ''  end+ 
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 2)=0  then '02, ' else ''  end+ 
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 4)=0  then '03, ' else ''  end+ 
	case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 8)=0  then '04' 
	else ''  end
	,
	isnull(Dict_Meters_Model.StringName,'н/д'),
	isnull(Hard_Meters.MeterSerialNumber,'')
	from 
	Info_Section_List
	join Info_Section_Description2 on Info_Section_Description2.Section_ID= Info_Section_List.Section_ID
	join Info_TP2 on Info_TP2.TP_ID = Info_Section_Description2.TP_ID
	join Info_TP2_Contr_Formula_List on Info_TP2.TP_ID= Info_TP2_Contr_Formula_List.TP_ID
	join Info_TP2_Contr_Formula_Description 
			on Info_TP2_Contr_Formula_Description.Formula_UN=Info_TP2_Contr_Formula_List.Formula_UN
			and Info_TP2_Contr_Formula_Description.TI_ID is not null
	join Info_TI on Info_TI.TI_ID= Info_TP2_Contr_Formula_Description.TI_ID
	outer apply
			(
			  select top 1 * from Info_Transformators where TI_ID=Info_TI.TI_ID 
			  and (StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())
			  order by StartDateTime desc
			) Info_Transformators
			outer apply
			(
			 select top 1 * from Info_Meters_TO_TI where TI_ID=Info_TI.TI_ID 
			 and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
			 order by StartDateTime desc
			)Info_Meters_TO_TI
	left join Hard_Meters on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
	left join Dict_Meters_Model on Hard_Meters.MeterModel_ID=Dict_Meters_Model.MeterModel_ID

	join Dict_PS on Dict_PS.PS_ID= Info_TI.PS_ID
	join Dict_HierLev3 on Dict_HierLev3.HierLev3_ID= dict_Ps.HierLev3_ID
	join Dict_HierLev2 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID
	join Dict_HierLev1 on Dict_HierLev1.HierLev1_ID= Dict_HierLev2.HierLev1_ID


	where
	Info_Section_List.Section_ID= @ObjectID
	and Info_TI.Deleted=0
	and Info_TI.Commercial=1
	and Info_TI.TI_ID not in (select TI_ID from Expl_InterrogatoryList_Content where InterrogatoryList_ID=@InterrogatoryList_ID)

	order by
	ParentName,
	Voltage desc,
	Place,
	TIName
	END

end



END


--если совсем нет то добавляем со своей нумерацией
if not Exists (select * from Expl_InterrogatoryList_Content where Expl_InterrogatoryList_Content.InterrogatoryList_ID=@InterrogatoryList_ID)
begin
	Insert into Expl_InterrogatoryList_Content
	(
	[InterrogatoryList_ID] ,
	[TI_ID],
	Number
	)
	select distinct @InterrogatoryList_ID, TI_ID, Num from @TI_table		
end
--если есть то добавляем без номеров
else
begin
	Insert into Expl_InterrogatoryList_Content
	(
	[InterrogatoryList_ID] ,
	[TI_ID],
	Number
	)
	select distinct @InterrogatoryList_ID, TI_ID, null from @TI_table		
	
end

--добавляем ОВ
Insert into Expl_InterrogatoryList_Content
	(
	[InterrogatoryList_ID] ,
	[TI_ID],
	Number
	)
select distinct @InterrogatoryList_ID, Hard_OV_List.TI_ID, null from 
Expl_InterrogatoryList_Content 
join Hard_OV_Positions_List on Expl_InterrogatoryList_Content.TI_ID= Hard_OV_Positions_List.TI_ID
join Hard_OV_List on Hard_OV_List.OV_ID=Hard_OV_Positions_List.OV_ID
where InterrogatoryList_ID=@InterrogatoryList_ID
and Hard_OV_List.TI_ID not in (select TI_ID from Expl_InterrogatoryList_Content where InterrogatoryList_ID=@InterrogatoryList_ID)



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



select '1' as result
 

end


go

grant EXECUTE on dbo.usp2_InterrogatoryList_InsertInfoTI to UserCalcService
go

grant EXECUTE on dbo.usp2_InterrogatoryList_InsertInfoTI to UserDeclarator
go




  
--exec usp2_InterrogatoryList_InsertInfoTI 3,1

