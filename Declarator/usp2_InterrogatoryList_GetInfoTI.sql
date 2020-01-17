
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InterrogatoryList_GetInfoTI')
          and type in ('P','PC'))
   drop procedure usp2_InterrogatoryList_GetInfoTI
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
--		опросный лист получение списка ТТ
--
-- ======================================================================================


create  proc [dbo].usp2_InterrogatoryList_GetInfoTI
@InterrogatoryList_ID int -- ИД опросного листа, по нему находим к какому типу объекта относится и добавляем ТИ
, @FilterTIType int --1- наши ТИ, 2- чужие
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


declare @Section_ID int

select @Section_ID=Section_ID from Expl_InterrogatoryList 
where Section_ID is not null and InterrogatoryList_ID=@InterrogatoryList_ID
set @Section_ID= ISNULL(@Section_ID,0)



declare @TI_table table 
        (
        Num int , 
        NumberOnScheme int,        
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
        TNDesignation_ID int,
		NumberAIS int,
		CertificateNumber nvarchar(200)
        )
      
      
IF 1=1
BEGIN

  

if 1=1
begin

	insert into @TI_table
			(
			Num,
			NumberOnScheme,
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
			MethodCalculacionSmallTI,
			TimeIntervals,
			PS_ID,
			TNDesignation_ID,
			CounterChannels,
			CounterType,
			CounterSerialNumber, 
			NumberAIS,
			CertificateNumber
			)       
	        
	select distinct

			Expl_InterrogatoryList_Content.Number,
			Expl_InterrogatoryList_Content.NumberOnScheme,
	'' H1Name,
	'' H2Name,
	'' H3Name,
	'' PSName,
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
	IncludedToAIIS='да',
	MethodCalculacionSmallTI='-',
	TimeIntervals='-',
	Section_ID=0,
	Info_Transformators.TNDesignation_ID,
	CounterChannels=  ''
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 1)=0  then '01, ' else ''  end+ 
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 2)=0  then '02, ' else ''  end+ 
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 4)=0  then '03, ' else ''  end+ 
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 8)=0  then '04' 
	--else ''  end
	,
	isnull(Dict_Meters_Model.StringName,'н/д'),
	isnull(Hard_Meters.MeterSerialNumber,''),
	Expl_InterrogatoryList_Content.NumberAIS,
	isnull(Dict_TI_AIs.CertificateNumber,'') as CertificateNumber
	from 
	Info_TI 
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
	join Expl_InterrogatoryList_Content on  Expl_InterrogatoryList_Content.InterrogatoryList_ID=@InterrogatoryList_ID and Expl_InterrogatoryList_Content.TI_ID=info_TI.TI_ID
	left join Dict_TI_AIs on Dict_TI_AIs.ATSAIS_ID=Info_TI.ATSAIS_ID
	where
	  Expl_InterrogatoryList_Content.InterrogatoryList_ID= @InterrogatoryList_ID
	        
	and Info_TI.Deleted=0
	and Info_TI.Commercial=1
	and
	(
	 (@FilterTIType=1 
		and  
		(
		 Info_TI.TI_ID not in (
				 select distinct Info_TI.TI_ID 
				 from
					Info_Section_List
					join Info_Section_Description2 on Info_Section_Description2.Section_ID= Info_Section_List.Section_ID
					join Info_TP2 on Info_TP2.TP_ID = Info_Section_Description2.TP_ID
					join Info_TP2_Contr_Formula_List on Info_TP2.TP_ID= Info_TP2_Contr_Formula_List.TP_ID
					join Info_TP2_Contr_Formula_Description 
							on Info_TP2_Contr_Formula_Description.Formula_UN=Info_TP2_Contr_Formula_List.Formula_UN	and Info_TP2_Contr_Formula_Description.TI_ID is not null
							join Info_TI on Info_TI.TI_ID=Info_TP2_Contr_Formula_Description.TI_ID and Info_TP2_Contr_Formula_Description.TI_ID is not null
							where Info_Section_List.Section_ID=@Section_ID
						) 							
		)				
							
	 )
	 or 
	 (@FilterTIType=2 
	 and  
	 Info_TI.TI_ID in (
	 select distinct Info_TI.TI_ID 
	 from
				Info_Section_List
				join Info_Section_Description2 on Info_Section_Description2.Section_ID= Info_Section_List.Section_ID
				join Info_TP2 on Info_TP2.TP_ID = Info_Section_Description2.TP_ID
				join Info_TP2_Contr_Formula_List on Info_TP2.TP_ID= Info_TP2_Contr_Formula_List.TP_ID
				join Info_TP2_Contr_Formula_Description 
						on Info_TP2_Contr_Formula_Description.Formula_UN=Info_TP2_Contr_Formula_List.Formula_UN	and Info_TP2_Contr_Formula_Description.TI_ID is not null
						join Info_TI on Info_TI.TI_ID=Info_TP2_Contr_Formula_Description.TI_ID and Info_TP2_Contr_Formula_Description.TI_ID is not null
						where Info_Section_List.Section_ID=@Section_ID
						) 
	 ) 
	 or 
	 @FilterTIType not in (1,2)
	)
	order by
	ParentName,
	Voltage desc,
	Place,
	TIName
	
	
	
	
--добавляем ОВ (будет добавлен и в 3.1 и в 3.2 если заменяет ТИ и там и там)
Insert into @TI_table
		(
			Num,
			NumberOnScheme,
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
			MethodCalculacionSmallTI,
			TimeIntervals,
			PS_ID,
			TNDesignation_ID,
			CounterChannels,
			CounterType,
			CounterSerialNumber,
			NumberAIS,
			CertificateNumber
			)       
	        
	select distinct

			Expl_InterrogatoryList_Content.Number,
			Expl_InterrogatoryList_Content.NumberOnScheme,
	'' H1Name,
	'' H2Name,
	'' H3Name,
	'' PSName,
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
	IncludedToAIIS='да',
	MethodCalculacionSmallTI='-',
	TimeIntervals='-',
	Section_ID=0,
	Info_Transformators.TNDesignation_ID,
	--CounterChannels=  
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 1)=0  then '01, ' else ''  end+ 
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 2)=0  then '02, ' else ''  end+ 
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 4)=0  then '03, ' else ''  end+ 
	--case when ((isnull(Info_TI.AbsentChannelsMask,0)) & 8)=0  then '04' 
	--else ''  end
	
	--если резервные каналы - как их отображать?
	CounterChannels= '' 
	 --case when isnull(Info_TI.XMLAIATSCode,0) in (1,11,21,31,41,51,61) or isnull(Info_TI.XMLAOATSCode,0) in (1,11,21,31,41,51,61)  then '01, ' else ''  end+
	 --case when isnull(Info_TI.XMLAIATSCode,0) in (2,12,22,32,42,52,62) or isnull(Info_TI.XMLAOATSCode,0) in (2,12,22,32,42,52,62) then '02, ' else ''  end+
	 --case when isnull(Info_TI.XMLRIATSCode,0) in (3,13,23,33,43,53,63) or isnull(Info_TI.XMLROATSCode,0) in (3,13,23,33,43,53,63)  then '03, ' else ''  end+
	 --case when isnull(Info_TI.XMLRIATSCode,0) in (4,14,24,34,44,54,64) or isnull(Info_TI.XMLROATSCode,0) in (4,14,24,34,44,54,64)  then '04' else ''  end 
	,
	isnull(Dict_Meters_Model.StringName,'н/д'),
	isnull(Hard_Meters.MeterSerialNumber,''),
	Expl_InterrogatoryList_Content.NumberAIS,
	isnull(Dict_TI_AIs.CertificateNumber,'') as CertificateNumber
	from 
	Info_TI 
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
	join Expl_InterrogatoryList_Content on  Expl_InterrogatoryList_Content.InterrogatoryList_ID=@InterrogatoryList_ID and Expl_InterrogatoryList_Content.TI_ID=info_TI.TI_ID
	left join Dict_TI_AIs on Dict_TI_AIs.ATSAIS_ID=Info_TI.ATSAIS_ID
	where	        
	Info_TI.Deleted=0
	and Info_TI.Commercial=1
	and Info_TI.TI_ID in 
	(		
			select distinct  Hard_OV_List.TI_ID
			from 
			@TI_table temp join
			Hard_OV_Positions_List on temp.TI_ID= Hard_OV_Positions_List.TI_ID
			join Hard_OV_List on Hard_OV_List.OV_ID=Hard_OV_Positions_List.OV_ID

			)
	and Info_TI.TI_ID not in (select TI_ID from  @TI_table)



end
END



select distinct
Number=Num,
NumberOnScheme,Place,
TIFullName = temp.TIName,
TIVoltage,
IncludedToAIIS,
temp.IsSmallTI,
MeterModel=temp.CounterType,

--каналы берем из XML 
AbsentChannelsMaskStringName= 
 case when isnull(Info_TI.XMLAIATSCode,0) in (1,11,21,31,41,51,61) or isnull(Info_TI.XMLAOATSCode,0) in (1,11,21,31,41,51,61)  then '01, ' else ''  end+
	 case when isnull(Info_TI.XMLAIATSCode,0) in (2,12,22,32,42,52,62) or isnull(Info_TI.XMLAOATSCode,0) in (2,12,22,32,42,52,62) then '02, ' else ''  end+
	 case when isnull(Info_TI.XMLRIATSCode,0) in (3,13,23,33,43,53,63) or isnull(Info_TI.XMLROATSCode,0) in (3,13,23,33,43,53,63)  then '03, ' else ''  end+
	 case when isnull(Info_TI.XMLRIATSCode,0) in (4,14,24,34,44,54,64) or isnull(Info_TI.XMLROATSCode,0) in (4,14,24,34,44,54,64)  then '04' else ''  end 
,
InterrogatoryList_ID=@InterrogatoryList_ID,
TI_ID= temp.TI_ID,
IsAlienTI= case when @FilterTIType=2 then 1 else 0 end,
 MethodCalculacionSmallTI,
        TimeIntervals, NumberAIS,
		CertificateNumber
 from @TI_table temp
 join
 Info_TI on temp.TI_ID=Info_TI.TI_ID
order  by Num

end


go

grant EXECUTE on dbo.usp2_InterrogatoryList_GetInfoTI to UserCalcService
go

grant EXECUTE on dbo.usp2_InterrogatoryList_GetInfoTI to UserDeclarator
go


 
--exec usp2_InterrogatoryList_GetInfoTI 1,1