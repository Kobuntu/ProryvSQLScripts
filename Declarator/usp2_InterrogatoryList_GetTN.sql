
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InterrogatoryList_GetTN')
          and type in ('P','PC'))
   drop procedure usp2_InterrogatoryList_GetTN
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
--		опросный лист получение списка ТN
--
-- ======================================================================================

create proc [dbo].[usp2_InterrogatoryList_GetTN]
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


--ПЕРЕЧЕНЬ ТН
declare @TN table 
        (Number int identity(1,1),
        TINumber varchar(200),
        TINumberMin int,
        TNDesignation_ID int,
        TN_ID int,
        ThreePhase bit,
        Phase varchar(50),
        TNModel varchar(200),
        TNCoeffHi int,
        TNCoeffLo int,
        TNCoeff nvarchar(200),
        TNClass varchar(50),
        TNRegistryNumber varchar(200),
        TNNomLoad float,
        TNFactLoad float,
        TNUloss float,
        TNInstrumenTNype varchar(200),
        TNSerialNumber varchar(200),
        TNAkt varchar(200),
        TNCalibrationDate varchar(200),
        TNCalibrationInterval int,
        TNInstallationPlace nvarchar(200),
        TNDesignationVoltage float,
        TNDesignationName varchar(200),
        BuildYear int , 
        usebusSystem bit
        )
        

insert into @TN
        (
        TINumberMin,
        TNDesignation_ID ,
        TN_ID ,
        ThreePhase,
        TNUloss ,
        Phase,
        TNModel ,
        TNCoeffHi ,
        TNCoeffLo ,
        TNCoeff ,
        TNClass ,
        TNNomLoad,
        TNRegistryNumber,
        TNInstrumenTNype ,
        TNSerialNumber ,
        TNAkt ,
        TNCalibrationInterval ,
        TNInstallationPlace,
        TNDesignationVoltage,
        TNDesignationName ,
        BuildYear,
        TNFactLoad,
        TNCalibrationDate        , 
        usebusSystem 
        )

  
  select distinct 
		min(TINumberMin) minnumber,
        TNDesignation_ID ,
        TN_ID ,
        ThreePhase,
        TNUloss ,
        Phase,
        TNModel ,
        TNCoeffHi ,
        TNCoeffLo ,
        TNCoeff ,
        TNClass ,
        TNNomLoad,
        TNRegistryNumber,
        TNInstrumenTNype ,
        TNSerialNumber ,
        TNAkt ,
        TNCalibrationInterval ,
        TNInstallationPlace,
        TNDesignationVoltage,
        TNDesignationName ,
        BuildYear,
        TNFactLoad,
        TNCalibrationDate        , 
        usebusSystem
        
  from
  
  (
        select 
        distinct
        TINumberMin=Expl_InterrogatoryList_Content.Number,
        TNDesignation_ID=Info_Transformators.TNDesignation_ID,    
        TN_ID =TN.TN_ID,
        ThreePhase=TNType.IsThreePhase,
        TNUloss=  case when TNType.IsThreePhase=0 and TN.TN_ID=Dict_TNDesignation.TNA_ID then TN.VoltageLossPhaseA
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Dict_TNDesignation.TNB_ID then TN.VoltageLossPhaseB
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Dict_TNDesignation.TNC_ID then TN.VoltageLossPhaseC
                        when  TNType.IsThreePhase=1  then   (case when ISNULL(TN.VoltageLossPhaseA,0)>= ISNULL(TN.VoltageLossPhaseB,0) and ISNULL(TN.VoltageLossPhaseA,0)>= ISNULL(TN.VoltageLossPhaseC,0) then ISNULL(TN.VoltageLossPhaseA,0)
															 when ISNULL(TN.VoltageLossPhaseB,0)>= ISNULL(TN.VoltageLossPhaseC,0) and ISNULL(TN.VoltageLossPhaseB,0)>= ISNULL(TN.VoltageLossPhaseA,0) then ISNULL(TN.VoltageLossPhaseB,0)
															 else ISNULL(TN.VoltageLossPhaseB,0) end  )                
                        else 0 end ,
        Phase=case when TNType.IsThreePhase=0 and TN.TN_ID=Dict_TNDesignation.TNA_ID then 'A'
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Dict_TNDesignation.TNB_ID then 'B'
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Dict_TNDesignation.TNC_ID then 'C'
                        when  TNType.IsThreePhase=1  then 'ABC'                 
                        else '-' end ,
        TNModel=TN.ModelName,
        TNCoeffHi =TNType.CoefUHigh,
        TNCoeffLo =TNType.CoefULow,
                TNCoeff =convert(varchar,isnull(TNType.CoefUHigh,0))+'/'+convert(varchar,isnull(TNType.CoefULow,0)),
               TNClass = TNType.ClassName,
                TNNomLoad=TN.NomLoad,
         TNRegistryNumber= TNType.RegistryNumber,
        TNInstrumenTNype =  'АП-50',
        TNSerialNumber =TN.SerialNumber,
        TNAkt ='н/д',
          TNCalibrationInterval = isnull(TNType.CalibrationInterval,0),
               TNInstallationPlace= TN.InstallationPlace,--??
        TNDesignationVoltage=SSH.Voltage ,
        TNDesignationName =Dict_TNDesignation.StringName,  
         BuildYear=Year(isnull(TN.ProductionDate,'01-01-1900')),
        TNFactLoad=TN.FactLoad,
        TNCalibrationDate      =''+ case when TN.CalibrationDate is not null then  convert(varchar,isnull(TN.CalibrationDate,'01-01-1900'),104) else '' end
        ,
        usebusSystem =1
        from 
          Info_TI  join 
        Expl_InterrogatoryList_Content
        on Info_TI.TI_ID=Expl_InterrogatoryList_Content.TI_ID
        join 
        Info_Transformators on Info_Transformators.TI_ID=Expl_InterrogatoryList_Content.TI_ID and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())        
        join Dict_TNDesignation on Dict_TNDesignation.TNDesignation_ID=Info_Transformators.TNDesignation_ID
        join Hard_Transformators_TN TN on TN.TN_ID=Dict_TNDesignation.TNA_ID
                                or TN.TN_ID=Dict_TNDesignation.TNB_ID
                                or TN.TN_ID=Dict_TNDesignation.TNC_ID
        join Dict_Transformators_TN_Model TNType on TNType.TNModel_ID=TN.TNModel_ID
        join Dict_BusSystem SSH on SSH.BusSystem_ID=Dict_TNDesignation.BusSystem_ID
         outer apply
        (
         select top 1 * from Info_Meters_TO_TI where TI_ID=Info_TI.TI_ID 
         and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
         order by StartDateTime desc
        )Info_Meters_TO_TI        
		left join Hard_Meters on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
        where Info_Transformators.UseBusSystem=1
           and
        Expl_InterrogatoryList_Content.InterrogatoryList_ID= @InterrogatoryList_ID
        
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

      
  union all
        
        select
        distinct
        Expl_InterrogatoryList_Content.Number,
        Info_Transformators.TI_ID,    
        TN.TN_ID,
        TNType.IsThreePhase,
           TNUloss=  case when TNType.IsThreePhase=0 and TN.TN_ID=Info_Transformators.TNA_ID then TN.VoltageLossPhaseA
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Info_Transformators.TNB_ID then TN.VoltageLossPhaseB
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Info_Transformators.TNC_ID then TN.VoltageLossPhaseC
                        when  TNType.IsThreePhase=1  then   (case when ISNULL(TN.VoltageLossPhaseA,0)>= ISNULL(TN.VoltageLossPhaseB,0) and ISNULL(TN.VoltageLossPhaseA,0)>= ISNULL(TN.VoltageLossPhaseC,0) then ISNULL(TN.VoltageLossPhaseA,0)
															 when ISNULL(TN.VoltageLossPhaseB,0)>= ISNULL(TN.VoltageLossPhaseC,0) and ISNULL(TN.VoltageLossPhaseB,0)>= ISNULL(TN.VoltageLossPhaseA,0) then ISNULL(TN.VoltageLossPhaseB,0)
															 else ISNULL(TN.VoltageLossPhaseB,0) end  )                
                        else 0 end ,
        case when TNType.IsThreePhase=0 and TN.TN_ID=Info_Transformators.TNA_ID then 'A'
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Info_Transformators.TNB_ID then 'B'
                        when  TNType.IsThreePhase=0 and TN.TN_ID=Info_Transformators.TNC_ID then 'C'
                        when  TNType.IsThreePhase=1  then 'ABC'                 
                        else '-' end TNPhase,
        TN.ModelName,
        TNType.CoefUHigh,
        TNType.CoefULow,
        convert(varchar,isnull(TNType.CoefUHigh,0))+'/'+convert(varchar,isnull(TNType.CoefULow,0)),
        TNType.ClassName,
        TN.NomLoad,
        TNType.RegistryNumber,
        'АП-50',
        TN.SerialNumber,
        'н/д',
        isnull(TNType.CalibrationInterval,0),
        TN.InstallationPlace,--??
        Info_TI.Voltage ,
        Info_TI.TIName,  
        Year(isnull(TN.ProductionDate,'01-01-1900')),
        TN.FactLoad,
        TNAktCalibration=''+ case when TN.CalibrationDate is not null then  convert(varchar,isnull(TN.CalibrationDate,'01-01-1900'),104) else '' end
        ,0
        from 
          Info_TI  join 
        Expl_InterrogatoryList_Content
        on Info_TI.TI_ID=Expl_InterrogatoryList_Content.TI_ID
        join 
        Info_Transformators on Info_Transformators.TI_ID=Expl_InterrogatoryList_Content.TI_ID and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())       
        
        join Hard_Transformators_TN TN on TN.TN_ID=Info_Transformators.TNA_ID
                                or TN.TN_ID=Info_Transformators.TNB_ID
                                or TN.TN_ID=Info_Transformators.TNC_ID
        join Dict_Transformators_TN_Model TNType on TNType.TNModel_ID=TN.TNModel_ID 
         outer apply
        (
         select top 1 * from Info_Meters_TO_TI where TI_ID=Info_TI.TI_ID 
         and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
         order by StartDateTime desc
        )Info_Meters_TO_TI        
		left join Hard_Meters on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
        where Info_Transformators.UseBusSystem=0
        and
        Expl_InterrogatoryList_Content.InterrogatoryList_ID= @InterrogatoryList_ID
        
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
)
as res

group by
       TNDesignation_ID ,
        TN_ID ,
        ThreePhase,
        TNUloss ,
        Phase,
        TNModel ,
        TNCoeffHi ,
        TNCoeffLo ,
        TNCoeff ,
        TNClass ,
        TNNomLoad,
        TNRegistryNumber,
        TNInstrumenTNype ,
        TNSerialNumber ,
        TNAkt ,
        TNCalibrationInterval ,
        TNInstallationPlace,
        TNDesignationVoltage,
        TNDesignationName ,
        BuildYear,
        TNFactLoad,
        TNCalibrationDate        , 
        usebusSystem


        order by        
	min(TINumberMin),
	Phase,
	TNSerialNumber
        
        
 --обновляем номера ТИ для ТН (т.к. 1 ТН- несколько ТИ то курсор...)                     
         declare @TNInf_ID int, @useBusSystem bit,  @resultTIforTN varchar(200)
          
         
         declare @TI_table table (Num int, TNInf_Id int ,UseBusSystem bit)
         insert into @TI_table
         select distinct 
         Expl_InterrogatoryList_Content.Number,
         TNDesignation_ID=case when Info_Transformators.UseBusSystem=0 then Info_Transformators.TI_ID else Info_Transformators.TNDesignation_ID end, 
         Info_Transformators.UseBusSystem
         
         from
           Expl_InterrogatoryList_Content 
        join 
        Info_Transformators on Info_Transformators.TI_ID=Expl_InterrogatoryList_Content.TI_ID and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())       
         outer apply
        (
         select top 1 * from Info_Meters_TO_TI where TI_ID=Info_Transformators.TI_ID 
         and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
         order by StartDateTime desc
        )Info_Meters_TO_TI        
		left join Hard_Meters on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
        where
        Expl_InterrogatoryList_Content.InterrogatoryList_ID= @InterrogatoryList_ID
        and Expl_InterrogatoryList_Content.Number is not null
		and
	(
	 (@FilterTIType=1 
		and  
		(
		 Expl_InterrogatoryList_Content.TI_ID not in (
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
	 Expl_InterrogatoryList_Content.TI_ID in (
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

         
         --select * from @TI_table
         
         DECLARE TI_For_TN CURSOR FOR  
         select distinct 
         TNInf_Id, usebusSystem
          from
          @TI_table
 

          OPEN TI_For_TN;
                FETCH NEXT FROM TI_For_TN       INTO @TNInf_ID , @useBusSystem         
                WHILE @@FETCH_STATUS = 0
                BEGIN     
                        --set @resultTIforTN=''
                        
                        --select  @resultTIforTN= @resultTIforTN+case when @resultTIforTN='' then ''  else ', ' end + Convert(varchar,Num) 
                        --from @TI_table
                        --where isnull(TNInf_Id,0)=@TNInf_ID and UseBusSystem=@useBusSystem
                        
                        
                        declare @s varchar( 8000 )
						set @s = ''

                         select 
						  @s = @s + ', ' +res.rangecol 
						  from
						 (
							 select 
							 rangecol=
							 case 
							 when countNumbers>2 then CONVERT(varchar,minNumber)+'-'+ CONVERT(varchar,maxNumber)
							  when countNumbers=2 then CONVERT(varchar,minNumber)+', '+ CONVERT(varchar,maxNumber)
							 else CONVERT(varchar,minNumber) end,
							 res.*	 
							 from 
							 (
								 SELECT minNumber=MIN(num),maxNumber= MAX(num), countNumbers= COUNT(num), TNInf_Id
								  FROM (
										 SELECT t.num , 
												t.num - dense_rank() over(order by t.num) as grp,
												TNInf_Id
										   FROM @TI_table t 
										   where TNInf_Id=@TNInf_ID and  isnull(UseBusSystem,0)=isnull(@useBusSystem,0)
									   ) as res
								 GROUP BY grp, TNInf_Id 
							 )
							 as res
						 )
						 as res			
						set @s = stuff( @s, 1, 1, '')
						 
                        update @TN
                        set TINumber=@s--@resultTIforTN
                        from @TN TN where isnull(TN.TNDesignation_ID,0)=@TNInf_ID  and TN.usebusSystem=@useBusSystem                     
                        
                FETCH NEXT FROM TI_For_TN       INTO @TNInf_ID, @useBusSystem 
                END
                CLOSE TI_For_TN;
                DEALLOCATE TI_For_TN;   
                    
                
        --update @TN
        --set TINumber=dbo.fnConvert_StrToContinuousRange(TInum,',')       
        --where isnull(TINumber,'') like '%,%'
        
      
 
 
 
                
        update  @TN
        set
        TNCoeff = convert(nvarchar,TNCoeffHi)+N':√3/'+convert(nvarchar,TNCoeffLo)+N':√3'
        where 
        ThreePhase=0
        
                
        update  @TN
        set TNCoeff     = convert(nvarchar,TNCoeffHi)+N'/'+convert(nvarchar,TNCoeffLo)
        where 
        ThreePhase=0 and TNCoeffLo=27500
        and (TNModel like 'ЗНОМ-35-65'
                or TNModel like 'ЗНОМ-35-54')
                
                
update @TN set TNCalibrationDate= REPLACE(TNCalibrationDate,'01.01.1900','н\д')   
        
update @TN set TNDesignationName= TNDesignationName+ ', фаза '+Phase    
where ThreePhase=0
                
                
update @TN set BuildYear=null
where ISNULL(BuildYear,1900)<=1900
                
select * from @TN
Order by Number
return
 

end
GO

grant EXECUTE on dbo.[usp2_InterrogatoryList_GetTN] to UserCalcService
go

grant EXECUTE on dbo.[usp2_InterrogatoryList_GetTN] to UserDeclarator
go


--exec [usp2_InterrogatoryList_GetTN] 5,1