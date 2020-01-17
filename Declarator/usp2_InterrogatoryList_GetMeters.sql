
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InterrogatoryList_GetMeters')
          and type in ('P','PC'))
   drop procedure usp2_InterrogatoryList_GetMeters
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
--		опросный лист получение списка usp2_InterrogatoryList_GetMeters
--
-- ======================================================================================


create proc [dbo].[usp2_InterrogatoryList_GetMeters]
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



---=========================================================
---первая таблица - перечень ТИ
---=========================================================

declare @TI_table table 
        (
        Num int , 
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
        

if 1=1
begin


insert into @TI_table
        (
        Num, ShemNumber,
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
        CounterSerialNumber
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
IncludedToAIIS='Да', 
MethodCalculacionSmallTI='-',
TimeIntervals='-',
Section_ID=@Section_ID,
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

end

 


---ПЕРЕЧЕНЬ ТТ
declare @TT table       
(
Num int identity(1,1),
TInum int,
TI_ID int,
TT_ID int,
Phase varchar(50),
TTModel varchar(200),
TTCoeffHi int,
TTCoeffLo int,
TTCoeff varchar(200),
TTKlass varchar(50),
TTReestrNumber varchar(200),
TTNomLoad decimal(10,2),
TTFactLoad decimal(10,2),
TTInstrumentType varchar(200),
TTSecondaryCircuitLen int,
TTSerialNumber varchar(200),
TTAkt varchar(200),
TTAktCalibration varchar(200),
TTCalibrationInterval int,
TTPlace nvarchar(200),
TIVoltage decimal(10,2),
TIName nvarchar(200),
BuildYear int
)



insert into @TT
        (
        TInum,
        TI_ID ,
        TT_ID ,
        Phase,
        TTModel ,
        TTCoeffHi ,
        TTCoeffLo ,
        TTCoeff ,
        TTKlass ,
        TTReestrNumber,
        TTNomLoad ,
         TTFactLoad,
        TTInstrumentType ,
        TTSerialNumber ,
        TTAkt ,
        TTCalibrationInterval ,
        TTPlace,
        TIVoltage ,
        TIName , 
        BuildYear,
        TTAktCalibration
        )

        select 
        distinct
        TI.Num,
        TI.TI_ID,
        TT.TT_ID,
        case when TT.TT_ID=Info_Transformators.TTA_ID and TT.TT_ID=Info_Transformators.TTC_ID then 'ABC' 
                        when TT.TT_ID=Info_Transformators.TTA_ID then 'A'
                        when TT.TT_ID=Info_Transformators.TTB_ID then 'B'
                        when TT.TT_ID=Info_Transformators.TTC_ID then 'C'
                        else '-' end TTPhase,
        TT.ModelName,
        TTType.CoefIHigh,
        TTType.CoefILow,
        convert(varchar,isnull(TTType.CoefIHigh,0))+'/'+convert(varchar,isnull(TTType.CoefILow,0)),
        TTType.ClassName,
        TTType.RegistryNumber,
        TT.NomLoad, TT.FactLoad,
        'Wh',
        TT.SerialNumber,
        'н/д',
        isnull(TTType.CalibrationInterval,0),
        '', --нет?
        TI.TIVoltage ,
        TI.TIName,
        Year(isnull(TT.ProductionDate,'01-01-1900')),
        TTAktCalibration=''+ case when TT.CalibrationDate is not null then  convert(varchar,isnull(TT.CalibrationDate,'01-01-1900'),104) else '' end
        from 
        @TI_table TI join Info_Transformators on Info_Transformators.TI_ID=TI.TI_ID and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())
        join Hard_Transformators_TT TT on TT.TT_ID=Info_Transformators.TTA_ID
                                or TT.TT_ID=Info_Transformators.TTB_ID
                                or TT.TT_ID=Info_Transformators.TTC_ID
        join Dict_Transformators_TT_Model TTType on TTType.TTModel_ID=TT.TTModel_ID
        Order by TI.Num
        
        --обновляем номера ТИ для таблицы ТТ
        update 
        @TT
        set
        TINum=TI.Num
        from @TT TT join @TI_table TI   on isnull(TT.TI_ID,0)=isnull(TI.TI_ID,1)
        

update @TT
set TTPlace=TI.Place
from @TT tt join @TI_table TI on TI.TI_ID=TT.TI_ID


update @TT set TTAktCalibration= REPLACE(TTAktCalibration,'01.01.1900','н\д')

 
 
                
 

------------------------------------------------------------------------------------------------------------------
--СЧЕТЧИКИ              
                
                declare @Counter table
        (
                Num int identity(1,1),
                TI_ID int,
                TNDesignation_ID int,   
                Counter_ID int,
                TINum varchar(200),
                TTNum varchar(200),     
                TNNum varchar(200),
                Model varchar(200),
                KlassA varchar(50),
                KlassR varchar(50),
                ReestrNumber varchar(200),
                EnergyType varchar(50),
                Shem varchar(50) , --: 3 ТТ - A4; 2 ТТ - A3
                BuildYear varchar(200),
                SerialNumber varchar(200),
                InstallationDate varchar(200),
                ActCalibration varchar(200),
                CalibrationInterval int,
                Gost varchar(200),
                TImeIntervals varchar(50), CalibrationDate datetime,
                CounterChannels varchar(200), 
                UseBusSystem bit
        )
        
        insert into @Counter    
        (       
                TINum,
                TI_ID ,
                TNDesignation_ID ,
                Model ,         
                SerialNumber ,
                CounterChannels
        )
        
        select distinct
        Num,
        TI_ID,
        TNDesignation_ID,
        CounterType,
        ''      ,
        CounterChannels
        from @TI_table
        order by Num
        
        
update @counter 
        set 
        TINum=TI.Num,
        TI_ID=TI.TI_ID,
        TNDesignation_ID= case when isnull(Info_Transformators.UseBusSystem,0)=0 then TI.TI_ID else TI.TNDesignation_ID end,
        
        Counter_ID=Counter.Meter_ID,
        SerialNumber=Counter.MeterSerialNumber,
        Model=CounterType.StringName,
        KlassA=CounterType.ClassActive,
        KlassR=CounterType.ClassReactive,
        ReestrNumber=CounterType.RegistryNumber,
        EnergyType='А, Р',
        BuildYear=case when Year(isnull(Counter.ManufacturedDate,'01-01-1900'))>1950 then convert(varchar,Year(isnull(Counter.ManufacturedDate,'01-01-1900'))) else '-' end ,
        CalibrationInterval=case when isnull(CounterType.CalibrationInterval,0)>0 then CounterType.CalibrationInterval else 0 end,
        Gost='ГОСТ 30206-94, ГОСТ 26035-83',--- в зависимости от года выпуска может быть другой гост
        TImeIntervals='30',
        Shem='A4',
        CalibrationDate=Counter.CalibrationDate,
        ActCalibration=' '+ convert(varchar,Counter.CalibrationDate,104) ,
        InstallationDate=case when Counter.InstallationDate is not null then convert(varchar,isnull(Counter.InstallationDate,'01-01-1900'),104) else '' end, 
        UseBusSystem=isnull(Info_Transformators.UseBusSystem,0)
        from 
          
          @TI_table TI       
        join Info_Meters_TO_TI Info_Meters_TO_TI 
                on Info_Meters_TO_TI.TI_ID = isnull(TI.TI_ID,0) and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
                
                
        join Hard_Meters Counter on Counter.Meter_ID = Info_Meters_TO_TI.Meter_ID
        join Dict_Meters_Model CounterType on CounterType.MeterModel_ID=Counter.MeterModel_ID   
        
        join @counter C on c.TI_ID=TI.TI_ID
          
        
         left join Info_Transformators on Info_Transformators.TI_ID=TI.TI_ID and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())       
         
         
        declare @CounteTTCount table    
        (TI_ID int,
        CountTT int)
                
        insert into @CounteTTCount
        select 
        distinct Info_Transformators.TI_ID,
        Sum(Case when isnull(Info_Transformators.TTA_ID,0)<>0  then 1 else 0 end+
                Case when isnull(Info_Transformators.TTB_ID,0)<>0  then 1 else 0 end+
                Case when isnull(Info_Transformators.TTC_ID,0)<>0 then 1 else 0 end)                    
        from @counter Counter   
        join Info_Transformators
        on Info_Transformators.TI_ID=isnull(Counter.TI_ID,0) and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())
        group by Info_Transformators.TI_ID
        
        
        update @Counter
        set 
        Shem= case when CounteTTCount.CountTT=3 then 'A4'
         when CounteTTCount.CountTT=2 then 'A3'
        else '' end     
        from 
        @Counter Counter join @CounteTTCount CounteTTCount
        on Counter.TI_ID=CounteTTCount.TI_ID
        
        
declare @TINumbers nvarchar(4000), @TINumberMin int, @NextTINumberMin int
set @NextTINumberMin=0        
        
        --обновляем номера ТТ для счетчиков              
         declare @TT_TI_ID int, @resultTTforCounter varchar(200)
         
         DECLARE TT_For_Counter CURSOR FOR 
                 select          
                 distinct       
                 TI_ID
                 from @TT
                 where TI_ID is not null
                 order by 
                 TI_ID

          OPEN TT_For_Counter;
                FETCH NEXT FROM TT_For_Counter  INTO @TT_TI_ID          
                WHILE @@FETCH_STATUS = 0
                BEGIN                 
                        	declare @minTTNUmber int, @maxTTNumber int
                        	
							select @minTTNUmber= min(Num )			
							from @TT TT where TT.TI_ID=@TT_TI_ID
							
							select @maxTTNumber= max(Num )							
							from @TT TT where TT.TI_ID=@TT_TI_ID							
							                
							--select @TINumbers , @TINumberMin ,isnull(@NextTINumberMin,@TINumberMin+1)
							
							update  @Counter 
							set
							 TTNum= case when @maxTTNumber -@minTTNUmber >1 then  CONVERT(varchar,@minTTNUmber)+'-'+CONVERT(varchar,@maxTTNumber)
							 when @maxTTNumber -@minTTNUmber =1 then  CONVERT(varchar,@minTTNUmber)+', '+CONVERT(varchar,@maxTTNumber)
							 else CONVERT(varchar,@minTTNUmber) end
							 where 
							 TI_ID=@TT_TI_ID          
                        
                FETCH NEXT FROM TT_For_Counter  INTO @TT_TI_ID
                END
                CLOSE TT_For_Counter;
                DEALLOCATE TT_For_Counter;              
                             
declare @TN table 
        (Number int ,
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
exec    [usp2_InterrogatoryList_GetTN] @InterrogatoryList_ID , @FilterTIType 

   
                
                
                
                
set @NextTINumberMin=0  

	DECLARE TI_For_TN CURSOR FOR 
	select distinct 
	TINumber, Number
	from @TN
	order by Number
	    
    OPEN TI_For_TN;
    FETCH NEXT FROM TI_For_TN       INTO @TINumbers , @TINumberMin         
    WHILE @@FETCH_STATUS = 0
    BEGIN                     
			
			declare @numbers table (Num varchar(200))
			delete from @numbers

			Declare @products varchar(200)
			set @products  = @TINumbers

			Declare @individual varchar(20) = null			
			Declare @individual2 varchar(20) = null

			WHILE LEN(@products) > 0
			BEGIN
				IF PATINDEX('%,%',@products) > 0
				BEGIN
				
				
					SET @individual = SUBSTRING(@products, 0, PATINDEX('%,%',@products))
							
				
					SET @products = SUBSTRING(@products, LEN(@individual + ',') + 1,
																 LEN(@products))
				END
				ELSE
				BEGIN
					SET @individual = @products
					SET @products = NULL
				END
					
					WHILE LEN(@individual) > 0
					BEGIN
						IF PATINDEX('%-%',@individual) > 0
						BEGIN
							
							declare @minnum int, @maxnum int												
							SET @minnum = convert(int,SUBSTRING(@individual, 0, PATINDEX('%-%',@individual)))
							SET @maxnum = convert(int,SUBSTRING(@individual, PATINDEX('%-%',@individual)+1, LEN(@individual))									 )
										 										 
							while 	@minnum<=@maxnum
							begin		 
								
								insert into @numbers
								values (@minnum)
								
								set @minnum=@minnum+1
							end
														
							
							
							SET @individual =''		
						
						END
						ELSE
						BEGIN
							SET @individual2 = @individual
							SET @individual = NULL							
										
							insert into @numbers
							values (@individual2)			
						END						 					
						
					END	
			END		
		
			declare @minTN int, @maxTN int
			
			select @minTN=MIN(Number) from @TN where TINumber like isnull(@TINumbers,'')
			select @maxTN=MAX(Number) from @TN where TINumber like isnull(@TINumbers,'')
		
			--select  @TINumberMin,@TINumbers, @minTN, @maxTN
			
		
				 update  @Counter 
				set
				 TNNum= case when (@minTN+1)<@maxTN then convert(varchar,@minTN)+'-'+convert(varchar,@maxTN)
				 when (@minTN+1)=@maxTN then convert(varchar,@minTN)+','+convert(varchar,@maxTN)
				 else convert(varchar,@minTN) end
				 where
				 TINum  in (select rtrim(ltrim(Num)) from @numbers)
                   
                   
            
    FETCH NEXT FROM TI_For_TN       INTO  @TINumbers , @TINumberMin
    END
    CLOSE TI_For_TN;
    DEALLOCATE TI_For_TN;   
                    
                    
   
select * from @Counter
order by Num
 
  
end
GO

grant EXECUTE on dbo.[usp2_InterrogatoryList_GetMeters] to UserCalcService
go

grant EXECUTE on dbo.[usp2_InterrogatoryList_GetMeters] to UserDeclarator
go



--exec [usp2_InterrogatoryList_GetMeters] 14,1