
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_InterrogatoryList_GetTT')
          and type in ('P','PC'))
   drop procedure usp2_InterrogatoryList_GetTT
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


create proc [dbo].[usp2_InterrogatoryList_GetTT]
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



---ПЕРЕЧЕНЬ ТТ
declare @TT table       
(
Number int identity(1,1),
TINumber int,
TI_ID int,
TT_ID int,
Phase varchar(50),
TTModel varchar(200),
TTCoeffHi int,
TTCoeffLo int,
TTCoeff varchar(200),
TTClass varchar(50),
TTRegistryNumber varchar(200),
TTNomLoad float,
TTFactLoad float,
TTInstrumentType varchar(200),
TTSecondaryCircuitLen int,
TTSerialNumber varchar(200),
TTAkt varchar(200),
TTCalibrationDate varchar(200),
TTCalibrationInterval int,
TTInstallationPlace nvarchar(200),
TIVoltage float,
TIName nvarchar(200),
BuildYear int
)



insert into @TT
        (
        TINumber,
        TI_ID ,
        TT_ID ,
        Phase,
        TTModel ,
        TTCoeffHi ,
        TTCoeffLo ,
        TTCoeff ,
        TTClass ,
        TTRegistryNumber,
        TTNomLoad ,
         TTFactLoad,
        TTInstrumentType ,
        TTSerialNumber ,
        TTAkt ,
        TTCalibrationInterval ,
        TTInstallationPlace,
        TIVoltage ,
        TIName , 
        BuildYear,
        TTCalibrationDate
        )

        select 
        distinct
        Expl_InterrogatoryList_Content.Number,
        Info_TI.TI_ID,
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
        TTPlace=TT.InstallationPlace, 
        TIVoltage=Info_TI.Voltage ,
        Info_TI.TIName,
        Year(isnull(TT.ProductionDate,'01-01-1900')),
        TTCalibrationDate=''+ case when TT.CalibrationDate is not null then  convert(varchar,isnull(TT.CalibrationDate,'01-01-1900'),104) else '' end
        from 
        Info_TI  join 
        Expl_InterrogatoryList_Content
        on Info_TI.TI_ID=Expl_InterrogatoryList_Content.TI_ID
        join 
        Info_Transformators on Info_Transformators.TI_ID=Expl_InterrogatoryList_Content.TI_ID and (Info_Transformators.StartDateTime<=GETDATE() and ISNULL(Info_Transformators.FinishDateTime,'01-01-2100')>=GETDATE())
        join Hard_Transformators_TT TT on TT.TT_ID=Info_Transformators.TTA_ID
                                or TT.TT_ID=Info_Transformators.TTB_ID
                                or TT.TT_ID=Info_Transformators.TTC_ID
        join Dict_Transformators_TT_Model TTType on TTType.TTModel_ID=TT.TTModel_ID
        outer apply
        (
         select top 1 * from Info_Meters_TO_TI where TI_ID=Info_TI.TI_ID 
         and (Info_Meters_TO_TI.StartDateTime<=GETDATE() and ISNULL(Info_Meters_TO_TI.FinishDateTime,'01-01-2100')>=GETDATE())
         order by StartDateTime desc
        )Info_Meters_TO_TI        
		left join Hard_Meters on Hard_Meters.Meter_ID=Info_Meters_TO_TI.METER_ID
        where  
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
Order by Expl_InterrogatoryList_Content.Number , TTPhase
        
         


update @TT set TTCalibrationDate= REPLACE(TTCalibrationDate,'01.01.1900','н\д')



select * from @TT
order by TINumber, Number

end



go

grant EXECUTE on dbo.[usp2_InterrogatoryList_GetTT] to UserCalcService
go

grant EXECUTE on dbo.[usp2_InterrogatoryList_GetTT] to UserDeclarator
go

