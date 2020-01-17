if exists (select 1
          from sysobjects
          where  id = object_id('[usp2_NSI_Search_Meters]')
          and type in ('P','PC'))
   drop procedure [usp2_NSI_Search_Meters]
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		Август, 2014
--
-- Описание:
--
--		Поиск счетчиков по заданым параметрам
--
-- ======================================================================================

--поиск счетчиков
create proc [dbo].[usp2_NSI_Search_Meters]
	--тип поиска: 0- Счетчики на указанном объекте, 1-Поиск по заводскому номеру, 2- Поиск по модели, 3-Счетчики неподключеные к ТИ
	@Search_MeterParam tinyint, 
	--тип объекта (для @Search_MeterParam=0): 1- ур1, 2- ур2, 3- ур3, 4- ПС
	@objType tinyint, 
	--идентификатор объекта (для @Search_MeterParam=0):
	@objID int,
	--искомое значение (зав нмоер или модель) - для (для @Search_MeterParam=1,2)
	@searchValue varchar(200) 
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	set @searchValue = replace(@searchValue,' ','%')

	select  distinct
	FullPlace=
		isnull(Dict_HierLev1.StringName,'')+
		case when Dict_HierLev2.StringName is null then '' else  ', '+ isnull(Dict_HierLev2.StringName,'') end + 
		case when Dict_HierLev3.StringName is null then '' else  ', '+ isnull(Dict_HierLev3.StringName,'') end + 
		case when Dict_PS.StringName is null then '' else  ', '+ isnull(Dict_PS.StringName,'') end,
	isnull( Info_TI.TIName,'') TIName,
	Voltage=cast (isnull(Info_TI.Voltage,0.0) as float),
	Dict_Meters_Types.MeterTypeName Producer,
	isnull(Dict_Meters_Model.StringName,'') MeterModel,
	Hard_Meters.MeterSerialNumber,
	Hard_Meters.LinkNumber MeterLinkNumber,
	Info_Meters_TO_TI.StartDateTime,
	Info_Meters_TO_TI.FinishDateTime,
	ConnectedToUSPD= CAST (( case when Hard_MetersUSPD_Links.USPD_ID is not null then 1 else 0 end) as bit), 
	ConnectedToE422= CAST ((case when Hard_MetersE422_Links.E422_ID is not null  then 1 else 0 end) as bit) ,
	ConnectedToTI= CAST ((case when Info_TI.TI_ID is not null  then 1 else 0 end ) as bit) ,
	Hard_Meters.Meter_ID,
	Hard_MetersUSPD_Links.USPD_ID,
	Hard_MetersE422_Links.E422_ID,
	Info_TI.TI_ID

	from 
	Hard_Meters

	left join Info_Meters_TO_TI on Hard_Meters.Meter_ID= Info_Meters_TO_TI.METER_ID
	left join Info_TI on Info_Meters_TO_TI.TI_ID= Info_TI.TI_ID
	left join Dict_PS on Info_TI.PS_ID=Dict_PS.PS_ID 
	left join Dict_HierLev3 on Dict_HierLev3.HierLev3_ID= Dict_PS.HierLev3_ID 
	left join Dict_HierLev2 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID 
	left join Dict_HierLev1 on Dict_HierLev1.HierLev1_ID= Dict_HierLev2.HierLev1_ID 
	left join Dict_Meters_Model 
	on Dict_Meters_Model.MeterModel_ID=Hard_Meters.MeterModel_ID 

	left join Dict_Meters_Types on Dict_Meters_Types.MeterType_ID=Hard_Meters.MeterType_ID
	left join Hard_MetersUSPD_Links on Hard_MetersUSPD_Links.METER_ID=Hard_Meters.Meter_ID
	left join Hard_MetersE422_Links on Hard_MetersE422_Links.METER_ID=Hard_Meters.Meter_ID
	 
	where
	((@Search_MeterParam =1 and isnull(Hard_Meters.MeterSerialNumber,'') like '%'+@searchValue+'%')) 
	 
	or 
	
	(@Search_MeterParam =3 and Hard_Meters.Meter_ID not in(select Meter_ID from Info_Meters_TO_TI))
	 
	or 
	(@Search_MeterParam =0  
		and 
		(
			 (@objType =4 and isnull(@objID,0)=isnull(Dict_PS.PS_ID,0))
			 or
			 (@objType =3 and isnull(@objID,0)=isnull(Dict_HierLev3.HierLev3_ID,0))
			 or
			 (@objType =2 and isnull(@objID,0)=isnull(Dict_HierLev2.HierLev2_ID,0))
			 or
			 (@objType =1 and isnull(@objID,0)=isnull(Dict_HierLev1.HierLev1_ID,0))		 
		 )
	 )
	 
	or ((@Search_MeterParam =2 and isnull(Dict_Meters_Model.StringName,'') like '%'+@searchValue+'%'))

	order by FullPlace, Voltage desc, TIName,  MeterSerialNumber
END
GO

   grant EXECUTE on [usp2_NSI_Search_Meters] to [UserCalcService]
go
   grant EXECUTE on [usp2_NSI_Search_Meters] to [UserDeclarator]
go
   grant EXECUTE on [usp2_NSI_Search_Meters] to [UserImportService]
go
   grant EXECUTE on [usp2_NSI_Search_Meters] to [UserExportService]
go

 