 if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Get_ExportServiceJob_TI')
          and type in ('P','PC'))
   drop procedure usp2_Get_ExportServiceJob_TI
go
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].usp2_Get_ExportServiceJob_TI
				 @job_ID int, @sectionType int
AS BEGIN
SET NOCOUNT ON
SET QUOTED_IDENTIFIER, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, ANSI_PADDING ON
SET NUMERIC_ROUNDABORT OFF
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
  

select distinct INfo_TI.TI_ID
from 
Expl_XML_ExportServiceJob_object
join vw_Dict_Hierarchy 
on 
(vw_Dict_Hierarchy.HierLev1_ID=Expl_XML_ExportServiceJob_object.HierLev1_ID and Expl_XML_ExportServiceJob_object.HierLev1_ID is not null)
or 
(vw_Dict_Hierarchy.HierLev2_ID=Expl_XML_ExportServiceJob_object.HierLev2_ID and Expl_XML_ExportServiceJob_object.HierLev2_ID is not null)
or 
(vw_Dict_Hierarchy.HierLev3_ID=Expl_XML_ExportServiceJob_object.HierLev3_ID and Expl_XML_ExportServiceJob_object.HierLev3_ID is not null)
or 
(vw_Dict_Hierarchy.PS_ID=Expl_XML_ExportServiceJob_object.PS_ID and Expl_XML_ExportServiceJob_object.PS_ID is not null)
or 
(vw_Dict_Hierarchy.TI_ID=Expl_XML_ExportServiceJob_object.TI_ID and Expl_XML_ExportServiceJob_object.TI_ID is not null)
join 
	INfo_TI on (Info_TI.TI_ID= vw_Dict_Hierarchy.TI_ID and vw_Dict_Hierarchy.TI_ID is not null)
where 
Expl_XML_ExportServiceJob_object.ExportServiceJob_ID=@job_ID
and info_TI.Deleted=0
--and 
--(
--(@UseAlternativeAIS=1 and (isnull(Info_TI.ATSAIS_ID,'') like @AISID or isnull(Info_TI.ATSAIS_ID2,'') like @AISID))
--or
--(@UseAlternativeAIS=0 and (isnull(Info_TI.ATSAIS_ID,'') like @AISID ))
--)

/*
сечения исключаем.. по ним как и в ручном экспорте делаем
union

select distinct INfo_TI.TI_ID
from 
Expl_XML_ExportServiceJob_object
join Info_Section_List on  Info_Section_List.Section_ID= Expl_XML_ExportServiceJob_object.Section_ID
join Info_Section_Description2 on Info_Section_Description2.Section_ID=Info_Section_List.Section_ID
join Info_Tp2 on Info_Section_Description2.TP_ID=Info_Tp2.TP_ID
join Info_TP2_OurSide_Formula_List on Info_Tp2.TP_ID= Info_TP2_OurSide_Formula_List.TP_ID
join Info_TP2_OurSide_Formula_Description on Info_TP2_OurSide_Formula_Description.Formula_UN=Info_TP2_OurSide_Formula_List.Formula_UN
join INfo_TI on Info_TI.TI_ID= Info_TP2_OurSide_Formula_Description.TI_ID
where 
Expl_XML_ExportServiceJob_object.ExportServiceJob_ID=@job_ID
and Info_Section_Description2.StartDateTime<=getdate()
and isnull(Info_Section_Description2.FinishDateTime,'01-01-2100')>=getdate()
and Info_TP2_OurSide_Formula_List.StartDateTime<=getdate()
and isnull(Info_TP2_OurSide_Formula_List.FinishDateTime,'01-01-2100')>=getdate()
and info_TI.Deleted=0
and 
(
--PPIMain
(@sectionType=0 and Info_Tp2.IsMoneyOurSide=1)
--PPIReserve
or (@sectionType=1 and Info_Tp2.IsMoneyOurSide=0)
--PSIMain
or (@sectionType=2 and Info_Tp2.IsMoneyOurSideMode2=1)
--PSIReserve
or (@sectionType=3 and Info_Tp2.IsMoneyOurSideMode2=0)
)
and Info_TP2.ExcludeFromXMLExport<>1


--and 
--(
--(@UseAlternativeAIS=1 and (isnull(Info_TI.ATSAIS_ID,'') like @AISID or isnull(Info_TI.ATSAIS_ID2,'') like @AISID))
--or
--(@UseAlternativeAIS=0 and (isnull(Info_TI.ATSAIS_ID,'') like @AISID ))
--)



union

select distinct INfo_TI.TI_ID
from 
Expl_XML_ExportServiceJob_object
join Info_Section_List on  Info_Section_List.Section_ID= Expl_XML_ExportServiceJob_object.Section_ID
join Info_Section_Description2 on Info_Section_Description2.Section_ID=Info_Section_List.Section_ID
join Info_Tp2 on Info_Section_Description2.TP_ID=Info_Tp2.TP_ID
join Info_TP2_Contr_Formula_List on Info_Tp2.TP_ID= Info_TP2_Contr_Formula_List.TP_ID
join Info_TP2_Contr_Formula_Description on Info_TP2_Contr_Formula_Description.Formula_UN=Info_TP2_Contr_Formula_List.Formula_UN
join INfo_TI on Info_TI.TI_ID= Info_TP2_Contr_Formula_Description.TI_ID
where 
Expl_XML_ExportServiceJob_object.ExportServiceJob_ID=@job_ID
and Info_Section_Description2.StartDateTime<=getdate()
and isnull(Info_Section_Description2.FinishDateTime,'01-01-2100')>=getdate()
and Info_TP2_Contr_Formula_List.StartDateTime<=getdate()
and isnull(Info_TP2_Contr_Formula_List.FinishDateTime,'01-01-2100')>=getdate()
and info_TI.Deleted=0
and 
(
--PPIMain
(@sectionType=0 and Info_Tp2.IsMoneyOurSide=0)
--PPIReserve
or (@sectionType=1 and Info_Tp2.IsMoneyOurSide=1)
--PSIMain
or (@sectionType=2 and Info_Tp2.IsMoneyOurSideMode2=0)
--PSIReserve
or (@sectionType=3 and Info_Tp2.IsMoneyOurSideMode2=1)
)
and Info_TP2.ExcludeFromXMLExport<>1

--and 
--(
--(@UseAlternativeAIS=1 and (isnull(Info_TI.ATSAIS_ID,'') like @AISID or isnull(Info_TI.ATSAIS_ID2,'') like @AISID))
--or
--(@UseAlternativeAIS=0 and (isnull(Info_TI.ATSAIS_ID,'') like @AISID ))
--)

*/
END
GO

grant EXECUTE on usp2_Get_ExportServiceJob_TI to [UserCalcService]
go
grant EXECUTE on usp2_Get_ExportServiceJob_TI to [UserDeclarator]
go
grant EXECUTE on usp2_Get_ExportServiceJob_TI to [UserExportService]
go



 