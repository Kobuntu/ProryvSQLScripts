if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetGlobalDetailTree')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetGlobalDetailTree
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--Устарела
create proc [dbo].[usp2_Info_GetGlobalDetailTree]

as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

--Точки поставки
	select tp.TP_ID,StringName,TPMode,IsMoneyOurSide,EvalModeOurSide,EvalModeContr,sd.Section_ID,ExcludeFromXMLExport,IASection,IsMoneyOurSideMode2, DirectConsumer_ID, TPATSCode  from dbo.Info_TP2 tp
	left join
	dbo.Info_Section_Description2 sd
	on tp.TP_ID = sd.TP_ID
	order by TP_ID  	
--Сечения
	select Section_ID,SectionName,SectionType,HierLev1_ID,HierLev2_ID,HierLev3_ID,PS_ID, ATSSectionCode,SubjORE_ID  from dbo.Info_Section_List order by HierLev3_ID,HierLev2_ID,HierLev1_ID,PS_ID
--ПС
	select PS_ID as ID,StringName as StringName,cast(HierLev3_ID as int) as P_ID, 3 as [Type], PSProperty, PSVoltage, PSType  from DICT_PS order by HierLev3_ID,PS_ID
--МСК
	select HierLev3_ID as ID,StringName as StringName,cast(HierLev2_ID as int) as P_ID, 2 as [Type]  from DICT_HierLev3 order by HierLev2_ID,HierLev3_ID
--ПМС
	select HierLev2_ID as ID,StringName as StringName,cast(HierLev1_ID as int)  as P_ID, 1 as [Type] from DICT_HierLev2 order by HierLev1_ID, HierLev2_ID
--Подстанции и предприятия контр агента
	select distinct co.ContrObject_ID,co.StringName as ContrObjectName,cast(cps.HierLev1_ID as int) as HierLev1_ID, cps.Contr_PS_ID, cps.StringName as ContrPSName, cps.PSProperty, co.ContrINN, co.EMailAddress,co.SubjORE_ID, cps.PSVoltage  from  
	(select ContrObject_ID,Contr_PS_ID from dbo.Info_Contr_TI where not ContrObject_ID is null) cti
	left join dbo.Dict_Contr_PS cps on cti.Contr_PS_ID = cps.Contr_PS_ID
	left join dbo.Dict_Contr_Objects co on cti.ContrObject_ID = co.ContrObject_ID
	order by HierLev1_ID, ContrObject_ID, Contr_PS_ID
--МЭС
	select cast(HierLev1_ID as int) as ID,StringName as StringName, cast(0 as int)  as P_ID, 0 as [Type] from DICT_HierLev1 HierLev1_ID order by HierLev1_ID
end
go
   grant EXECUTE on usp2_Info_GetGlobalDetailTree to [UserCalcService]
go
