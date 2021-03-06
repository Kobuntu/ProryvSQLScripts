if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetGlobalHierarchyTree')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetGlobalHierarchyTree
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create proc [dbo].[usp2_Info_GetGlobalHierarchyTree]

as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	--ТИ
	select TI_ID as ID,TIName as StringName,PS_ID as P_ID, 4 as [Type] from Info_TI order by ps_id, ti_id
	--ПС
	select PS_ID as ID,StringName as StringName,cast(HierLev3_ID as int) as P_ID, 3 as [Type]  from DICT_PS order by HierLev3_ID,PS_ID
	--МСК
	select HierLev3_ID as ID,StringName as StringName,cast(HierLev2_ID as int) as P_ID, 2 as [Type]  from DICT_HierLev3 order by HierLev2_ID,HierLev3_ID
	--ПМС
	select HierLev2_ID as ID,StringName as StringName,cast(HierLev1_ID as int)  as P_ID, 1 as [Type] from DICT_HierLev2 order by HierLev1_ID, HierLev2_ID
	--МЭС
	select cast(HierLev1_ID as int) as ID,StringName as StringName, cast(0 as int)  as P_ID, 0 as [Type] from DICT_HierLev1 HierLev1_ID order by HierLev1_ID
end
go
   grant EXECUTE on usp2_Info_GetGlobalHierarchyTree to [UserCalcService]
go
