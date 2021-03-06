if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetGlobalTree_ContrObjects')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetGlobalTree_ContrObjects
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create proc [dbo].[usp2_Info_GetGlobalTree_ContrObjects]

as
begin
	--Подстанции и предприятия контр агента
	select distinct co.ContrObject_ID,co.StringName as COName,cast(cps.HierLev1_ID as int) as HierLev1_ID, cps.Contr_PS_ID, cps.StringName as CPSName, cps.PSProperty, co.ContrINN, co.EMailAddress,ISNULL(co.SubjORE_ID,1) as SubjORE_ID, cps.PSVoltage  
	into #y
	from  
	(select ContrObject_ID,Contr_PS_ID from dbo.Info_Contr_TI) cti
	left join dbo.Dict_Contr_PS cps on cti.Contr_PS_ID = cps.Contr_PS_ID
	left join dbo.Dict_Contr_Objects co on cti.ContrObject_ID = co.ContrObject_ID
	where co.ContrObject_ID is not null or cps.Contr_PS_ID is not null
	order by HierLev1_ID, ContrObject_ID, Contr_PS_ID

	--Объединяем с предприятиями у которых нет точек ТИ КА
	select * from #y
	union 
	select ContrObject_ID,StringName as COName, NULL, NULL, NULL, NULL, ContrINN, EMailAddress,SubjORE_ID, NULL  
	from dbo.Dict_Contr_Objects b
	WHERE NOT EXISTS (select distinct ContrObject_ID from #y a  WHERE a.ContrObject_ID = b.ContrObject_ID)

end

go
   grant EXECUTE on usp2_Info_GetGlobalTree_ContrObjects to [UserCalcService]
go
