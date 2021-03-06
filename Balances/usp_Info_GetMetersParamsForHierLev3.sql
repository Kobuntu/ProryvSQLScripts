if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParamsForMSK')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParamsForMSK
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParamsForHierLev3')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParamsForHierLev3
go


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Данные необходимые для расчетов балансов МСК 
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetMetersParamsForHierLev3]
(	
	@balanceHierLev3ID varchar(22),	
	@datestart datetime,
	@dateend datetime
	
)

AS


begin
---Объединяем с данными по ТИ которые не состоят в балансах ПС
    select ibd.Balance_HierLev3_UN,BalancePS_UN,
		ibd.BalanceSectionType_UN,ibd.TI_ID,
		ti.TIName,
		ti.PS_ID,
		cti.ContrTI_ID,
		cti.Contr_PS_ID,
		ibd.ChannelType,
		ibd.Section_ID,
		st.BalanceSectionName,
		st.BalanceSectionName2,			 
		st.BalanceSectionNumber,
		st.BalanceSectionNumber2
	from 
	(select Balance_HierLev3_UN,BalanceSectionType_UN,BalancePS_UN,TI_ID,ContrTI_ID,ChannelType,Section_ID
			from dbo.Info_Balance_HierLev3_Description where (Balance_HierLev3_UN = @balanceHierLev3ID and ((not (TI_ID is Null)) or (not (ContrTI_ID is Null)) or (BalanceSectionType_UN = 'MSK_POST_OT_RSK' or BalanceSectionType_UN = 'MSK_OTP_TO_RSK'  or BalanceSectionType_UN = 'MSK_RASXOD_SN_XN_PN_on_PS' )))) ibd
	left join 
	(select TI_ID,TIName,PS_ID from dbo.Info_TI) ti
	on ti.TI_ID = ibd.TI_ID
	left join 
	(select ContrTI_ID,TIName,Contr_PS_ID from dbo.Info_Contr_TI) cti
	on cti.ContrTI_ID = ibd.ContrTI_ID
	left join 
	(select * from dbo.Dict_Balance_Section_Types) st
	on (ibd.BalanceSectionType_UN = st.BalanceSectionType_UN) -- and (ibd.BalanceSectionType_UN = 'MSK_POST_OT_RSK' or ibd.BalanceSectionType_UN = 'MSK_OTP_TO_RSK'))
	union
--Сечения которые просто добавлены как точки, для них выбираем ТИ которые входят в эти сечения
    select ibd.Balance_HierLev3_UN,null as BalancePS_UN,
		ibd.BalanceSectionType_UN,
		ti.TI_ID,
		ti.TIName,
		ti.PS_ID,
		cti.ContrTI_ID,
		cti.Contr_PS_ID,
		cast(
		case ibd.BalanceSectionType_UN 
		when 'MSK_POST_OT_ES' then (case when (ti.TI_ID is null) then 2 else 1 end)
		when 'MSK_POST_OT_SMEG' then (case when (ti.TI_ID is null) then 2 else 1 end)
		when 'MSK_POST_OT_MSK' then (case when (ti.TI_ID is null) then 2 else 1 end)
		when 'MSK_POST_OT_MSK' then (case when (ti.TI_ID is null) then 2 else 1 end)
		else (case when (ti.TI_ID is null) then 1 else 2 end) end  as tinyint)
		as ChannelType,
		null as Section_ID,
		st.BalanceSectionName,
		st.BalanceSectionName2,			 
		st.BalanceSectionNumber,
		st.BalanceSectionNumber2
	from 
	(select Balance_HierLev3_UN,BalanceSectionType_UN,Section_ID
			from dbo.Info_Balance_HierLev3_Description where (Balance_HierLev3_UN = @balanceHierLev3ID and ((TI_ID is Null) and (ContrTI_ID is Null) and (not BalanceSectionType_UN = 'MSK_POST_OT_RSK') and (not BalanceSectionType_UN = 'MSK_OTP_TO_RSK') and (not BalanceSectionType_UN = 'MSK_RASXOD_SN_XN_PN_on_PS')))) ibd
	left join 
	(SELECT  Section_ID, TI_ID
			FROM [dbo].[Info_Section_Description]) isd
	 on isd.Section_ID = ibd.Section_ID
	left join 
	(SELECT  Section_ID,ContrTI_ID
			FROM [dbo].[Info_Section_Description_Contr]) сisd
	 on сisd.Section_ID = ibd.Section_ID
	left join 
	(select t.TI_ID,TIName,t.PS_ID from dbo.Info_TI t
		join Dict_PS p 
		on t.PS_ID = p.PS_ID and p.PSProperty<>0 ) ti
	on ti.TI_ID = isd.TI_ID
	left join 
	(select t.ContrTI_ID,TIName,t.Contr_PS_ID from dbo.Info_Contr_TI t
		join Dict_Contr_PS p 
		on t.Contr_PS_ID = p.Contr_PS_ID and p.PSProperty<>0 
	) cti
	on cti.ContrTI_ID = сisd.ContrTI_ID
	
	left join 
	(select * from dbo.Dict_Balance_Section_Types) st
	on (ibd.BalanceSectionType_UN = st.BalanceSectionType_UN)
-----------------------------------------------------------
 order by st.BalanceSectionNumber,st.BalanceSectionNumber2
end
go
   grant exec on usp2_Info_GetMetersParamsForHierLev3 to [UserCalcService]
go