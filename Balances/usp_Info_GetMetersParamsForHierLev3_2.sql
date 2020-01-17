if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParamsForMSK2')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParamsForMSK2
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParamsForHierLev3_2')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParamsForHierLev3_2
go
---------------------------------------------------------------------------
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
--		Апрель, 2009
--
-- Описание:
--
--		Данные необходимые для расчетов балансов МСК 
--		новая версия
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetMetersParamsForHierLev3_2]
(	
	@balanceHierLev3ID varchar(22)	
)

AS


begin
---Объединяем с данными по ТИ которые не состоят в балансах ПС
    select ibl.Balance_HierLev3_UN,ibl.ForAutoUse,ibl.HighLimit,ibl.LowerLimit,
			BalancePS_UN,ibd.BalanceSectionType_UN,ibd.TI_ID,ibd.ChannelType,ibd.Section_ID,ibd.TP_ID,ibd.ContrTI_ID,	
			case 
			when (not ibd.TI_ID is null) or (not ibd.TP_ID is null and tp.IsMoneyOurSide=1) then ti.PS_ID
			else cti.Contr_PS_ID
			end as PS_ID,
			ps.PSProperty as PSProperty,cps.PSProperty as CPSProperty,
			st.BalanceSectionName,
			st.BalanceSectionName2,			 
			st.BalanceSectionNumber,
			st.BalanceSectionNumber2,
			--Названия объектов
			case 
			when not ibd.TI_ID is null then ti.TIName
			when not ibd.ContrTI_ID is null then  cti.TIName
			when not ibd.PS_ID is null then ps.StringName 
			when not ibd.Section_ID is null then isl.SectionName
			when not ibd.TP_ID is null then tp.StringName
			end as StringName,
			case 
			when (not ibd.TI_ID is null) or (not ibd.TP_ID is null and tp.IsMoneyOurSide=1) then ps.StringName
			else cps.StringName
			end as PSName,
			case 
			when (not ibd.TI_ID is null) or (not ibd.TP_ID is null and tp.IsMoneyOurSide=1) then 1
			else 0
			end as IsOurSide
	from
	(select Balance_HierLev3_UN,ForAutoUse,HighLimit,LowerLimit from dbo.Info_Balance_HierLev3_List where Balance_HierLev3_UN = @balanceHierLev3ID) ibl
	left join 
	(select Balance_HierLev3_UN,BalanceSectionType_UN,BalancePS_UN,TI_ID,ContrTI_ID,ChannelType,Section_ID, TP_ID, PS_ID
			from dbo.Info_Balance_HierLev3_Description where ( (not (Section_ID is Null)) or (not (TI_ID is Null)) or (not (TP_ID is Null)) or (not (ContrTI_ID is Null)) or (BalanceSectionType_UN = 'MSK_POST_OT_RSK' or BalanceSectionType_UN = 'MSK_OTP_TO_RSK'  or BalanceSectionType_UN = 'MSK_RASXOD_SN_XN_PN_on_PS' ))) ibd
	on 
	ibd.Balance_HierLev3_UN = ibl.Balance_HierLev3_UN
	left join 
	dbo.Info_TP2 tp
	on ibd.TP_ID = tp.TP_ID
	left join 
	(select TI_ID,TP_ID,PS_ID,Voltage,TIName  from dbo.Info_TI) ti
	on 	ti.TI_ID = ibd.TI_ID or (tp.IsMoneyOurSide=1 and tp.TP_ID = ti.TP_ID)
	LEFT JOIN Dict_PS  ps
	ON ps.PS_ID = ti.PS_ID
	left join 
	(select ContrTI_ID,TP_ID2,Contr_PS_ID,TIName from dbo.Info_Contr_TI) cti
	on cti.ContrTI_ID = ibd.ContrTI_ID or (tp.IsMoneyOurSide=0 and tp.TP_ID = cti.TP_ID2)
	LEFT JOIN dbo.Dict_Contr_PS  cps
	ON cps.Contr_PS_ID = cti.Contr_PS_ID
	left join 
	(select * from dbo.Dict_Balance_Section_Types) st
	on (ibd.BalanceSectionType_UN = st.BalanceSectionType_UN) -- and (ibd.BalanceSectionType_UN = 'MSK_POST_OT_RSK' or ibd.BalanceSectionType_UN = 'MSK_OTP_TO_RSK'))
	left join 
	Info_Section_List isl
	on ibd.Section_ID = isl.Section_ID
 order by st.BalanceSectionNumber,st.BalanceSectionNumber2
end
---------------------------------------------------------------------------
go
   grant exec on usp2_Info_GetMetersParamsForHierLev3_2 to [UserCalcService]
go