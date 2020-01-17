if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParamsForHierLev1Flow')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParamsForHierLev1Flow
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
--		Май, 2009
--
-- Описание:
--
--		Данные необходимые для расчетов балансов для группы межрегиональных перетоков (ФСК)
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetMetersParamsForHierLev1Flow]
(	
	@balanceHierLev0ID varchar(22), 
	@IsOurSidebyBusRelation bit, --Относительно какой стороны берем информацию (false(0) - относительно контрольной(нерасчетной))
	@MoneyOurSideMode tinyint = 0	
	
)
AS
begin
	--Берем точки поставки
    select l.HierLev1_ID as MainHierLev1, d.Balance_HierLev0_UN, d.TP_ID, d.HierLev1_ID as SendHierLev1, ISNULL(3 - VoltageLevel,tf.PSProperty) as PSProperty,tf.StringName as TPName, dhm.StringName as MainHierLev1Name,dhs.StringName as SendHierLev1Name, cast(8 as int) as TypeHierarchy, d.ChannelTypeAP, d.ChannelTypeAO, Voltage, cast(null as integer) as Section_Id, d.StringNumber  from 
	(
		select * from Info_Balance_HierLev0_Description 
		where Balance_HierLev0_UN = @balanceHierLev0ID and BalanceSectionType_UN = 'ENES_MSK_Peretoki' and  not TP_ID is null 
	) d
	join Info_Balance_HierLev0_List l on l.Balance_HierLev0_UN = d.Balance_HierLev0_UN
	outer apply usf2_Info_GetTPParams(d.TP_ID, null,null,null,@IsOurSidebyBusRelation,@MoneyOurSideMode, null) tf
	left join dbo.Dict_HierLev1 dhm on l.HierLev1_ID = dhm.HierLev1_ID
	left join dbo.Dict_HierLev1 dhs on d.HierLev1_ID = dhs.HierLev1_ID
	--Объединяем с точкми поставки из сечений входящих в данную подгруппу
	union
	select l.HierLev1_ID as MainHierLev1, d.Balance_HierLev0_UN, sl.TP_ID, d.HierLev1_ID as SendHierLev1,ISNULL(3 - VoltageLevel,tf.PSProperty) as PSProperty,tf.StringName as TPName, dhm.StringName as MainHierLev1Name,dhs.StringName as SendHierLev1Name, cast(8 as int) as TypeHierarchy, d.ChannelTypeAP, d.ChannelTypeAO, Voltage, d.Section_Id, d.StringNumber from 
	(
		select * from Info_Balance_HierLev0_Description 
		where Balance_HierLev0_UN = @balanceHierLev0ID and BalanceSectionType_UN = 'ENES_MSK_Peretoki' and not Section_id is null
	) d
	join Info_Balance_HierLev0_List l on l.Balance_HierLev0_UN = d.Balance_HierLev0_UN
	join dbo.Info_Section_Description2 sl on sl.Section_ID = d.Section_id
	outer apply usf2_Info_GetTPParams(sl.TP_ID, null,null,null,@IsOurSidebyBusRelation,@MoneyOurSideMode, null) tf
	left join dbo.Dict_HierLev1 dhm on l.HierLev1_ID = dhm.HierLev1_ID
	left join dbo.Dict_HierLev1 dhs on d.HierLev1_ID = dhs.HierLev1_ID
	--Объединяем с ТИ
	union all	
	select l.HierLev1_ID as MainHierLev1, d.Balance_HierLev0_UN, d.TI_ID, d.HierLev1_ID as SendHierLev1,dp.PSProperty,it.TIName as TPName, dhm.StringName as MainHierLev1Name,dhs.StringName as SendHierLev1Name, cast(4 as int) as TypeHierarchy, d.ChannelTypeAP, d.ChannelTypeAO, it.Voltage, cast(null as integer) as Section_Id, d.StringNumber from 
	(
		select * from Info_Balance_HierLev0_Description 
		where Balance_HierLev0_UN = @balanceHierLev0ID and BalanceSectionType_UN = 'ENES_MSK_Peretoki' and not ti_id is null
	) d
	join Info_Balance_HierLev0_List l on l.Balance_HierLev0_UN = d.Balance_HierLev0_UN
	join Info_ti it on it.ti_id = d.ti_id
	join Dict_ps dp on dp.ps_id = it.ps_id
	left join dbo.Dict_HierLev1 dhm on l.HierLev1_ID = dhm.HierLev1_ID
	left join dbo.Dict_HierLev1 dhs on d.HierLev1_ID = dhs.HierLev1_ID
	--Объединяем с ТИ КА
	union all	
	select l.HierLev1_ID as MainHierLev1, d.Balance_HierLev0_UN, d.ContrTI_ID, d.HierLev1_ID as SendHierLev1,dp.PSProperty,it.TIName as TPName, dhm.StringName as MainHierLev1Name,dhs.StringName as SendHierLev1Name, cast(6 as int) as TypeHierarchy, d.ChannelTypeAP, d.ChannelTypeAO, it.Voltage, cast(null as integer) as Section_Id, d.StringNumber from 
	(
		select * from Info_Balance_HierLev0_Description 
		where Balance_HierLev0_UN = @balanceHierLev0ID and BalanceSectionType_UN = 'ENES_MSK_Peretoki' and not Contrti_id is null
	) d
	join Info_Balance_HierLev0_List l on l.Balance_HierLev0_UN = d.Balance_HierLev0_UN
	join Info_Contr_ti it on it.Contrti_id = d.Contrti_id
	join Dict_Contr_ps dp on dp.Contr_ps_id = it.Contr_ps_id
	left join dbo.Dict_HierLev1 dhm on l.HierLev1_ID = dhm.HierLev1_ID
	left join dbo.Dict_HierLev1 dhs on d.HierLev1_ID = dhs.HierLev1_ID
	order by SendHierLev1, PSProperty, StringNumber
end
---------------------------------------------------------------------------
go
   grant exec on usp2_Info_GetMetersParamsForHierLev1Flow to [UserCalcService]
go