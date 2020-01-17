if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetMetersParamsForHierLev0')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetMetersParamsForHierLev0
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
--		Данные необходимые для расчетов балансов ЕНЭС 
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetMetersParamsForHierLev0]
(	
	@balanceHierLev0ID varchar(22),
	@IsOurSidebyBusRelation bit, --Относительно какой стороны берем информацию (false(0) - относительно контрольной(нерасчетной))
	@MoneyOurSideMode tinyint = 0
)

AS


begin
---Объединяем с данными по ТИ которые не состоят в балансах ПС
    select ibl.Balance_HierLev0_UN,ibl.ForAutoUse,ibl.HighLimit,ibl.LowerLimit,
			BalancePS_UN,ibd.BalanceSectionType_UN,ibd.TI_ID,ibd.Section_ID,ibd.TP_ID,ibd.ContrTI_ID,ibd.PS_ID as SeparatePS_ID,	
			case when ibd.TP_ID is not null 
				then case when tp.IsCA = 0 then tp.PS_ID else null end 
				else ti.PS_ID 
			end as PS_ID, 
			ps.PSProperty as PSProperty,
			cps.PSProperty as CPSProperty,
			case when ibd.TP_ID is not null 
				then case when tp.IsCA = 1 then tp.PS_ID else null end 
				else cti.Contr_PS_ID 
			end as Contr_PS_ID,
			st.BalanceSectionName,
			st.BalanceSectionName2,			 
			st.BalanceSectionNumber,
			st.BalanceSectionNumber2, 
			case when ibd.TP_ID is not null 
				then tp.Voltage
				else ti.Voltage
			end as ti_voltage, 
			cti.Voltage as cti_voltage, 
			tp.IsMoneyOurSide, 
			st.IsUseInGeneralBalance, st.IsRSK,
			sl.Section_ID as MainSection_ID, sl.SubjORE_ID, ibd.ChannelTypeAP, ibd.ChannelTypeAO
	from
	(select Balance_HierLev0_UN,ForAutoUse,HighLimit,LowerLimit from dbo.Info_Balance_HierLev0_List where Balance_HierLev0_UN = @balanceHierLev0ID) ibl
	left join 
	(select Balance_HierLev0_UN,BalanceSectionType_UN,BalancePS_UN,TI_ID,ContrTI_ID,Section_ID, TP_ID,PS_ID, ChannelTypeAP, ChannelTypeAO, StringNumber
			from dbo.Info_Balance_HierLev0_Description) ibd
	on 
	ibd.Balance_HierLev0_UN = ibl.Balance_HierLev0_UN
	left join 
	(select TI_ID,TP_ID,PS_ID,Voltage from dbo.Info_TI) ti
	on 	ti.TI_ID = ibd.TI_ID
	LEFT JOIN Dict_PS  ps
	ON ps.PS_ID = ti.PS_ID or ibd.PS_ID = ps.PS_ID
	--Определяем первую попавшуюся ТП для отдельно описанной ПС
	left join 
	(select top 1 TI_ID,PS_ID, TP_ID from dbo.Info_TI) ti_ps on ti_ps.PS_ID = ibd.PS_ID
	left join 
	(select ContrTI_ID,TP_ID2,Contr_PS_ID,Voltage from dbo.Info_Contr_TI) cti
	on cti.ContrTI_ID = ibd.ContrTI_ID
	LEFT JOIN dbo.Dict_Contr_PS  cps
	ON cps.Contr_PS_ID = cti.Contr_PS_ID

	outer apply usf2_Info_GetTPParams(ibd.TP_ID, null, null, null, @IsOurSidebyBusRelation, @MoneyOurSideMode, null) tp
	
	left join dbo.Dict_Balance_Section_Types st
	on ibd.BalanceSectionType_UN = st.BalanceSectionType_UN 
---Определяем сечение и код ОРЕ
	left join dbo.Info_Section_Description2 sd on sd.tp_id = tp.TP_ID 
	left join dbo.Info_Section_List sl on sl.Section_ID = sd.Section_ID or ibd.Section_ID = sl.Section_ID
	
	where st.BalanceSectionType_UN <> 'ENES_220_MSK_POST_OT_330' --Пропускаем эту подгруппу (просьба Красновой)
	
 order by st.BalanceSectionNumber,st.BalanceSectionNumber2, sl.SubjORE_ID, ti.PS_ID, cti.Contr_PS_ID, ibd.StringNumber, ibd.TI_ID,ibd.ContrTI_ID,ibd.TP_ID
end
---------------------------------------------------------------------------
go
   grant exec on usp2_Info_GetMetersParamsForHierLev0 to [UserCalcService]
go