if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_ReplacementOfAccountingFacilities')
          and type in ('P','PC'))
   drop procedure usp2_Rep_ReplacementOfAccountingFacilities
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
--		Июнь, 2012
--
-- Описание:
--
--		Информация для отчета о произведенных заменах средств учета на объектах потребителей
--
-- ======================================================================================
create proc [dbo].[usp2_Rep_ReplacementOfAccountingFacilities]
	@PS_Array varchar(max),
	@DTStart DateTime,
	@DTEnd DateTime
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select distinct ti.TI_ID as Items into #tis 
from usf2_Utils_Split(@PS_Array, ';') usf
join Info_TI ti on ti.PS_ID = usf.Items

--Информация по смене трансформаторов
select it.* 
into #transInfo
from Info_Transformators it
	inner join 
	(
		select Count(TI_ID) as [count], ti_id from Info_Transformators
		where 
		StartDateTime <= @DTEnd
		and FinishDateTime >= @DTStart
		and ti_id in (select Items from #tis)
		group by TI_ID
		having Count(TI_ID)>1
	) sit
	on 
	StartDateTime <= @DTEnd
	and FinishDateTime >= @DTStart
	and it.TI_ID = sit.ti_id
	order by it.TI_ID, it.StartDateTime, it.FinishDateTime

--Информация по смене счетчика
select usf.TI_ID, usf.MeterSerialNumber, usf.StartDateTime, usf.FinishDateTime, imh.ChannelType, imh.FirstData, imh.LastData,
mt.MeterTypeName, -- Тип счетчика
z.StringName as ZoneName --Информация по тарифу
into #metersInfo
from 
(
	select TI_ID from Info_Meters_TO_TI
	where StartDateTime <= @DTEnd and FinishDateTime >= @DTStart and TI_ID in (select Items from #tis) 
	group by TI_ID
	having COUNT(TI_ID) > 1
) tis
cross apply dbo.usf2_Utils_Monit_Exchanges_Meters_TO(tis.TI_ID,@DTStart,@DTEnd) usf

--Информация по типу счетчика
join Hard_Meters hm on hm.Meter_ID = usf.METER_ID
join dbo.Dict_Meters_Types mt on mt.MeterType_ID = hm.MeterType_ID
--История смены тарифов(пследние значения перед сменой и после смены)
left join dbo.Info_Meters_ReplaceHistory_Channels imh 
on usf.MetersReplaceSession_ID = imh.MetersReplaceSession_ID
--Берем только записи где отсутствует запись по каналу или только данные по тарифам, и только по активной энергии
and imh.ChannelType is null or (imh.ChannelType > 10 and imh.ChannelType % 10 < 3)
--Названия тарифных зон (для отчета)
left join dbo.DictTariffs_ToTI t on t.TI_ID = tis.TI_ID
left join dbo.DictTariffs_Zones z on z.Tariff_ID = t.Tariff_ID 
and (ChannelType1 = imh.ChannelType or ChannelType2 = imh.ChannelType or ChannelType3 = imh.ChannelType or ChannelType4 = imh.ChannelType)
 order by tis.TI_ID, FinishDateTime;

--Информация по точке
select TI_ID, ti.TIName, ps.StringName as PSName, h3.StringName as H3Name,
dbo.usf2_Info_GetTariffChannelsForTIUseChannelReverse(ti.TI_ID, ti.AbsentChannelsMask, case when ti.AIATSCode=2 then 1 else 0 end) as Список_тарифных_каналов
from Info_TI ti
join Dict_PS ps on ps.PS_ID = ti.PS_ID
join Dict_HierLev3 h3 on ps.HierLev3_ID = h3.HierLev3_ID
where ti.TI_ID in (select Items from #tis)
and 
(
	ti.TI_ID in (select distinct TI_ID from #transInfo)
	OR 
	ti.TI_ID in (select distinct TI_ID from #metersInfo)
)
order by PSName desc, TIName desc 

select * from #transInfo
select * from #metersInfo

drop table #tis;
drop table #transInfo
drop table #metersInfo

end
go
   grant EXECUTE on usp2_Rep_ReplacementOfAccountingFacilities to [UserCalcService]
go



