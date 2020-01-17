if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Контроль_дублирования_и_миграции')
          and type in ('P','PC'))
   drop procedure usp2_Контроль_дублирования_и_миграции
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
--		Март, 2016
--
-- Описание:
--
--		Контроль дублирования и миграции
--
-- ======================================================================================

create proc [dbo].[usp2_Контроль_дублирования_и_миграции]
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	declare @dtNow DateTime;
	set @dtNow = GetDate();
	select ti.TI_ID, ti.PS_ID, link.USPD_ID as УСПД_Сбора, hm.Meter_ID, hcl.USPD_ID as УСПД_Питания, hm.MeterSerialNumber
	into #tmp
	from Hard_Meters hm
	cross apply --Только описанные счетчики
	(
		select top 1 METER_ID, TI_ID from Info_Meters_TO_TI m
		where m.METER_ID = hm.Meter_ID and (FinishDateTime is null or FinishDateTime >= @dtNow)
		order by m.StartDateTime desc
	) m			
	join Info_TI ti on ti.TI_ID = m.TI_ID
	join dbo.Hard_MetersUSPD_Links link on link.METER_ID = m.METER_ID
	--Ищем к какому каналу сбора привязана данная ПС, затем определяем УСПД
	left join [dbo].[Hard_CommChannels] hcc on hcc.PS_ID = ti.PS_ID
	left join [dbo].[Hard_USPDCommChannels_Links] hcl on hcl.CommChannel_ID = hcc.CommChannel_ID
	where hm.MeterType_ID = 2006 and ti.PS_ID in 
	(
	  select distinct ps.PS_ID from [dbo].[Dict_PS_PowerSupply_PS_List] ps
	  --where PS_ID = 100 тут фильтр по ПС (если понадобится на будущее)
	  union 
	  select distinct ps.PowerSupplyPS_ID from [dbo].[Dict_PS_PowerSupply_PS_List] ps
	  --where PS_ID = 100 и тут
	)
	--Отсеиваем нормальные
	and (hcl.USPD_ID is null or hcl.USPD_ID<>link.USPD_ID)
	select #tmp.*, ti.TIName, ps.StringName as ПС, pss.StringName as ПС_Сбора, powersppss.StringName as ПС_Питающая from #tmp
	join Info_TI ti on ti.TI_ID = #tmp.TI_ID
	join Dict_PS ps on ps.PS_ID = #tmp.PS_ID
	left join [dbo].[Hard_USPDCommChannels_Links] hcl on hcl.USPD_ID = УСПД_Сбора
	left join [dbo].[Hard_CommChannels] hcc on hcc.CommChannel_ID = hcl.CommChannel_ID
	left join Dict_PS pss on pss.PS_ID = hcc.PS_ID
	left join [Dict_PS_PowerSupply_PS_List] powersp on powersp.PS_ID = hcc.PS_ID
	left join Dict_PS powersppss on powersppss.PS_ID = powersp.PowerSupplyPS_ID
	where #tmp.ti_id in 
	(
		--Дубликаты
		select TI_ID 
		from #tmp 
		group by (TI_ID)
		having Count(TI_ID) > 1
	)
	or #tmp.ti_id in 
	(
		select TI_ID from #tmp
		where УСПД_Питания is not null 
		-- and УСПД_Питания<>УСПД_Сбора -- старый фильтр не подошел для 2-х УСПД на ПС
		and УСПД_Питания not in 
		(
		select USPD_ID from Hard_USPDCommChannels_Links
		where Hard_USPDCommChannels_Links.CommChannel_ID= hcl.CommChannel_ID
		)
	)
	order by TIName, ps.StringName
	drop table #tmp
end

go
  grant EXECUTE on usp2_Контроль_дублирования_и_миграции to [UserCalcService]
go