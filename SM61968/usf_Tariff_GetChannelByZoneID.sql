set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Tariff_GetChannelByZoneID')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Tariff_GetChannelByZoneID
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2011
--
-- Описание:
--
--		Определяем тарифный канал по ТИ, номеру обычного канала, идентификатору тарифной зоны
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Tariff_GetChannelByZoneID]
(	
	@ti_id int, --ТИ
	@eventDateTime DateTime, --Дата, время
	@ChannelType tinyint, --Канал
	@tariffZoneNumber int -- идентификатор тарифной зоны
)
RETURNS tinyint
AS
BEGIN
	
	if (@tariffZoneNumber > 0) 
		return (select top 1 ISNULL(
		case @Channeltype 
		when 1 then z.ChannelType1 
		when 2 then z.ChannelType2 
		when 3 then z.ChannelType3 
		when 4 then z.ChannelType4 
		end, 0) from dbo.DictTariffs_ToTI ti
		left join dbo.DictTariffs_Zones z on ti.Tariff_ID = z.Tariff_ID and (z.TariffZone_ID % 10) = @tariffZoneNumber
		where ti.TI_ID = @ti_id and @eventDateTime >= ti.StartDateTime 
		and (ti.FinishDateTime is null or @eventDateTime <= ti.FinishDateTime));
		
	return @ChannelType;
end;
go
grant EXECUTE on usf2_Tariff_GetChannelByZoneID to [UserCalcService]
go
grant EXECUTE on usf2_Tariff_GetChannelByZoneID to [UserMaster61968Service]
go