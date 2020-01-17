if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ReverseTariffChannel')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ReverseTariffChannel
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
--		Май, 2011
--
-- Описание:
--
--		Переворачиваем тарифицированный канал
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_ReverseTariffChannel]
(	
	@IsCA bit,
	@ChannelType tinyint,
	
	@AIATSCode tinyint,
	@AOATSCode tinyint,
	@RIATSCode tinyint,
	@ROATSCode tinyint,
	
	@ti_id int,
	@DTStart DateTime,
	@DTEnd DateTime
)
RETURNS tinyint
AS
begin

declare @isInverted bit;

set @isInverted = case when @AIATSCode=2 then 1 else 0 end;

set @isInverted = ISNULL((select top 1 IsInverted 
	from [dbo].[ArchCalc_Channel_InversionStatus] 
	where TI_ID = @ti_id and StartDateTime <= @DTEnd and (FinishDateTime is null or FinishDateTime >= @DTStart)
	order by StartDateTime desc), @isInverted); 

--Если это обычный канал
if (@isInverted = 0) begin 
	return @ChannelType;
end

if (@ChannelType<=4) begin
	if (@ChannelType = 1) return 2;
	if (@ChannelType = 2) return 1;
	if (@ChannelType = 3) return 4;
	if (@ChannelType = 4) return 3;
end

declare
@Res tinyint
if (@IsCA = 0) begin
	set @Res = (select top 1 case 
	when ChannelType1 = @ChannelType then ChannelType2 
	when ChannelType2 = @ChannelType then ChannelType1
	when ChannelType3 = @ChannelType then ChannelType4 
	when ChannelType4 = @ChannelType then ChannelType3
	end
	from 
	dbo.DictTariffs_Zones tz  join 
	(
		select Tariff_ID from dbo.DictTariffs_ToTI
		where ti_id = @ti_id and StartDateTime <= @DTEnd and (FinishDateTime is null OR @DTStart <= FinishDateTime)
	) tt
	on tz.Tariff_ID = tt.Tariff_ID and tz.StartDateTime <= @DTEnd and (tz.FinishDateTime is null OR @DTStart <= tz.FinishDateTime)
	where ChannelType1 = @ChannelType or ChannelType2 = @ChannelType or ChannelType3 = @ChannelType or ChannelType4 = @ChannelType)
end else begin
	if (@ChannelType<=4) begin
		if (@ChannelType = 1) return @AIATSCode;
		if (@ChannelType = 2) return @AOATSCode;
		if (@ChannelType = 3) return @RIATSCode;
		if (@ChannelType = 4) return @ROATSCode;
	end

	set @Res = (select top 1 case 
	when ChannelType1 = @ChannelType then ChannelType2 
	when ChannelType2 = @ChannelType then ChannelType1
	when ChannelType3 = @ChannelType then ChannelType4 
	when ChannelType4 = @ChannelType then ChannelType3
	end
	from 
	dbo.DictTariffs_Zones tz  join 
	(
		select Tariff_ID from dbo.DictTariffs_ToContrTI
		where ContrTI_ID = @ti_id and StartDateTime <= @DTEnd and (FinishDateTime is null OR @DTStart <= FinishDateTime)
	) tt
	on tz.Tariff_ID = tt.Tariff_ID and tz.StartDateTime <= @DTEnd and (tz.FinishDateTime is null OR @DTStart <= tz.FinishDateTime)
	where ChannelType1 = @ChannelType or ChannelType2 = @ChannelType or ChannelType3 = @ChannelType or ChannelType4 = @ChannelType)
end	

return ISNULL(@Res, @ChannelType);

end
go
   grant exec on usf2_ReverseTariffChannel to [UserCalcService]
   grant exec on usf2_ReverseTariffChannel to [UserSlave61968Service]
go