if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTariffChannelsForTIUseChannelReverse')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTariffChannelsForTIUseChannelReverse
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
--		Август, 2015
--
-- Описание:
--
--		Возвращаем список привязаных тарифных каналов для ТИ в виде строки, с учетом переворота канала
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetTariffChannelsForTIUseChannelReverse]
(	
	@TI_ID int, 
	@AbsentChannelsMask tinyint,
	@IsChannelReverse bit --Переворачивать или нет
)
RETURNS varchar(1000)
AS
begin
declare
@Res varchar(1000),
@IsAPAbsent bit,
@IsAOAbsent bit,
@IsRPAbsent bit,
@IsROAbsent bit;

set @AbsentChannelsMask = ISNULL(@AbsentChannelsMask, 0);

set @IsAPAbsent = @AbsentChannelsMask & 1;
set @IsAOAbsent = (@AbsentChannelsMask / 2) & 1;
set @IsRPAbsent = (@AbsentChannelsMask / 4) & 1;
set @IsROAbsent = (@AbsentChannelsMask / 8) & 1;

if (@IsChannelReverse = 1) begin 
	set @Res = case when @IsAPAbsent=0 then '2,' else '' end
	+case when @IsAOAbsent=0 then '1,' else '' end
	+case when @IsRPAbsent=0 then '4,' else '' end
	+case when @IsROAbsent=0 then '3,' else '' end;
end else begin
	set @Res = case when @IsAPAbsent=0 then '1,' else '' end
	+case when @IsAOAbsent=0 then '2,' else '' end
	+case when @IsRPAbsent=0 then '3,' else '' end
	+case when @IsROAbsent=0 then '4,' else '' end;
end;

if (@IsAPAbsent=0 OR @IsAOAbsent=0 OR @IsRPAbsent=0 OR @IsROAbsent=0) begin

	declare @dt datetime;
	set @dt = GetDate();

	if (@IsChannelReverse = 1) begin 
		select @Res = @Res 
		+ case when @IsAPAbsent=1 or z.ChannelType2<10 then '' else cast(z.ChannelType2 as varchar) + ',' end
		+ case when @IsAOAbsent=1 or z.ChannelType1<10 then '' else cast(z.ChannelType1 as varchar) + ',' end 
		+ case when @IsRPAbsent=1 or z.ChannelType4<10 then '' else cast(z.ChannelType4 as varchar) + ',' end
		+ case when @IsROAbsent=1 or z.ChannelType3<10 then '' else cast(z.ChannelType3 as varchar) + ',' end
		from dbo.DictTariffs_ToTI d 
		join dbo.DictTariffs_Zones z on d.Tariff_ID = z.Tariff_ID
		where d.TI_ID = @TI_ID and @dt >= d.StartDateTime and (d.FinishDateTime is null or @dt <= d.FinishDateTime)
	end else begin
		select @Res = @Res 
		+ case when @IsAPAbsent=1 or z.ChannelType1<10 then '' else cast(z.ChannelType1 as varchar) + ',' end
		+ case when @IsAOAbsent=1 or z.ChannelType2<10 then '' else cast(z.ChannelType2 as varchar) + ',' end 
		+ case when @IsRPAbsent=1 or z.ChannelType3<10 then '' else cast(z.ChannelType3 as varchar) + ',' end
		+ case when @IsROAbsent=1 or z.ChannelType4<10 then '' else cast(z.ChannelType4 as varchar) + ',' end
		from dbo.DictTariffs_ToTI d 
		join dbo.DictTariffs_Zones z on d.Tariff_ID = z.Tariff_ID
		where d.TI_ID = @TI_ID and @dt >= d.StartDateTime and (d.FinishDateTime is null or @dt <= d.FinishDateTime)
	end;
end;
					
if (LEN(@Res) > 0) return substring(@Res, 0, LEN(@Res));						
	
return @Res

end
go
   grant exec on usf2_Info_GetTariffChannelsForTIUseChannelReverse to [UserCalcService]
go