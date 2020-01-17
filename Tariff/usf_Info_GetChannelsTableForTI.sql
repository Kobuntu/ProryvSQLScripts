if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetChannelsTableForTI')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetChannelsTableForTI
go
/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/18/2008 11:53:14 ******/
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
--		Апрель 2011
--
-- Описание:
--
--		Выбираем тарифные каналы переворачиваем если нужно группируем по типу канала
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_Info_GetChannelsTableForTI]
(
	@TI_ID int, 
	@AbsentChannelsMask tinyint,
	@IsChannelReverse bit
)
returns @tbl TABLE 
(
	ChannelType tinyint, --Канал не перевернутый
	channel tinyint, -- Канал уже перевернутый (для запроса в БД)
	groupingCh tinyint, -- Принадлежность основному каналу (% 1-)
	isMainChannel bit  --Признак основного суммарного канала
)
as
BEGIN
declare
@IsAPAbsent bit,
@IsAOAbsent bit,
@IsRPAbsent bit,
@IsROAbsent bit,
@Tariff_ID int;


set @Tariff_ID = (select top 1 Tariff_ID from dbo.DictTariffs_ToTI d 
			where TI_ID = @TI_ID and d.StartDateTime = (select MAX(StartDateTime) 
			from dbo.DictTariffs_ToTI 
			where TI_ID = @TI_ID));

IF (@IsChannelReverse=1) begin
	if ((@AbsentChannelsMask / 2) & 1)=0 begin
		insert into @tbl 
		select 1, 2, 1, 1
		union all
		select z.ChannelType1, z.ChannelType2, 1, 0 
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
	if (@AbsentChannelsMask & 1)=0 begin 
		insert into @tbl 
		select 2, 1, 2, 1
		union all
		select z.ChannelType2, z.ChannelType1, 2, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
	if ((@AbsentChannelsMask / 8) & 1)=0 begin
		insert into @tbl 
		select 3, 4, 3, 1
		union all
		select z.ChannelType3, z.ChannelType4, 3, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end
	if ((@AbsentChannelsMask / 4) & 1)=0 begin
		insert into @tbl 
		select 4, 3, 4, 1
		union all
		select z.ChannelType4, z.ChannelType3, 4, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
	
end else begin
	if (@AbsentChannelsMask & 1)=0 begin 
		insert into @tbl 
		select 1, 1, 1, 1
		union all
		select z.ChannelType1, z.ChannelType1, 1, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
	if ((@AbsentChannelsMask / 2) & 1) = 0 begin
		insert into @tbl 
		select 2, 2, 2, 1
		union all
		select z.ChannelType2, z.ChannelType2, 2, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
	if ((@AbsentChannelsMask / 4) & 1)=0 begin
		insert into @tbl 
		select 3, 3, 3, 1
		union all
		select z.ChannelType3, z.ChannelType3, 3, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
	if ((@AbsentChannelsMask / 8) & 1)=0 begin
		insert into @tbl 
		select 4, 4, 4, 1
		union all
		select z.ChannelType4, z.ChannelType4, 4, 0  
		from dbo.DictTariffs_Zones z 
		where z.Tariff_ID = @Tariff_ID
	end;
end;

RETURN
END
go
   grant select on usf2_Info_GetChannelsTableForTI to [UserCalcService]
go