if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchCalcChannelInversionStatus')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchCalcChannelInversionStatus
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
-- Возвращаем списки каналов с учетом переворотов для ТИ
-- ======================================================================================
create FUNCTION [dbo].[usf2_ArchCalcChannelInversionStatus] (
			@TI_ID int,
			@ChannelType tinyint,
			--@EventDate smallDatetime,
			@DateStart datetime,
			@DateEnd datetime, --Идентификатор аварии
			@IsChannelsInverted bit
)	
	RETURNS TABLE 
	WITH SCHEMABINDING
AS
RETURN
	select top 100
		case ISNULL(i.IsInverted, @IsChannelsInverted) 
			when 0 then @ChannelType
			when 1 then 
			case @ChannelType
				when 1 then 2
				when 2 then 1
				when 3 then 4
				when 4 then 3
				else case @ChannelType % 10
					when 1 then (select top 1 ChannelType2 
								from dbo.DictTariffs_ToTI tti 
								join dbo.DictTariffs_Zones tz on tz.Tariff_ID = tti.Tariff_ID and tz.StartDateTime <= @DateEnd and (tz.FinishDateTime is null OR @DateStart <= tz.FinishDateTime)
								where tti.TI_ID = @TI_ID and tti.StartDateTime <= @DateEnd and (tti.FinishDateTime is null OR @DateStart <= tti.FinishDateTime)
								and ChannelType1=@ChannelType
								)
					when 2 then (select top 1 ChannelType1 
								from dbo.DictTariffs_ToTI tti 
								join dbo.DictTariffs_Zones tz on tz.Tariff_ID = tti.Tariff_ID and tz.StartDateTime <= @DateEnd and (tz.FinishDateTime is null OR @DateStart <= tz.FinishDateTime)
								where tti.TI_ID = @TI_ID and tti.StartDateTime <= @DateEnd and (tti.FinishDateTime is null OR @DateStart <= tti.FinishDateTime)
								and ChannelType2=@ChannelType)
					when 3 then (select top 1 ChannelType4 
								from dbo.DictTariffs_ToTI tti 
								join dbo.DictTariffs_Zones tz on tz.Tariff_ID = tti.Tariff_ID and tz.StartDateTime <= @DateEnd and (tz.FinishDateTime is null OR @DateStart <= tz.FinishDateTime)
								where tti.TI_ID = @TI_ID and tti.StartDateTime <= @DateEnd and (tti.FinishDateTime is null OR @DateStart <= tti.FinishDateTime)
								and ChannelType3=@ChannelType)
					when 4 then (select top 1 ChannelType3 
								from dbo.DictTariffs_ToTI tti 
								join dbo.DictTariffs_Zones tz on tz.Tariff_ID = tti.Tariff_ID and tz.StartDateTime <= @DateEnd and (tz.FinishDateTime is null OR @DateStart <= tz.FinishDateTime)
								where tti.TI_ID = @TI_ID and tti.StartDateTime <= @DateEnd and (tti.FinishDateTime is null OR @DateStart <= tti.FinishDateTime)
								and ChannelType4=@ChannelType)
					end
			end
		end as ChannelType, i.StartDateTime as StartChannelStatus, 
		i.FinishDateTime as FinishChannelStatus
	from 
	(
		select c.TI_ID, c.IsInverted, StartDateTime, c.FinishDateTime 
		from --Записи, которые не инвертируются
		(
			--Определяем первый диапазон записи в архиве, который не ивертируется
			select TI_ID, IsInverted, StartDateTime, FinishDateTime from 
			(
				select @TI_ID as TI_ID, @IsChannelsInverted as IsInverted, @DateStart as StartDateTime, 
				ISNULL(DATEADD(minute, -30, (select top 1 StartDateTime from [dbo].[ArchCalc_Channel_InversionStatus] a 
					where a.TI_ID =@TI_ID and a.StartDateTime <= @DateEnd 
					and (a.FinishDateTime is null or a.FinishDateTime >= @DateStart) and IsInverted <> @IsChannelsInverted)),@DateEnd) as FinishDateTime
			) i where StartDateTime <= @DateEnd and (FinishDateTime is null or FinishDateTime >= @DateStart)
			union all
			--Остальные диапазоны записи в архиве, которые не ивертируется
			select a.TI_ID, a.IsInverted, 

				DATEADD(minute, 30, a.FinishDateTime) as StartDateTime,
			
				case when b.StartDateTime is null or b.StartDateTime > @DateEnd 
					then @DateEnd
					else DATEADD(minute, -30, b.StartDateTime)
				end as FinishDateTime
			from 
			(
				select TI_ID, @IsChannelsInverted as IsInverted, StartDateTime, FinishDateTime, ROW_NUMBER() over (order by TI_ID, StartDateTime) as n
				from [dbo].[ArchCalc_Channel_InversionStatus] a 
				where a.TI_ID =@TI_ID and a.StartDateTime <= @DateEnd 
					and (a.FinishDateTime is null or a.FinishDateTime >= @DateStart) and IsInverted <> @IsChannelsInverted
			) a 
			left join 
			(
				select TI_ID, @IsChannelsInverted as IsInverted, StartDateTime, FinishDateTime, ROW_NUMBER() over (order by TI_ID, StartDateTime) as n
				from [dbo].[ArchCalc_Channel_InversionStatus] a 
				where a.TI_ID =@TI_ID and a.StartDateTime <= @DateEnd 
					and (a.FinishDateTime is null or a.FinishDateTime >= @DateStart) and IsInverted <> @IsChannelsInverted
			) b on a.n = b.n - 1
		) c where c.StartDateTime <= @DateEnd and (c.FinishDateTime is null or c.FinishDateTime >= @DateStart)
		union all
		--Объединыем с записями, которые инвертируются
		select TI_ID, IsInverted,
		case when StartDateTime < @DateStart
			then @DateStart
			else StartDateTime
		end as StartDateTime, 

		case when FinishDateTime > @DateEnd
			then @DateEnd
			else FinishDateTime
		end as FinishDateTime 
		from [dbo].[ArchCalc_Channel_InversionStatus] i where i.TI_ID =@TI_ID and i.StartDateTime <= @DateEnd and (i.FinishDateTime is null or i.FinishDateTime >= @DateStart)
		and IsInverted <> @IsChannelsInverted
	) i

	order by StartDateTime
go
grant select on usf2_ArchCalcChannelInversionStatus to [UserCalcService]
go
grant select on usf2_ArchCalcChannelInversionStatus to [UserDeclarator]
go
