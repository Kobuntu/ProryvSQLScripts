if exists (select 1
					 from sysobjects
					 where  id = object_id('usp2_Arch_30_DispatchDateTime')
									and type in ('P','PC'))
	drop procedure usp2_Arch_30_DispatchDateTime
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август, 2016
--
-- Описание:
--
--		Читаем дата/время последнего сбора и ручного изменения получасовки
--
-- ======================================================================================
create proc [dbo].[usp2_Arch_30_DispatchDateTime]
	(
		@TI_ID int,
		@Date DateTime,
		@ChannelType tinyint,
		@DataSourceType tinyint
	)
AS
	BEGIN
		set nocount on
		set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
		set numeric_roundabort off
		set transaction isolation level read uncommitted

		DECLARE
		@TypeTable tinyint,
		@ParmDefinition NVARCHAR(1000),
		@SqlTable NVARCHAR(200),
		@sqlTableNumber NVARCHAR(3),
		@SQLString NVARCHAR(2000);

		SET @ParmDefinition = N'@TI_ID int, @Date DateTime, @ChannelType tinyint, @DataSourceType tinyint'

		set @Date = floor(cast(@Date as float)) --Округляем дату/время до суток
		set @TypeTable = (select titype from Info_TI where TI_ID = @TI_ID)

		if (@TypeTable > 10) set @sqlTableNumber = ltrim(str(@TypeTable - 10,2));

		if (@TypeTable = 2) begin
			--Малые ТИ читаем только из старой таблицы
			set @SqlTable = 'ArchCalc_30_Month_Values';
		end else if (@TypeTable > 10 ) begin
			set @SqlTable = 'ArchCalcBit_30_Virtual_' + @sqlTableNumber;
		end else begin
			set @SqlTable = 'ArchCalc_30_Virtual';
		end;

		select @channelType = dbo.usf2_ReverseTariffChannel(0,	@channelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, @TI_ID, @Date, @Date)
		from Info_TI where TI_ID = @TI_ID


		set @SQLString = 'select top 1 [DispatchDateTime], ManualEnterDateTime from ' + @SqlTable+ '
	where TI_ID = @TI_ID and [ChannelType] = @ChannelType and [EventDate] = @Date '

		if (@TypeTable<>2) begin
			set @SQLString = @SQLString + 'and [DataSource_ID] = (select DataSource_ID from [dbo].[Expl_DataSource_List] where [DataSourceType] = @DataSourceType)'
		end 


		--print @SQLString
		EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Date, @ChannelType, @DataSourceType

	END
go
grant EXECUTE on usp2_Arch_30_DispatchDateTime to [UserCalcService]
go