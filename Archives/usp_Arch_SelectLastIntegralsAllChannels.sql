if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Arch_SelectLastIntegralsAllChannels')
          and type in ('P','PC'))
   drop procedure usp2_Arch_SelectLastIntegralsAllChannels
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
--		Ноябрь, 2011
--
-- Описание:
--
--		Выбирает последние значения интегралов, по все каналам по одной точке
--
-- ======================================================================================

create proc [dbo].[usp2_Arch_SelectLastIntegralsAllChannels]

	@TI_ID int

as
begin
	declare 
	@titype tinyint,
	@ChannelType1 tinyint, @ChannelType2 tinyint, @ChannelType3 tinyint, @ChannelType4 tinyint,
	@AIATSCode int,@AOATSCode int,@RIATSCode int,@ROATSCode int, @AbsentChannelsMask tinyint;

	DECLARE @ParmDefinition NVARCHAR(1000);
	SET @ParmDefinition = N'@TI_ID int,@ChannelType1 tinyint, @ChannelType2 tinyint, @ChannelType3 tinyint, @ChannelType4 tinyint';
	DECLARE @SQLString NVARCHAR(4000);

	declare @tablePrefix NVARCHAR(300);
	declare @tableSufix NVARCHAR(300); 

	select @titype = TIType, @AIATSCode = AIATSCode, @AOATSCode = AOATSCode, @RIATSCode = RIATSCode, @ROATSCode = ROATSCode, @AbsentChannelsMask = ISNULL(AbsentChannelsMask, 0)
	from Info_TI where TI_ID = @ti_id;

	if (@titype < 11) BEGIN
		SET @tablePrefix =N'insert into #result
			select top 1 TI_ID,EventDateTime,ChannelType,Data   from dbo.ArchComm_Integrals
			where TI_ID = @ti_id'
	END ELSE BEGIN
		SET @tablePrefix =N'insert into #result
			select top 1 TI_ID,EventDateTime,ChannelType,Data   from dbo.ArchBit_Integrals_' + ltrim(str(@titype - 10,2)) + '
			where TI_ID = @ti_id'
	END;

	set @tableSufix = N'order by EventDateTime desc'

	create table #result
	(
		TI_ID int,
		EventDateTime DateTime,
		ChannelType tinyint,
		Data float
	);

	--Обычные каналы
	SET @SQLString = case when @AbsentChannelsMask & 1 = 1 then '' else @tablePrefix + N' and ChannelType = 1 ' + @tableSufix + '; ' end
		+ case when (@AbsentChannelsMask / 2) & 1 = 1 then '' else @tablePrefix + N' and ChannelType = 2 ' + @tableSufix + '; ' end
		+ case when (@AbsentChannelsMask / 4) & 1 = 1 then '' else @tablePrefix + N' and ChannelType = 3 ' + @tableSufix + '; ' end
		+ case when (@AbsentChannelsMask / 8) & 1 = 1 then '' else @tablePrefix + N' and ChannelType = 4 ' + @tableSufix + '; ' end
		
	EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID, @ChannelType1 , @ChannelType2 , @ChannelType3 , @ChannelType4 ;
	 
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct ChannelType1, ChannelType2, ChannelType3, ChannelType4 
	from dbo.DictTariffs_ToTI t
	join dbo.DictTariffs_Zones z on z.Tariff_ID = t.Tariff_ID
	where t.TI_ID = @TI_ID and t.StartDateTime = (select max(StartDateTime) from DictTariffs_ToTI where TI_ID = @TI_ID)
	  open t;
		FETCH NEXT FROM t into @ChannelType1, @ChannelType2, @ChannelType3, @ChannelType4
		WHILE @@FETCH_STATUS = 0
		BEGIN
		
		SET @SQLString = case when @AbsentChannelsMask & 1 = 1 then '' else @tablePrefix + N' and ChannelType = @ChannelType1 ' + @tableSufix + '; ' end
		+ case when (@AbsentChannelsMask / 2) & 1 = 1 then '' else @tablePrefix + N' and ChannelType = @ChannelType2 ' + @tableSufix + '; ' end
		+ case when (@AbsentChannelsMask / 4) & 1 = 1 then '' else @tablePrefix + N' and ChannelType = @ChannelType3 ' + @tableSufix + '; ' end
		+ case when (@AbsentChannelsMask / 8) & 1 = 1 then '' else @tablePrefix + N' and ChannelType = @ChannelType4 ' + @tableSufix + '; ' end
		
		EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID, @ChannelType1 , @ChannelType2 , @ChannelType3 , @ChannelType4 
		
		FETCH NEXT FROM t into @ChannelType1, @ChannelType2, @ChannelType3, @ChannelType4
		end;
		CLOSE t
		DEALLOCATE t
		

	select distinct EventDateTime, @TI_ID as TI_ID
	,dbo.usf2_ReverseTariffChannel(0, ChannelType, @AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode, @TI_ID, EventDateTime, EventDateTime) as ChannelType
	,Data from #result

	drop table #result;

end
go
   grant EXECUTE on usp2_Arch_SelectLastIntegralsAllChannels to [UserCalcService]
go
grant EXECUTE on usp2_Arch_SelectLastIntegralsAllChannels to [UserSlave61968Service]
go
