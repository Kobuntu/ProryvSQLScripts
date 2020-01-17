if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchBit_LastIntegral')
          and type in ('P','PC'))
   drop procedure usp2_ArchBit_LastIntegral
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
--		Февраль, 2011
--
-- Описание:
--
--		Выбираем последнее значение бытового барабанов
--
-- ======================================================================================

create proc [dbo].[usp2_ArchBit_LastIntegral]

	@TI_ID int,
	@EventDate1 datetime,
	@ChannelType tinyint,
	@DaysLimit tinyint = 100, -- Количество дней до которых ограничиваем поиск
	@TP_ID int = null, -- Для определения приоритета
	@ClosedPeriod_ID uniqueidentifier = null, -- Идентификатор закрытого периода
	@DataSourceType tinyint = null, -- Идентификатр источника, если он нужен
	@DataOut float output,
	@EventDateTimeOut dateTime output,
	@ManualEnterDataOut float output,
	@CoeffOut int output, 
	@DataSourceTypeOut tinyint output,
	@StatusOut int output,
	@IsSearchBack bit = 1

as
begin

select @DataOut = null, @EventDateTimeOut = null,@ManualEnterDataOut = null,@CoeffOut = null, @DataSourceTypeOut = null;

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@DirectChannelType tinyint,
@TIType tinyint,
@Coeff int,
@dataSource_ID int,
@sqlTableNumber NVARCHAR(3),
@SqlTable NVARCHAR(200),
@SqlRequest NVARCHAR(4000),
@SqlPriorityTable NVARCHAR(200),
@ParmDefinition NVARCHAR(4000),
@EventDateRanged DateTime;

--Ищем назад, до нужной даты на @DaysLimit дней
if (@IsSearchBack = 1) set @EventDateRanged = DATEADD(day, -@DaysLimit, @EventDate1);
else begin 
--Ищем вперед от указанной даты на @DaysLimit дней
	set @EventDateRanged = @EventDate1;
	set @EventDate1 = DATEADD(day, @DaysLimit, @EventDateRanged);
end


set @CoeffOut = (select top (1) COEFU*COEFI from Info_Transformators it
		where it.TI_ID = @TI_ID and StartDateTime <= @EventDate1 
		and ISNULL(FinishDateTime, '21000101') >= @EventDateRanged order by StartDateTime)

set @DirectChannelType = @ChannelType

select @TIType = TIType, @ChannelType = dbo.usf2_ReverseTariffChannel(0, @ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, @TI_ID, @EventDateRanged, @EventDate1)
from Info_TI where TI_ID=@TI_ID;

if (@dataSourceType is not null) set @dataSource_id = (select top 1 dataSource_id from Expl_DataSource_List where DataSourceType = @dataSourceType);

set @SqlPriorityTable = 'Expl_DataSource_PriorityList';

--Таблица из которой читаем
			if (@titype > 10) begin
				--Бытовая точка
				set @sqlTableNumber = ltrim(str(@titype - 10,2));
				if (@ClosedPeriod_ID is not null) begin
					set @SqlTable = 'ArchCalcBit_Integrals_Closed_' + @sqlTableNumber + ' a ';
					set @SqlPriorityTable += '_Closed'
				end else begin 
					set @SqlTable = 'ArchCalcBit_Integrals_Virtual_' + @sqlTableNumber + ' a ';
				end
			end else begin
				if (@ClosedPeriod_ID is not null) begin 
					set @SqlTable = 'ArchCalc_Integrals_Closed' + ' a ';
					set @SqlPriorityTable += '_Closed'
				end else begin
					set @SqlTable = 'ArchCalc_Integrals_Virtual' + ' a ';
				end
			end;



set @SqlRequest = N'select top (1) @outEventDateTime=EventDateTime,@outData=Data, @outManualEnterData=ManualEnterData, @outDataSourceType = (select top 1 DataSourceType from Expl_DataSource_List where DataSource_ID = a.DataSource_ID)'

--if (@dataSource_id is null) set @SqlRequest += N' ,@outPriority=pl.Priority'

set @SqlRequest += N', @outStatus = a.Status from ( select top (1) a.* from ' + @SqlTable 
set @SqlRequest += N' where TI_ID = @TI_ID and EventDateTime between @EventDateRanged and @EventDate1 and ChannelType = @ChannelType and (Data >= 0 OR ManualEnterData >= 0)'
--Если задано закрытие
if (@ClosedPeriod_ID is not null)
	set @SqlRequest += N' and ClosedPeriod_ID = @ClosedPeriod_ID';

if (@dataSource_id is not null)
	set @SqlRequest += N' and dataSource_id = @dataSource_id';

--Задана ТП
if (@TP_ID is not null and @dataSource_id is null)
	set @SqlRequest += N' and (a.DataSource_ID = ISNULL((select DataSource_ID from Expl_DataSource_To_TI_TP dttp 
						where dttp.TI_ID = @TI_ID and dttp.TP_ID = @TP_ID and dttp.Month = Month(a.EventDateTime)), a.DataSource_ID))';

set @SqlRequest += N' order by EventDateTime desc) a '


if (@dataSource_id is null)
	set @SqlRequest += N' left join ' + @SqlPriorityTable + ' pl on pl.dataSource_id = a.dataSource_id ';

if (@dataSource_id is null)  set @SqlRequest += N' order by priority desc'

SET @ParmDefinition = N'@TI_ID int,@EventDate1 DateTime,@ChannelType tinyint, @DaysLimit tinyint,@TP_ID int,@ClosedPeriod_ID uniqueidentifier, @dataSource_id int, @DirectChannelType tinyint, @EventDateRanged DateTime,
@outEventDateTime DateTime output, @outData float output, @outManualEnterData float output, @outDataSourceType tinyint output, @outStatus int output'
EXEC sp_executesql @SqlRequest, @ParmDefinition,@TI_ID, @EventDate1,@ChannelType,@DaysLimit, @TP_ID,@ClosedPeriod_ID, @dataSource_id, @DirectChannelType, @EventDateRanged,
@outEventDateTime = @EventDateTimeOut output, @outData = @DataOut output, @outManualEnterData = @ManualEnterDataOut output, @outDataSourceType = @DataSourceTypeOut output, @outStatus = @StatusOut output;

end

go
   grant EXECUTE on usp2_ArchBit_LastIntegral to [UserCalcService]
go