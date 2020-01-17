if exists (select 1
          from sysobjects
          where  id = object_id('usp2_GroupTP_SaveDeltaFromOpenDataTp')
          and type in ('P','PC'))
 drop procedure usp2_GroupTP_SaveDeltaFromOpenDataTp
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UpdateUsedForFillHalfHoursIntegralValues')
          and type in ('P','PC'))
 drop procedure usp2_UpdateUsedForFillHalfHoursIntegralValues
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'IntegralVirtualValuesTableType' AND ss.name = N'dbo')
DROP TYPE [dbo].[IntegralVirtualValuesTableType]
-- Пересоздаем заново
CREATE TYPE [dbo].[IntegralVirtualValuesTableType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
	[ClosedPeriod_ID] uniqueidentifier NULL,
	[DataSourceType] tinyint  NOT NULL,
	[IsUsedForFillHalfHours] bit NULL
	PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDateTime] ASC,
	[ChannelType] ASC,
	[DataSourceType] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::IntegralVirtualValuesTableType to [UserCalcService]
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'DeltaFromOpenDataTp' AND ss.name = N'dbo')
DROP TYPE [dbo].[DeltaFromOpenDataTp]
-- Пересоздаем заново
CREATE TYPE [dbo].[DeltaFromOpenDataTp] AS TABLE(
	[ID] [int] NOT NULL,
	[Delta] [float] NOT NULL
)
GO

grant EXECUTE on TYPE::DeltaFromOpenDataTp to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2014
--
-- Описание:
--
--		Обновляем поле IsUsedForFillHalfHours в таблице интегральных значений
--
-- ======================================================================================

create proc [dbo].[usp2_UpdateUsedForFillHalfHoursIntegralValues]
	@IntegralVirtualValuesTable IntegralVirtualValuesTableType READONLY --Таблицу которую пишем в базу данных
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

if (not exists(select top 1 1 from @IntegralVirtualValuesTable)) return;

select iv.*, ti.Titype,
dbo.usf2_ReverseTariffChannel(0, iv.[ChannelType], ti.AIATSCode,ti.AOATSCode,ti.RIATSCode,ti.ROATSCode, iv.TI_ID, iv.[EventDateTime], iv.[EventDateTime]) as ReversedChannel,
isnull(dsl.DataSource_ID, 0) as DataSource_ID
into #tis
from @IntegralVirtualValuesTable iv
join Info_TI ti on ti.TI_ID = iv.TI_ID
left join Expl_DataSource_List dsl on dsl.DataSourceType = iv.DataSourceType
order by ti.TIType, iv.TI_ID, iv.ChannelType, iv.DataSourceType

declare 
	@titype tinyint,
	@closedPeriod_id uniqueidentifier,
	@parmDefinition NVARCHAR(1000),
	@sqlString NVARCHAR(4000),
	@sqlTable NVARCHAR(200),
	@sqlAdditionalFilter1 NVARCHAR(200),
	@sqlAdditionalFilter2 NVARCHAR(200),
	@sqlTableNumber NVARCHAR(3);

	set @parmDefinition = '@titype tinyint, @closedPeriod_Id uniqueidentifier';
	set @sqlAdditionalFilter1 = ' and a.ClosedPeriod_Id = iv.ClosedPeriod_Id ';
	set @sqlAdditionalFilter2 = ' and iv.ClosedPeriod_Id = @closedPeriod_Id ';

--Теперь перебираем ПУ и возвращаем информацию по каждому
declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType, ClosedPeriod_ID from #tis
  open t;
	FETCH NEXT FROM t into @titype, @closedPeriod_id

	WHILE @@FETCH_STATUS = 0
	BEGIN

		if (@titype > 10) begin
			--Бытовая точка
			set @sqlTableNumber = ltrim(str(@titype - 10,2));
			if (@ClosedPeriod_ID is not null) begin
				set @SqlTable = 'ArchCalcBit_Integrals_Closed_' + @sqlTableNumber;
			end else begin 
				set @SqlTable = 'ArchCalcBit_Integrals_Virtual_' + @sqlTableNumber;
			end;
		end else begin
			if (@ClosedPeriod_ID is not null) begin 
				set @SqlTable = 'ArchCalc_Integrals_Closed';
			end else begin
				set @SqlTable = 'ArchCalc_Integrals_Virtual';
			end;
		end;
		set @SQLString = N'update '+@SqlTable+' 
		set [IsUsedForFillHalfHours] = iv.[IsUsedForFillHalfHours]
		from '+@SqlTable+' a
		join #tis iv on a.TI_ID = iv.TI_ID and a.EventDateTime = iv.EventDateTime 
		and a.ChannelType = ReversedChannel'
		+ case when @ClosedPeriod_ID is null then ' and a.DataSource_ID = iv.DataSource_ID ' else @sqlAdditionalFilter1 end +
		'where iv.titype = @titype' + case when @ClosedPeriod_ID is null then '' else @sqlAdditionalFilter2 end;

		--print @SQLString
		EXEC sp_executesql  @SQLString, @parmDefinition, @titype, @closedPeriod_id


	FETCH NEXT FROM t into @titype, @closedPeriod_id
	END;
	CLOSE t
	DEALLOCATE t

drop table #tis

end
go
   grant EXECUTE on usp2_UpdateUsedForFillHalfHoursIntegralValues to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2013
--
-- Описание:
--
--		Сохранение расхождения между открытыми данными и закрытыми по группе ТП
--
-- ======================================================================================
create proc [dbo].[usp2_GroupTP_SaveDeltaFromOpenDataTp]

	@TpValues dbo.DeltaFromOpenDataTp READONLY,
	@ClosedPeriod_ID uniqueidentifier,
	@EventDateTime datetime,
	@ToYear int,
	@ToMonth tinyint,
	@isCustomerCoordinated bit,
	@User_ID ABS_NUMBER_TYPE_2

as

begin

--Все действия необходимо делать в одной транзакции
	BEGIN TRY  BEGIN TRANSACTION

	--Помечаем все записи о том что они не активные 
		UPDATE [dbo].[Expl_ClosedPeriod_DeltaTpFromOpen]
		set IsActive = 0
		where ClosedPeriod_ID = @ClosedPeriod_ID and TP_ID in (select ID from @TpValues)

	--Добавление/обновление значений
		merge [dbo].[Expl_ClosedPeriod_DeltaTpFromOpen] as d
		using @TpValues tp
		on d.ClosedPeriod_ID = @ClosedPeriod_ID and d.TP_ID=tp.ID and d.ToYear = @ToYear and d.ToMonth = @ToMonth
		when matched then update set Delta = tp.Delta, DispatchDateTime = @EventDateTime, IsActive = 1, 
			[User_ID] = @User_ID, IsCustomerCoordinated = @isCustomerCoordinated
		when not matched then 
		insert
			   ([ClosedPeriod_ID]
			   ,[TP_ID]
			   ,[DispatchDateTime]
			   ,[ToYear]
			   ,[ToMonth]
			   ,[IsActive]
			   ,[Delta], [User_ID], [IsCustomerCoordinated])
		values (@ClosedPeriod_ID,ID,@EventDateTime, @ToYear, @ToMonth, 1, Delta, @User_ID, @isCustomerCoordinated);

	 --Снимаем признак с интегральных ТИ о том что они использовались в разнесении получасовок методом 6.7 и 8.2
	 --Все ТИ участвующие в формулах
		declare @startMonthYear datetime, @endMonthYear DateTime;
		--Дата, время закрытия
		set @startMonthYear = (select dateadd(mm,([Year]-1900)* 12 + [Month] - 1,0) from Expl_ClosedPeriod_List where ClosedPeriod_ID = @ClosedPeriod_ID)  
		set @endMonthYear = dateadd(month,1,@startMonthYear); --Дата, время окончания

		create table #periods
		(
		 EventDate DateTime
		)

		insert into #periods values (@startMonthYear);
		insert into #periods values (@endMonthYear);

		declare @tps IntType;
		insert into @tps 
		select Id from @TpValues;
		declare @IntegralVirtualValuesTable IntegralVirtualValuesTableType;

		
		insert into @IntegralVirtualValuesTable (TI_ID, ChannelType, DataSourceType, EventDateTime, IsUsedForFillHalfHours, ClosedPeriod_ID)
		select distinct ti_id, ChannelType, 0, EventDate, 0, ClosedPeriod_ID from usf2_Info_GetTisForTpByFormula(@tps, @ClosedPeriod_ID), #periods

		--select * from @IntegralVirtualValuesTable

		--Отмечаем в закрытом периоде интегралы о том чтобы их не использовать в распределении методом 6.7 или 6.8.2
		exec usp2_UpdateUsedForFillHalfHoursIntegralValues @IntegralVirtualValuesTable

	COMMIT END TRY
	BEGIN CATCH
		--Ошибка, откатываем все изменения
		IF @@TRANCOUNT > 0 ROLLBACK 

		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 -- На верху нужен exception
		--SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
end

go
   grant EXECUTE on usp2_GroupTP_SaveDeltaFromOpenDataTp to [UserCalcService]
go

