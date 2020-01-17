if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Arch_Integral_Select')
          and type in ('P','PC'))
   drop procedure usp2_Arch_Integral_Select
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2013
--
-- Описание:
--
--		Выбираем показания счетчиков
--
-- ======================================================================================

create proc [dbo].[usp2_Arch_Integral_Select]

	@TI_ID int, -- ТИ
	@DateStart datetime, --Время начала выборки
	@DateEnd datetime, -- Время конца выборки
	@ChannelType tinyint, --Канал
	@TypeTable tinyint, --Тип таблицы, где лежат данные
	@IsStartAndEndDateOnly bit = 0,
	@DataSource_ID int = null, -- Источник, если не указывается то в порядке приоритета
	@ClosedPeriod_ID uniqueidentifier = null, --Закрытый период, если не указан, читаем из таблицы расчетного профиля
	@IsAutoRead bit = null -- Читать авточтение
	

as
begin

	DECLARE 
		@ParmDefinition NVARCHAR(1000),
		@SQLString NVARCHAR(4000),
		@SqlTable NVARCHAR(200),
		@SqlPrefix NVARCHAR(200),
		@SqlRequstAdditionalField NVARCHAR(1000),
		@SqlAdditionalFieldVirtual NVARCHAR(1000),
		@SqlAdditionalFieldMain NVARCHAR(1000),
		@SqlAdditionalOrder NVARCHAR(1000),
		@SqlJoinToExpl_DataSource_PriorityList NVARCHAR(1000),
		@SqlWhereAdditional NVARCHAR(1000),
		@tableNumberString varchar(2);

set @SqlJoinToExpl_DataSource_PriorityList = '';
set @SqlWhereAdditional = '';
set @SqlRequstAdditionalField = '';

if (@IsAutoRead is null) set @IsAutoRead = 0;

--Формируем название таблицы из которой читаем данные ------------------------------------------
	if (@TypeTable > 10) begin --Бытовая точка
		if (@DataSource_ID = 0) begin -- Признак основного профиля
			set @SqlTable = 'ArchBit_30_Values_' + @tableNumberString;	
		end else begin
			--Выборка из таблицы закрытого периода, уточняем закрытый период
			if (@ClosedPeriod_ID is not null) begin
				set @SqlTable = @SqlTable + 'ArchCalcBit_Integrals_Closed_';
				set @SqlWhereAdditional = ' and t1.ClosedPeriod_ID = @ClosedPeriod_ID'
			end else begin
				set @SqlTable = 'ArchCalcBit_Integrals_Virtual_';
			end;

			set @SqlJoinToExpl_DataSource_PriorityList = ' left join Expl_DataSource_PriorityList expl on t1.DataSource_ID = expl.DataSource_ID and t1.EventDateTime between expl.StartDateTime and ISNULL(expl.FinishDateTime, ''21000101'')';
			set @SqlAdditionalOrder = ', Priority desc '
			set @SqlRequstAdditionalField =', Priority '

			--Указываем источник
			if (@DataSource_ID is not null) begin
				set @SqlWhereAdditional = @SqlWhereAdditional + ' and t1.DataSource_ID=@DataSource_ID'	
			end;
		end

		set @SqlTable = @SqlTable +  + ltrim(str(@TypeTable - 10,2))
	end else begin
		set @SqlTable = 'ArchComm_Integrals';	--Обычные старые таблицы, профиль только один
	end

SET @ParmDefinition = N'@TI_ID int,@DateStart DateTime,@DateEnd DateTime,@ChannelType tinyint,@IsAutoRead bit'

	if (@IsStartAndEndDateOnly=0) begin --Чтение всех данных за диапазон
		SET @SQLString = N'select t1.EventDateTime,t1.Data,t1.Status, t1.IntegralType' + @SqlRequstAdditionalField + 'from '+ @SqlTable + ' t1 '+
			+ @SqlJoinToExpl_DataSource_PriorityList +
			'where t1.TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDateTime between @DateStart and @DateEnd)
			and (@IsAutoRead = 0 OR IntegralType = 0)
			order by t1.EventDateTime' + @SqlAdditionalOrder 
	end else begin --Чтение только данных на начало и конец
		SET @SQLString = N'declare 
			@EventDateTimeStart DateTime,
			@EventDateTimeEnd DateTime
			select @EventDateTimeStart = Min(EventDateTime), @EventDateTimeEnd= Max(EventDateTime) from '+ @SqlTable + ' where TI_ID = @TI_ID and ChannelType=@ChannelType
					and (EventDateTime between @DateStart and @DateEnd)' + @SqlWhereAdditional + 
			' select t1.EventDateTime,t1.Data,t1.Status, t1.IntegralType from '+ @SqlTable + ' t1' +
			+ @SqlJoinToExpl_DataSource_PriorityList +
			' where t1.TI_ID = @TI_ID and ChannelType=@ChannelType
			and (EventDateTime=@EventDateTimeStart  or (EventDateTime between DateAdd(minute, -150, @EventDateTimeEnd) and @EventDateTimeEnd))
			and (@IsAutoRead = 0 OR IntegralType = 0)
			order by t1.EventDateTime' + @SqlAdditionalOrder 
	end
	
	--print @SQLString
	EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID, @DateStart,@DateEnd,@ChannelType,@IsAutoRead;
end

go
   grant EXECUTE on usp2_Arch_Integral_Select to [UserCalcService]
go