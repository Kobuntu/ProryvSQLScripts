if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchBit_SetIntegralValue')
          and type in ('P','PC'))
   drop procedure usp2_ArchBit_SetIntegralValue
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
--		Сохраняем 2 значения в виртуальном профиле бытовых барабанов
--
-- ======================================================================================

create proc [dbo].[usp2_ArchBit_SetIntegralValue]

	@TI_ID int,
	@dataSource_id int, --Источник для которого будем писать (по умолчанию МРСК)
	@EventDate1 datetime,
	@ChannelArray varchar(4000),
	@CUS_ID tinyint,
	@DispatchDateTime DateTime,
	@IsCoeff bit,
	@IsForcedInsertOrUpdate bit = 0, --Признак того что в любом случае обновляем данные 
								--(если время 00:00:00, то в основном профиле ставим признак недостоверности)
								--Если 0, то при наличии данных уточняем переписать или нет
	@UserName varchar(64), --Имя пользователя
	@IsSmartMetering bit = 0

as
begin

set nocount on

declare 
@TIType tinyint,
@Coeff int,
@AIATSCode tinyint,
@AOATSCode tinyint,
@RIATSCode tinyint,
@ROATSCode tinyint,
@IntegralType tinyint;
	
	select @TIType = TIType, @AIATSCode = AIATSCode, @AOATSCode = AOATSCode, @RIATSCode = RIATSCode, @ROATSCode= ROATSCode  from Info_TI where TI_ID=@TI_ID;

	if (@dataSource_id is null) set @dataSource_id = ISNULL((select DataSource_ID from Expl_DataSource_List where DataSourceType = 4), 0);

	--Проверяем пользователя
	declare @User_ID varchar(22);

	if (@IsSmartMetering is null or @IsSmartMetering = 0) begin
		set @User_ID = (select [User_ID] from dbo.Expl_Users where [UserName] = @UserName);
		if (@User_ID is null) BEGIN
			RAISERROR('Неверный пользователь!', 10, 1)
		END;
	end;


	
	--Проверяем какой статус авточтения ставить 
	if (@EventDate1 = FLOOR(cast(@EventDate1 as float))) set  @IntegralType = 0;
	else  set  @IntegralType = 1;
	
	--dbo.usf2_ReverseTariffChannel(0, @ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, @TI_ID, @EventDate1, @EventDate1)
		if (@IsCoeff = 1) BEGIN

			set @Coeff = ISNULL((select COEFU*COEFI from Info_Transformators it
			where it.TI_ID = @TI_ID and it.StartDateTime =
								(
								select max(Info_Transformators.StartDateTime)
								from Info_Transformators
								where Info_Transformators.TI_ID = @TI_ID
									and Info_Transformators.StartDateTime <= @EventDate1 
									and ISNULL(Info_Transformators.FinishDateTime, '21000101') >= @EventDate1
								)),1);
		END else set @Coeff = 1;

		if (@TIType <= 10) begin
			DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
			set @ErrMsg = 'Указанная точка не является бытовой!'
			set @ErrSeverity = 10 
			SELECT @ErrMsg 
			RAISERROR(@ErrMsg, @ErrSeverity, 1)
		end;
		
		DECLARE @ParmDefinition NVARCHAR(1500);
		SET @ParmDefinition = N'@TI_ID int,@ca varchar(4000),@EventDate1 DateTime,@Coeff int, @AIATSCode tinyint, @AOATSCode tinyint, @RIATSCode tinyint, @ROATSCode tinyint,@DispatchDateTime DateTime,@CUS_ID tinyint,@IntegralType tinyint,@IsForcedInsertOrUpdate bit,@User_ID varchar(22),@dataSource_id int'
		DECLARE @SQLString NVARCHAR(4000);
		SET @SQLString = N''

	create table #ChannelValues
	(
		ChannelType tinyint,
		Data float
	);
		
	BEGIN TRY 

	DECLARE 
			@Counter int,
			@Slice varchar(200),
			@channelType tinyint,
			@data float,
			@s varchar(1000),
			@c tinyint;
		SET @Counter = 1 
		SET @s = ''

		--Наполнение таблицы с каналом и значением
		WHILE @Counter<>0 	BEGIN 
			set @Counter = CHARINDEX(';',@ChannelArray) 
			IF @Counter<>0 set @Slice = LEFT(@ChannelArray,@Counter - 1)
			ELSE set @Slice = @ChannelArray
			set @channelType = dbo.usf2_ReverseTariffChannel(0,	cast(LEFT(@Slice,CHARINDEX('|',@Slice) - 1) as tinyint), @AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode, @TI_ID, @EventDate1, @EventDate1);
			set @c = @channelType % 10;
			set @s = @s+ISNULL((select top 1 StringName from DictTariffs_ToTI tt join DictTariffs_Zones tz on tz.Tariff_ID = tt.Tariff_ID and @EventDate1 between tz.StartDateTime and tz.FinishDateTime and (ChannelType1 = @channelType or ChannelType2 = @channelType or ChannelType3 = @channelType or ChannelType4 = @channelType)
					where tt.TI_ID = 1066 and @EventDate1 between tt.StartDateTime and tt.FinishDateTime),'') + ' ';
			set @data = convert(float,Replace(RIGHT(@Slice,LEN(@Slice) - CHARINDEX('|',@Slice)), ',', '.'), 120) / @Coeff;
			if (@c = 1) set @s=@s+'АП' else if (@c = 2) set @s=@s+'АО' else if (@c = 3) set @s=@s+'РП' else if (@c = 4) set @s=@s+'РО'
			set @s=@s+' '+str(@data,8,2)+', '
			
			insert into #ChannelValues (ChannelType, Data) values (@channelType, @data);

			SET @ChannelArray = RIGHT(@ChannelArray,LEN(@ChannelArray) - @Counter) 
			IF LEN(@ChannelArray) = 0 BREAK; 
		END


		--Сначала пишем в журнал сохранения интегралов абонентов
		if (@IsSmartMetering = 1) begin
			declare @BitAbonent_ID int, @BitAbonentSurname nvarchar(255);
			select top 1 @BitAbonent_ID = BitAbonent_ID, @BitAbonentSurname = BitAbonentSurname
			from [dbo].[InfoBit_Abonents_List] 
			where BitAbonent_ID = (select top 1 BitAbonent_ID from InfoBit_Abonents_To_TI where TI_ID = @TI_ID and @EventDate1 between StartDateTime and FinishDateTime)
			if (@BitAbonent_ID is null) RAISERROR('У абонента не задан счетчик!', 10, 1)
			--Проверка на возможность записи 3 раза
			if ((select Count(Data) from ArchBit_Abonents_ManualEnter where BitAbonent_ID = @BitAbonent_ID and floor(CAST(DispatchDateTime as float)) = floor(CAST(@DispatchDateTime as float)) and ChannelType = 1) >=3) begin
				RAISERROR('Нельзя более трех раз сохранять показания в течении одних суток!', 15, 1)
			end

			Merge ArchBit_Abonents_ManualEnter as a
			using (select @BitAbonent_ID as BitAbonent_ID, ChannelType, Data, @DispatchDateTime as DispatchDateTime from #ChannelValues) cv 
			on a.BitAbonent_ID = cv.BitAbonent_ID and a.ChannelType = cv.ChannelType and a.DispatchDateTime = cv.DispatchDateTime
			WHEN MATCHED THEN 
			UPDATE set Data = cv.Data
			WHEN NOT MATCHED THEN 
			INSERT (BitAbonent_ID, ChannelType, Data, DispatchDateTime) values (cv.BitAbonent_ID, cv.ChannelType, cv.Data, cv.DispatchDateTime);
			
		end 

		set @SQLString = N'Merge ArchCalcBit_Integrals_Virtual_' + ltrim(str(@TIType - 10,2)) + ' as a
		USING (select @TI_ID as TI_ID,@EventDate1 as EventDate1, @dataSource_id as dataSource_id, ChannelType, Data from #ChannelValues) as ca 
		ON a.TI_ID = ca.TI_ID and a.ChannelType = ca.ChannelType and a.EventDateTime = ca.EventDate1 and a.DataSource_ID = ca.DataSource_ID
		WHEN MATCHED THEN 
		UPDATE set ManualEnterData = ca.Data * 1000, IntegralType = @IntegralType, DispatchDateTime = @DispatchDateTime
				,ContrReplaceStatus = 0,ManualEnterStatus = 1, CUS_ID = @CUS_ID, [Status] = 0
		WHEN NOT MATCHED THEN 
		INSERT (TI_ID, EventDateTime, ChannelType,DataSource_ID, Data, ManualEnterData, IntegralType, DispatchDateTime, ContrReplaceStatus, ManualEnterStatus, CUS_ID, Status)
		values(ca.TI_ID,ca.EventDate1, ca.ChannelType, ca.dataSource_id,-1, ca.Data * 1000, @IntegralType, @DispatchDateTime, 0, 1, @CUS_ID, 0);'
		
	
		EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID ,@ChannelArray,@EventDate1,@Coeff, @AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode,@DispatchDateTime,@CUS_ID, @IntegralType,@IsForcedInsertOrUpdate,@User_ID,@dataSource_id;

		if (@User_ID is not null) begin	set @s=@s+'(кВт*ч)';
			if not exists (select TI_ID,ChannelType,EventDate,[User_ID],EventDateTime from dbo.Expl_User_Journal_Replace_30_Virtual vv 
				where vv.ti_id = @ti_id and vv.ChannelType = @C and vv.EventDate = convert(smalldatetime, @EventDate1) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime) begin 
				insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, CommentString, CUS_ID, ZamerDateTime)
				select @ti_ID, @ChannelType,convert(smalldatetime, @EventDate1) as EventDate, @User_ID, @DispatchDateTime, 11, @s, @CUS_ID, convert(smalldatetime, @EventDate1) as ZamerDateTime
			end else begin 
				update dbo.Expl_User_Journal_Replace_30_Virtual
				set  ti_ID = @ti_ID, ChannelType = @ChannelType,EventDate = convert(smalldatetime, @EventDate1), [User_ID] = @User_ID, EventDateTime =@DispatchDateTime, EventParam = 11, CommentString = @s, CUS_ID = @CUS_ID, ZamerDateTime = @EventDate1 
				where Expl_User_Journal_Replace_30_Virtual.ti_id = @ti_id and Expl_User_Journal_Replace_30_Virtual.ChannelType = @C and Expl_User_Journal_Replace_30_Virtual.EventDate = convert(smalldatetime, @EventDate1)
					and Expl_User_Journal_Replace_30_Virtual.[User_ID] = @User_ID and Expl_User_Journal_Replace_30_Virtual.EventDateTime = @DispatchDateTime 
			end 
		end

		select '' as [Message], cast(0 as bit) as IsNeedRequest;

		
	END TRY
	BEGIN CATCH
     DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT;
        
      SELECT 
		@ErrorMessage = ERROR_MESSAGE(),
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()
        
        --Если ошибка на вставку данных 
        if(@ErrorNumber=2627) BEGIN
			--Просто выдаем предупреждение о том что данные есть и необходимо из переписать
			set @ErrorMessage = 'Уже есть показания на данное время, хотите';
			
			if (@IntegralType = 0) BEGIN
				set @ErrorMessage = @ErrorMessage + ' установить признак недостоверности в основной профиль и';
			END
			
			set @ErrorMessage = @ErrorMessage + ' ввести новые данные в расчетный профиль?';
			
			select @ErrorMessage as [Message], cast(1 as bit) as IsNeedRequest;
        END ELSE BEGIN
			--Ошибка SQL
			RAISERROR 
					(
						@ErrorMessage, 
						@ErrorSeverity, 
						1,               
						@ErrorNumber,    -- parameter: original error number.
						@ErrorSeverity,  -- parameter: original error severity.
						@ErrorState     -- parameter: original error state.
					);
		END
	        
     END CATCH
end

go
   grant EXECUTE on usp2_ArchBit_SetIntegralValue to [UserCalcService]
go