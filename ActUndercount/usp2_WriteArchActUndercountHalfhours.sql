if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteArchActUndercountHalfhours')
          and type in ('P','PC'))
   drop procedure usp2_WriteArchActUndercountHalfhours
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ActUndercountHalfhoursType' AND ss.name = N'dbo')
DROP TYPE [dbo].[ActUndercountHalfhoursType]

-- Пересоздаем заново
CREATE TYPE [dbo].[ActUndercountHalfhoursType] AS TABLE 
(
	[ActUndercount_UN] [uniqueidentifier] NOT NULL,
	[HalfhourDateTime] [datetime] NOT NULL,
	[AddedValue] [float] NOT NULL,
	PRIMARY KEY CLUSTERED 
	(
		[ActUndercount_UN] ASC,
		[HalfhourDateTime] ASC
	)WITH (IGNORE_DUP_KEY = OFF)
)
go

grant EXECUTE on TYPE::ActUndercountHalfhoursType to [UserCalcService]
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
--		Октябрь, 2017
--
-- Описание:
--
--		Пишем таблицу 30 минуток по акту недоучета
--
-- ======================================================================================
create proc [dbo].[usp2_WriteArchActUndercountHalfhours]
	@TI_ID [dbo].[TI_ID_TYPE],
	@ChannelType [dbo].[TI_CHANNEL_TYPE],
	@StartDateTime [datetime],
	@FinishDateTime [datetime],
	@AddedValue [float],
	@CommentString [varchar](max),
	@User_ID [dbo].[ABS_NUMBER_TYPE_2],
	@IsInactive [bit],
	@CUS_ID [dbo].[CUS_ID_TYPE],
	@ActUndercount_UN [uniqueidentifier],
	@ActMode [tinyint],
	@IsFinishDateTimeInclusive [bit],
	@Arch30ValuesTable ActUndercountHalfhoursType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

BEGIN TRY BEGIN TRANSACTION

	if exists(select top 1 1 from ArchCalc_Replace_ActUndercount 
		where TI_ID =  @TI_ID and ChannelType = @ChannelType and StartDateTime <= @StartDateTime and FinishDateTime >= @FinishDateTime
			and ActUndercount_UN <> @ActUndercount_UN and ISNULL(IsInactive,0)=0 and ActMode = 0) begin
		RAISERROR('Обнаружен другой действующий акт для данной ТИ с несовместимым режимом! Не допускается совмещать акт <Замещение> c актами с другими режимами в одни и те же получасовки.',17,1);
	end

	declare @isExists bit;
	
	if exists(select top 1 1 from ArchCalc_Replace_ActUndercount where ActUndercount_UN = @ActUndercount_UN) begin
		set @isExists= 1;
	end else begin
		set @isExists= 0;
	end

	declare @methodName varchar(100);

	if @isExists = 1 and not exists(select top 1 1 from @Arch30ValuesTable) begin
		--Это простое изменение активности записи
		update ArchCalc_Replace_ActUndercount set IsInactive = @IsInactive, FinishDateTime = @FinishDateTime, 
		IsFinishDateTimeInclusive = @IsFinishDateTimeInclusive where ActUndercount_UN = @ActUndercount_UN

		if (@IsInactive = 0) set @methodName = 'Активна';
		else if (@IsInactive = 1) set @methodName = 'Не активна';

		--Пишем об изменении активности записи
		insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, 
			CUS_ID, ZamerDateTime, CommentString)
		select @TI_ID, @ChannelType,@StartDateTime, @User_ID, GETDATE(), 9, --Признак акта недоучета
			0, @FinishDateTime, 'Измен статус записи на <' + @methodName + '> ' + @CommentString

	end else begin
		--Здесь изменение или добавление получасовок
		--Удаление старой записи
		if (@isExists = 1) begin
			delete from ArchCalc_Replace_ActUndercount where ActUndercount_UN = @ActUndercount_UN
		end

		--Добавление новой
		insert into ArchCalc_Replace_ActUndercount ([TI_ID]
           ,[ChannelType]
           ,[StartDateTime]
           ,[FinishDateTime]
           ,[AddedValue]
           ,[CommentString]
           ,[User_ID]
           ,[IsInactive]
           ,[CUS_ID]
           ,[ActUndercount_UN]
           ,[ActMode]
           ,[IsFinishDateTimeInclusive])
		values(@TI_ID, @ChannelType, @StartDateTime, @FinishDateTime, @AddedValue, 
			@CommentString,	@User_ID, @IsInactive, @CUS_ID, @ActUndercount_UN, @ActMode, @IsFinishDateTimeInclusive)
		
		MERGE [dbo].[ArchCalc_Replace_ActUndercount_Halfhours] as a USING @Arch30ValuesTable n
		ON a.ActUndercount_UN = n.ActUndercount_UN and a.HalfhourDateTime = n.HalfhourDateTime
		WHEN MATCHED THEN UPDATE SET [AddedValue] = n.AddedValue
		WHEN NOT MATCHED THEN 
		INSERT ([ActUndercount_UN],[HalfhourDateTime],[AddedValue])
		VALUES ([ActUndercount_UN],[HalfhourDateTime],[AddedValue]);

		if (@ActMode = 0) set @methodName = 'Замещение';
		else if (@ActMode = 1) set @methodName = 'Дополнение';
		else if (@ActMode = 2) set @methodName = 'Вычитание';
		else set @methodName = '[не определен]';

		--Вставка в журнал замещений запись о том что был акт недоучета 
		insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, 
			CUS_ID, ZamerDateTime, CommentString)
		select @TI_ID, @ChannelType,@StartDateTime, @User_ID, GETDATE(), 9, --Признак акта недоучета
			0, @FinishDateTime, 'Распределено ' + ltrim(Str(@AddedValue / 1000, 15,3)) + ' кВт*ч,' +
						' применен метод <' + @methodName + '> ' + @CommentString
	end

	
COMMIT
END TRY  
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK 
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 16 
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
end
go
   grant EXECUTE on usp2_WriteArchActUndercountHalfhours to [UserCalcService]
go