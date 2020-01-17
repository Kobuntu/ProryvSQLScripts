if exists (select 1
          from sysobjects
          where  id = object_id('usp2_TpRange_WriteArray')
          and type in ('P','PC'))
 drop procedure usp2_TpRange_WriteArray
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_TpRange_ApplyArray')
          and type in ('P','PC'))
 drop procedure usp2_TpRange_ApplyArray
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'TpActiveType' AND ss.name = N'dbo')
DROP TYPE [dbo].[TpActiveType]
-- Пересоздаем заново
CREATE TYPE [dbo].[TpActiveType] AS TABLE
(
	TP_ID int,
	IsActive bit,
	VoltageLevel tinyint null
)
GO

grant EXECUTE on TYPE::TpActiveType to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2014
--
-- Описание:
--
--		Сохраняем схемы учета для сечения
--
-- ======================================================================================
create proc [dbo].[usp2_TpRange_WriteArray]
	@TpRangeScheme_ID uniqueidentifier,
	@Section_ID int,
	@SchemeName varchar(255),
	@TpActive TpActiveType READONLY,
	@ApplyDateTime DateTime = null,
	@User_ID varchar(22) = null,
	@SchemeDescription varchar(max) = null
	
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	BEGIN TRY  BEGIN TRANSACTION

		if exists(select top 1 1 from Info_TpRangeScheme where TpRangeScheme_ID = @TpRangeScheme_ID) begin
			update [dbo].[Info_TpRangeScheme] set ApplyDateTime = @ApplyDateTime, User_ID = @User_ID, 
			SchemeName = @SchemeName, SchemeDescription = @SchemeDescription
			where TpRangeScheme_ID = @TpRangeScheme_ID and Section_ID = @Section_ID
		end else begin 
			insert into [dbo].[Info_TpRangeScheme] ([TpRangeScheme_ID]
			   ,[Section_ID]
			   ,[ApplyDateTime]
			   ,[User_ID]
			   ,[SchemeName]
			   ,[SchemeDescription])
			values (@TpRangeScheme_ID
			   ,@Section_ID
			   ,NULL
			   ,@User_ID
			   ,@SchemeName
			   ,@SchemeDescription)
		end

		delete from Info_TpRangeScheme_To_Tp where TpRangeScheme_ID = @TpRangeScheme_ID

		insert into Info_TpRangeScheme_To_Tp (TpRangeScheme_ID, TP_ID, IsActive, VoltageLevel)
		select @TpRangeScheme_ID, a.TP_ID, a.IsActive, a.VoltageLevel from  @TpActive a

		COMMIT
	END TRY
	BEGIN CATCH
		--Ошибка, откатываем все изменения
		IF @@TRANCOUNT > 0 ROLLBACK 

		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
	
end
go
   grant EXECUTE on usp2_TpRange_WriteArray to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2014
--
-- Описание:
--
--		Применяем схемы учета для сечения
--
-- ======================================================================================
create proc [dbo].[usp2_TpRange_ApplyArray]
	@TpRangeScheme_ID uniqueidentifier,
	@Section_ID int,
	@TpActive TpActiveType READONLY,
	@ApplyDateTime DateTime,
	@User_ID varchar(22)
	
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	BEGIN TRY  BEGIN TRANSACTION

		update [dbo].[Info_TpRangeScheme] set ApplyDateTime = @ApplyDateTime, User_ID = @User_ID 
		where TpRangeScheme_ID = @TpRangeScheme_ID and Section_ID = @Section_ID

		--Выключать очень просто
		--Точки которые необходимо выключить, двигаем время на наше
		update Info_Section_Description2 set FinishDateTime = DateAdd(minute, -1, @ApplyDateTime)
		where 
		Section_ID = @Section_ID and TP_ID in (select distinct TP_ID from @TpActive where IsActive = 0) 
		and StartDateTime<@ApplyDateTime and ISNULL(FinishDateTime, '21000101') > @ApplyDateTime

		--Удаляем записи которые стартуют после времени закрытия
		delete from Info_Section_Description2 where 
		Section_ID = @Section_ID and TP_ID in (select distinct TP_ID from @TpActive where IsActive = 0) 
		and StartDateTime=@ApplyDateTime 

	
		--Теперь обрабатываем точки, которые надо включить
		declare @TP_ID int, @StartForwardDateTime DateTime, @StartBackDateTime DateTime, @VoltageLevel tinyint, @ExistsVoltageLevel tinyint;

		declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TP_ID, VoltageLevel from @TpActive where IsActive = 1
		open t;
		FETCH NEXT FROM t into @TP_ID, @VoltageLevel
		WHILE @@FETCH_STATUS = 0
		BEGIN

		--Включаем ТП только если не работает на нужный момент времени, т.е. мы не вклиниваемся в существующий диапазон (эта ситуация не обрабатывается)
		if (not exists(select top 1 1 from Info_Section_Description2 where Section_ID = @Section_ID 
			and TP_ID = @TP_ID and StartDateTime<=@ApplyDateTime and ISNULL(FinishDateTime, '21000101') > @ApplyDateTime)) begin
		
			--Ищем диапазон спереди, к которму можно присоединить
			set @StartForwardDateTime = (select top 1 StartDateTime from Info_Section_Description2 where Section_ID = @Section_ID and TP_ID = @TP_ID 
				and StartDateTime > @ApplyDateTime order by StartDateTime)

			--Ищем диапазон сзади, к которому можно присоединить
			set @StartBackDateTime = (select top 1 StartDateTime from Info_Section_Description2 where Section_ID = @Section_ID and TP_ID = @TP_ID 
				and FinishDateTime = DateAdd(minute, -1, @ApplyDateTime) order by StartDateTime)

			--Если есть диапазон спереди, то его просто расширяем назад к нашей записи,
			if (@StartForwardDateTime is not null) begin
				if (@StartBackDateTime is not null) begin 
					
					--Есть диапазон сзади
					-----------------------------------------------------> напраление времени
					--					^					^
					--		Back		|applyDateTime		|	start Forward
					--	запись сзади	|наша запись		|запись спереди

					--Есть 2 диапазона, наш попадает ровно между ними, 
					--первый расширяем до окончания второго
					update Info_Section_Description2 Set FinishDateTime = 
						(
							select top 1 FinishDateTime from Info_Section_Description2 where Section_ID = @Section_ID and TP_ID = @TP_ID 
							and StartDateTime > @ApplyDateTime order by StartDateTime
						)
					where Section_ID = @Section_ID and TP_ID = @TP_ID and FinishDateTime = DateAdd(minute, -1, @ApplyDateTime);

					--удаляем второй 
					delete from Info_Section_Description2 where Section_ID = @Section_ID and TP_ID = @TP_ID and StartDateTime = @StartForwardDateTime;

				end else begin

					--Просто расширяем назад диапазон спереди
					-----------------------------------------------------> напраление времени
					--					^					^
					--					|applyDateTime		|	Forward
					--					|наша запись		|запись спереди


					update Info_Section_Description2 Set StartDateTime = @ApplyDateTime 
					where Section_ID = @Section_ID and TP_ID = @TP_ID and StartDateTime = @StartForwardDateTime;
				end
			
			end else begin 
				if (@StartBackDateTime is not null) begin

					--Есть диапазин сзади к которому можно присоединить, окончание == началу нашего времени
					-----------------------------------------------------> напраление времени
					--					^					
					--		Back finish	|applyDateTime		
					--	запись сзади	|наша запись		

					update Info_Section_Description2 Set  FinishDateTime = null
					where Section_ID = @Section_ID and TP_ID = @TP_ID and FinishDateTime = DateAdd(minute, -1, @ApplyDateTime);
				end else begin
					--Вставляем новую запись
					insert into Info_Section_Description2 (Section_ID, TP_ID, StartDateTime, FinishDateTime, IsTransit, CUS_ID)
					values (@Section_ID, @TP_ID, @ApplyDateTime, null, 0, 0);
				end
			end;
		end

		--Обрабатываем тарифный уровень напряжения, изменяем если только задан
		if (@VoltageLevel is not null) begin
			
			--Выключения не обрабатываем, null считаем что тарифный уровень остается без изменений
			
			--Ищем запись в которую вклиниваемся
			select top 1 @StartBackDateTime = StartDateTime, @ExistsVoltageLevel = VoltageLevel from Info_TP_VoltageLevel where TP_ID = @TP_ID
			and StartDateTime <= @ApplyDateTime	and (FinishDateTime is null or (FinishDateTime is not null and FinishDateTime > @ApplyDateTime))

			if (@ExistsVoltageLevel is not null and @ExistsVoltageLevel <> @VoltageLevel) begin

				--Попали в действующую запись с другим уровнем напряжения

				if (@StartBackDateTime = @ApplyDateTime) begin
					--Запись совпадает по времени начала, надо ее удалить 
					delete from Info_TP_VoltageLevel where TP_ID = @TP_ID and StartDateTime = @ApplyDateTime;
				end else begin
					--Закрываем эту запись
					update Info_TP_VoltageLevel Set FinishDateTime = DateAdd(minute, -1, @ApplyDateTime) where TP_ID = @TP_ID and StartDateTime = @StartBackDateTime;
				end;


				--Вставляем новую запись
				insert into Info_TP_VoltageLevel (TP_ID, StartDateTime, FinishDateTime, VoltageLevel, DispatchDateTime)
				values (@TP_ID, @ApplyDateTime, null, @VoltageLevel, GETDATE());

			end else if (@ExistsVoltageLevel is null) begin
				--Вставляем новую запись
				insert into Info_TP_VoltageLevel (TP_ID, StartDateTime, FinishDateTime, VoltageLevel, DispatchDateTime)
				values (@TP_ID, @ApplyDateTime, null, @VoltageLevel, GETDATE());
			end
		end;

		FETCH NEXT FROM t into @TP_ID,@VoltageLevel
	END;
	CLOSE t
	DEALLOCATE t

	COMMIT
	END TRY
	BEGIN CATCH
		--Ошибка, откатываем все изменения
		IF @@TRANCOUNT > 0 ROLLBACK 

		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
	
end
go
   grant EXECUTE on usp2_TpRange_ApplyArray to [UserCalcService]
go