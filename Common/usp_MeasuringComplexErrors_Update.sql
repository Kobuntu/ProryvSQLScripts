if exists (select 1
          from sysobjects
          where  id = object_id('usp2_MeasuringComplexErrors_Update')
          and type in ('P','PC'))
   drop procedure usp2_MeasuringComplexErrors_Update
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Март, 2010
--
-- Описание:
--
--		Сохранение погрешностей измерительного комплекса
--
-- ======================================================================================

create PROCEDURE [dbo].[usp2_MeasuringComplexErrors_Update]
	@IsNew bit,
    @TI_ID int,
	@Old_StartDateTime datetime = NULL,
    @StartDateTime datetime,
	@FinishDateTime datetime,
	@MeasuringComplexError float,
    @CUS_ID tinyint
AS BEGIN
	IF @IsNew = 1 BEGIN
		INSERT dbo.Info_MeasuringComplexError (TI_ID, StartDateTime, FinishDateTime, MeasuringComplexError, CUS_ID)
		VALUES (@TI_ID, @StartDateTime, @FinishDateTime, @MeasuringComplexError, @CUS_ID)
    END ELSE BEGIN
        UPDATE dbo.Info_MeasuringComplexError SET
			StartDateTime = @StartDateTime, FinishDateTime = @FinishDateTime, MeasuringComplexError = @MeasuringComplexError, CUS_ID = @CUS_ID
		WHERE TI_ID = @TI_ID AND StartDateTime = @Old_StartDateTime
	END
END
go
   grant EXECUTE on usp2_MeasuringComplexErrors_Update to [UserCalcService]
go