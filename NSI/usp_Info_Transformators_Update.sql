if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_Transformators_Update')
          and type in ('P','PC'))
   drop procedure usp2_Info_Transformators_Update
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Январь, 2010
--
-- Описание:
--
--		Сохранение коэффициентов трансформации
--
-- ======================================================================================

create PROCEDURE [dbo].[usp2_Info_Transformators_Update]
	@IsNew bit,
    @TI_ID int,
	@Old_StartDateTime datetime = NULL,
    @StartDateTime datetime,
	@FinishDateTime datetime,
	@COEFU float,
	@COEFI float,
    @CUS_ID tinyint
AS BEGIN
	IF @IsNew = 1 BEGIN
		INSERT dbo.Info_Transformators (TI_ID, StartDateTime, FinishDateTime, COEFU, COEFI, CUS_ID)
		VALUES (@TI_ID, @StartDateTime, @FinishDateTime, @COEFU, @COEFI, @CUS_ID)
    END ELSE BEGIN
        UPDATE dbo.Info_Transformators SET
			StartDateTime = @StartDateTime, FinishDateTime = @FinishDateTime, COEFU = @COEFU, COEFI = @COEFI, CUS_ID = @CUS_ID
		WHERE TI_ID = @TI_ID AND StartDateTime = @Old_StartDateTime
	END
END

go
   grant EXECUTE on usp2_Info_Transformators_Update to [UserCalcService]
go