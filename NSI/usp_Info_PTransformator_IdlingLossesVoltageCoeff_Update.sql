if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_PTransformator_IdlingLossesVoltageCoeff_Update')
          and type in ('P','PC'))
   drop procedure usp2_Info_PTransformator_IdlingLossesVoltageCoeff_Update
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
--		Май, 2010
--
-- Описание:
--
--		Сохранение коэффициентов Usr/Un трансформатора
--
-- ======================================================================================

create PROCEDURE [dbo].[usp2_Info_PTransformator_IdlingLossesVoltageCoeff_Update]
	@IsNew bit,
    @PTransformator_ID int,
	@Old_StartDateTime datetime = NULL,
    @StartDateTime datetime,
	@FinishDateTime datetime,
	@CoeffAverVoltageToNominal float,
    @CUS_ID tinyint
AS BEGIN
	IF @IsNew = 1 BEGIN
		INSERT dbo.Info_PTransformator_IdlingLossesVoltageCoeff (PTransformator_ID, StartDateTime, FinishDateTime, CoeffAverVoltageToNominal, CUS_ID)
		VALUES (@PTransformator_ID, @StartDateTime, @FinishDateTime, @CoeffAverVoltageToNominal, @CUS_ID)
    END ELSE BEGIN
        UPDATE dbo.Info_PTransformator_IdlingLossesVoltageCoeff SET
			StartDateTime = @StartDateTime, FinishDateTime = @FinishDateTime, CoeffAverVoltageToNominal = @CoeffAverVoltageToNominal, CUS_ID = @CUS_ID
		WHERE PTransformator_ID = @PTransformator_ID AND StartDateTime = @Old_StartDateTime
	END
END

go
   grant EXECUTE on usp2_Info_PTransformator_IdlingLossesVoltageCoeff_Update to [UserCalcService]
go