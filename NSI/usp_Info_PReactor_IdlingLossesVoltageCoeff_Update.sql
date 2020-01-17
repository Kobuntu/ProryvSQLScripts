if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_PReactor_IdlingLossesVoltageCoeff_Update')
          and type in ('P','PC'))
   drop procedure usp2_Info_PReactor_IdlingLossesVoltageCoeff_Update
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
--		Сохранение коэффициентов Usr/Un реактора
--
-- ======================================================================================

Create PROCEDURE [dbo].[usp2_Info_PReactor_IdlingLossesVoltageCoeff_Update]
	@IsNew bit,
    @PReactor_ID int,
	@Old_StartDateTime datetime = NULL,
    @StartDateTime datetime,
	@FinishDateTime datetime,
	@CoeffAverVoltageToNominal float,
    @CUS_ID tinyint
AS BEGIN
	IF @IsNew = 1 BEGIN
		INSERT dbo.Info_PReactors_IdlingLossesVoltageCoeff (PReactor_ID, StartDateTime, FinishDateTime, CoeffAverVoltageToNominal, CUS_ID)
		VALUES (@PReactor_ID, @StartDateTime, @FinishDateTime, @CoeffAverVoltageToNominal, @CUS_ID)
    END ELSE BEGIN
        UPDATE dbo.Info_PReactors_IdlingLossesVoltageCoeff SET
			StartDateTime = @StartDateTime, FinishDateTime = @FinishDateTime, CoeffAverVoltageToNominal = @CoeffAverVoltageToNominal, CUS_ID = @CUS_ID
		WHERE PReactor_ID = @PReactor_ID AND StartDateTime = @Old_StartDateTime
	END
END

go
   grant EXECUTE on usp2_Info_PReactor_IdlingLossesVoltageCoeff_Update to [UserCalcService]
go