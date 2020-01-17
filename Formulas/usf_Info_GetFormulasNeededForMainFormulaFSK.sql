if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFormulasNeededForMainFormulaFSK')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFormulasNeededForMainFormulaFSK
go

/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/25/2008 12:50:13 ******/
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
--		Январь, 2009
--
-- Описание:
--
--		Создает список идентификаторов формул которые необходимы для расчета указанной формулы для ТП КА
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetFormulasNeededForMainFormulaFSK]
(	
	-- Add the parameters for the function here
	@formulaId varchar(22),
	@Innerparam int,
	@ClosedPeriod_ID uniqueidentifier = null
)
RETURNS TABLE
AS RETURN
with formulas(formulaId, InnerLevel, closedPeriod_ID) as
(
	select distinct ifd.UsedFormula_UN, @Innerparam + 1, '00000000-0000-0000-0000-000000000000'
	from dbo.Info_TP2_OurSide_Formula_Description ifd
	where	ifd.Formula_UN = @formulaId and not ifd.UsedFormula_UN is null
	union all 
	--Рекурсивная часть на вложенные далее формулы
	select ifd.UsedFormula_UN, f.InnerLevel + 1, '00000000-0000-0000-0000-000000000000'
	from formulas f
	join dbo.Info_TP2_OurSide_Formula_Description ifd on ifd.Formula_UN = f.formulaId
	where ifd.UsedFormula_UN is not null

)

select * from formulas
go
   grant select on usf2_Info_GetFormulasNeededForMainFormulaFSK to [UserCalcService]
go