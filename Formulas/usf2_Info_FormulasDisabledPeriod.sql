if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_FormulasDisabledPeriod')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_FormulasDisabledPeriod
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
--		Ноябрь, 2016
--
-- Описание:
--
--		Возвращаем периоды отключения формул
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_FormulasDisabledPeriod] (
		@formulas nvarchar(4000), @dtStart DateTime, @dtEnd DateTime
)
      RETURNS @tbl TABLE (
	[Formula_UN] nvarchar(22),
	[StartIndx] int, 
	[FinishIndx] int
	
)
   BEGIN
		insert into @tbl select fd.Formula_UN, 
		CEILING(cast(DATEDIFF(n, @dtStart, StartDateTime) as float) / 30) as StartIndx,  
		CEILING(cast(DATEDIFF(n, @dtStart, ISNULL(FinishDateTime, @dtEnd)) as float) / 30) as FinishIndx
		from [dbo].[Info_Formula_DisabledPeriod] fd
		where Formula_UN in (select distinct STRnumber from usf2_Utils_iter_strintlist_to_table(@formulas))
		and StartDateTime <= @dtEnd and (FinishDateTime is null or @dtStart <= FinishDateTime)
		order by Formula_UN, StartDateTime

	RETURN
END
go
   grant select on usf2_Info_FormulasDisabledPeriod to [UserCalcService]
go