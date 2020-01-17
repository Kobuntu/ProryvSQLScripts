set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_BitRange')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_BitRange
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2013
--
-- Описание:
--
--		Взводим биты с @fromIndx и по @toIndx в @value
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_Utils_BitRange]
(	
	@fromIndx int,
	@toIndx int,
	@value bigint
)
RETURNS bigint
AS
begin
declare
@i int,
@Res bigint

set @Res = @value;
set @i = @fromIndx;
while @i<=@toIndx begin
	set @Res = dbo.sfclr_Utils_BitOperations(@Res, @i, 1);
	set @i = @i + 1;
end;
return @Res
end

go
grant EXECUTE on usf2_Utils_BitRange to [UserCalcService]
go