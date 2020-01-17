if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_CoeffTransformators')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_CoeffTransformators
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь, 2011
--
-- Описание:
--
-- Возврат коэфф трансформации для определенной даты
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_CoeffTransformators] (
			@TI_ID int,
			@EventDateTime DateTime,
			@indx tinyint,
			@IsDivOnCoeffTransformation bit
)	
returns float
WITH SCHEMABINDING
AS
begin
if (@IsDivOnCoeffTransformation = 0) return 1;

set @EventDateTime = DateAdd(minute, @indx * 30, @EventDateTime);

if ISNULL((
select top 1 IsCoeffTransformationDisabled from [dbo].[ArchCalc_CoeffTransformation_DisabledStatus] d 
where d.TI_ID = @TI_ID and @EventDateTime between d.StartDateTime and ISNULL(d.FinishDateTime,'21000101') 
order by StartDateTime desc
), (select top 1 IsCoeffTransformationDisabled from dbo.Info_TI where TI_ID = @TI_ID))=0 begin
	return ISNULL((select top 1 tr.COEFI * tr.COEFU from [dbo].[Info_Transformators] tr 
	where tr.TI_ID = @TI_ID and @EventDateTime between tr.StartDateTime and ISNULL(tr.FinishDateTime,'21000101') 
	order by StartDateTime desc), 1)
end
return 1
end
go
grant exec on usf2_Info_CoeffTransformators to [UserCalcService]
go