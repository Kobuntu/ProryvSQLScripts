if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FormulaSelect')
          and type in ('P','PC'))
   drop procedure usp2_Info_FormulaSelect
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
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
--		Сентябрь, 2008
--
-- Описание:
--
--		Выбираем параметры формулы, также параметры вложенных формул и уровни вложенности этих формул
--
-- ======================================================================================

create proc [dbo].[usp2_Info_FormulaSelect]
	@formulaId varchar(22)
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@InnerLevel int,
@Formula_UN varchar(22),
@formulaName varchar(255),
@FormulaType_ID tinyint,
@FormulaClassification_ID tinyint,
@PS_ID int,
@HierLev3_ID int,
@HierLev2_ID int,
@HierLev1_ID int


--Берем параметры самой формулы
	select 0 as InnerLevel,fl.Formula_UN,fd.StringNumber,fd.OperBefore,fd.UsedFormula_UN
	,fd.TI_ID,fd.ContrTI_ID,fd.ChannelType,fd.OperAfter,FormulaName
	, FormulaType_ID, Section_ID, TP_ID, IsIntegral
	, FormulaClassification_ID, PS_ID,HierLev3_ID,HierLev2_ID, HierLev1_ID, fd.FormulaConstant_UN,
	Formula_TP_OurSide_UN, UANode_ID
	into #FormulaDescription
	from dbo.Info_Formula_List fl
	left join 
	(
		select 0 as InnerLevel,Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,ContrTI_ID,ChannelType,OperAfter,
		Section_ID, TP_ID
		, cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral,
		FormulaConstant_UN,Formula_TP_OurSide_UN, UANode_ID from 
		dbo.Info_Formula_Description
	) fd
	on fl.Formula_UN = fd.Formula_UN
	where fl.Formula_UN=@formulaId
	union all
	--И параметры вложенной формулы
	select distinct InnerLevel,Formula_UN = formulaId,StringNumber,OperBefore,UsedFormula_UN,TI_ID,ContrTI_ID,ChannelType,OperAfter,FormulaName, FormulaType_ID, Section_ID, TP_ID, IsIntegral
		,FormulaClassification_ID, PS_ID,HierLev3_ID,HierLev2_ID, HierLev1_ID,FormulaConstant_UN, Formula_TP_OurSide_UN, UANode_ID
	from usf2_Info_GetFormulasNeededForMainFormula(@formulaId,0)
	
	
	
	--Группируем формулы по родителям
	select distinct InnerLevel, Formula_UN, FormulaName, FormulaType_ID, FormulaClassification_ID, PS_ID,HierLev3_ID,HierLev2_ID, HierLev1_ID 
	into #GropingFormulas
	from #FormulaDescription
	
	--select * from #FormulaDescription
	
	
	--Перебираем группы формул
	declare formulaGroupingCursor cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select InnerLevel, Formula_UN, FormulaName, FormulaType_ID, FormulaClassification_ID, PS_ID,HierLev3_ID,HierLev2_ID, HierLev1_ID 
	from #GropingFormulas
	open formulaGroupingCursor;
	FETCH NEXT FROM formulaGroupingCursor into @InnerLevel, @Formula_UN, @formulaName, @FormulaType_ID, @FormulaClassification_ID, @PS_ID, @HierLev3_ID, @HierLev2_ID, @HierLev1_ID
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
		if @FormulaClassification_ID > 9 and @FormulaClassification_ID < 13 BEGIN

			select * 
			into #description
			from #FormulaDescription  
			where Formula_UN=@Formula_UN
			
			--select * from #description
			
			--	--Если есть описание формулы
			if (select Count(*) from #description where UsedFormula_UN is not null OR TI_ID  is not null OR ContrTI_ID is not null OR Section_ID  is not null OR TP_ID  is not null) > 0 begin
			
				declare @PhaseNumber tinyint;
				--Номер фазы который мы выбираем
				set @PhaseNumber = 	case @FormulaClassification_ID
					when 10 then 1
					when 11 then 2
					when 12 then 3
				end;
			
				select fd.TI_ID 
				into #tiList
				from #description fd
				join Info_TI ti on ti.TI_ID = fd.TI_ID
				where fd.Formula_UN=@Formula_UN and fd.TI_ID is not null and (ti.PhaseNumber is null or ti.PhaseNumber <> @PhaseNumber);
				
				update #FormulaDescription
				set OperBefore = OperBefore + '0*'
				where Formula_UN= @Formula_UN AND TI_ID is not null and TI_ID in (select ti_id from #tiList);
				
				drop table #tiList;
							
			end else begin
				
				--Описания нет формируем сами
				--Удаляем пустую запись
				delete from #FormulaDescription
				where Formula_UN= @Formula_UN
				
				--Выбираем точки с нужными нам фазами
				insert into #FormulaDescription (InnerLevel,Formula_UN,StringNumber,OperBefore,UsedFormula_UN
					,TI_ID,ContrTI_ID,ChannelType,OperAfter,FormulaName
					, FormulaType_ID, Section_ID, TP_ID, IsIntegral)
				exec usp2_Info_FormulaFormingDescription @InnerLevel, @Formula_UN, @formulaName,@FormulaType_ID,@FormulaClassification_ID,
					@PS_ID,@HierLev3_ID,@HierLev2_ID,@HierLev1_ID
					
			end
				
				drop table #description
			
			END
	
	--select @Formula_UN, @FormulaClassification_ID, @PS_ID, @HierLev3_ID, @HierLev2_ID, @HierLev1_ID
	
	FETCH NEXT FROM formulaGroupingCursor into @InnerLevel, @Formula_UN, @formulaName, @FormulaType_ID, @FormulaClassification_ID, @PS_ID, @HierLev3_ID, @HierLev2_ID, @HierLev1_ID
	end;
	CLOSE formulaGroupingCursor
	DEALLOCATE formulaGroupingCursor


	select InnerLevel,Formula_UN,StringNumber,OperBefore,UsedFormula_UN
					,TI_ID,ContrTI_ID,ChannelType,OperAfter,FormulaName
					, FormulaType_ID, Section_ID, TP_ID, ISNULL(IsIntegral,0) as IsIntegral, FormulaConstant_UN, Formula_TP_OurSide_UN,UANode_ID
	from #FormulaDescription 
	order by InnerLevel, Formula_UN, StringNumber 

end
go
   grant EXECUTE on usp2_Info_FormulaSelect to [UserCalcService]
go
   grant EXECUTE on usp2_Info_FormulaSelect to [userdatacollectorservice]
go