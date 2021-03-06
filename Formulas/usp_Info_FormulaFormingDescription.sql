if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FormulaFormingDescription')
          and type in ('P','PC'))
   drop procedure usp2_Info_FormulaFormingDescription
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
--		Июль, 2011
--
-- Описание:
--
--		Формируем description для формулы с фазами
-- ======================================================================================
create proc [dbo].[usp2_Info_FormulaFormingDescription] 
			@InnerLevel int,
			@formulaId varchar(22),
			@formulaName varchar(255),
			@FormulaType_ID tinyint,
			@FormulaClassification_ID tinyint,
			@PS_ID int,
			@HierLev3_ID int,
			@HierLev2_ID int,
			@HierLev1_ID int
AS
BEGIN
create table #tmp(
	ti_id int
	)
	
	declare @PhaseNumber tinyint;
	
	--Номер фазы который мы выбираем
	set @PhaseNumber = 	case @FormulaClassification_ID
		when 10 then 1
		when 11 then 2
		when 12 then 3
	end;
	
--Смотрим родителя 
	if (@PS_ID is not null) BEGIN
		insert into #tmp
		select ti_id from Info_ti where PS_ID = @PS_ID  and PhaseNumber = @PhaseNumber
	END ELSE BEGIN
		if (@HierLev3_ID is not null) BEGIN
			insert into #tmp
			select ti_id from Info_ti ti
			join dbo.Dict_PS ps on ti.PS_ID = ps.PS_ID
			where ps.HierLev3_ID = @HierLev3_ID and ti.PhaseNumber = @PhaseNumber
		END ELSE BEGIN
			if (@HierLev2_ID is not null) BEGIN
				insert into #tmp
				select ti_id from Info_ti ti
				join dbo.Dict_PS ps on ti.PS_ID = ps.PS_ID
				join dbo.Dict_HierLev3 dh3 on dh3.HierLev3_ID = ps.HierLev3_ID
				where dh3.HierLev2_ID = @HierLev2_ID and ti.PhaseNumber = @PhaseNumber
			END ELSE BEGIN
				if (@HierLev1_ID is not null) BEGIN
					insert into #tmp
					select ti_id from Info_ti ti
						join dbo.Dict_PS ps on ti.PS_ID = ps.PS_ID
					join dbo.Dict_HierLev3 dh3 on dh3.HierLev3_ID = ps.HierLev3_ID
					join dbo.Dict_HierLev2 dh2 on dh2.HierLev2_ID = dh3.HierLev2_ID
					where dh2.HierLev1_ID = @HierLev1_ID and ti.PhaseNumber = @PhaseNumber
				END
			END	
		END		
	END	
	declare @c int;
	
	set @c = (select Count(*) from #tmp);

	--select * from #tmp
	select @InnerLevel,@formulaId as Formula_UN,cast(ROW_NUMBER() over (order by TI_ID) as int) as StringNumber, '' as OperBefore,null as UsedFormula_UN,fd.TI_ID,null as ContrTI_ID,cast(1 as tinyint) as ChannelType,
	case when ROW_NUMBER() over (order by TI_ID) = @c then '' else '+' end as OperAfter
	,@formulaName as FormulaName, @FormulaType_ID as FormulaType_ID, null as Section_ID, null as TP_ID, cast(1 as bit) as IsIntegral
	from #tmp fd
	
	drop table #tmp;
END
go
   grant EXECUTE on usp2_Info_FormulaFormingDescription to [UserCalcService]
go
   grant EXECUTE on usp2_Info_FormulaFormingDescription to [userdatacollectorservice]
go