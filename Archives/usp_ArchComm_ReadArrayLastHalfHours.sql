if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_ReadArrayLastHalfHours')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_ReadArrayLastHalfHours
go

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
--		Последние нерасчетные значения по списку точек
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_ReadArrayLastHalfHours]
	@TI_Array nvarchar(max), --Это идентификаторы ТИ разделенные запятой
	@isCoeffEnabled bit, --Домножать или нет на коэф. трансформации
	@DiscreteType tinyint --Период дискретизации 0 - получасовки, 1 - часовка, 47 - сутки
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select TI_ID, TIType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, PS_ID, TPCoefOurSide
into #tis
from [dbo].[usf2_Utils_Split](@TI_Array, ',') a
join Info_TI ti on ti.TI_ID = a.Items
order by TIType, TI_ID

declare @titype tinyint,
@sqlString1 NVARCHAR(4000),
@sqlString2 NVARCHAR(max),
@sqlString3 NVARCHAR(4000),
@sqlExecute NVARCHAR(max),
@sqlDiscreteWhere NVARCHAR(4000),
@sqlTable NVARCHAR(200),
@tableName nvarchar(100);

if (@DiscreteType <> 47) begin
	set @sqlDiscreteWhere = '(VAL_01 is not null and VAL_02 is not null) or (VAL_03 is not null and VAL_04 is not null) or (VAL_05 is not null and VAL_06 is not null) or (VAL_07 is not null and VAL_08 is not null) or (VAL_09 is not null and VAL_10 is not null)
		and (VAL_11 is not null and VAL_12 is not null) or (VAL_13 is not null and VAL_14 is not null) or (VAL_15 is not null and VAL_16 is not null) or (VAL_17 is not null and VAL_18 is not null) or (VAL_19 is not null and VAL_20 is not null)
		and (VAL_21 is not null and VAL_22 is not null) or (VAL_23 is not null and VAL_24 is not null) or (VAL_25 is not null and VAL_26 is not null) or (VAL_27 is not null and VAL_28 is not null) or (VAL_29 is not null and VAL_30 is not null)
		and (VAL_31 is not null and VAL_32 is not null) or (VAL_33 is not null and VAL_34 is not null) or (VAL_35 is not null and VAL_36 is not null) or (VAL_37 is not null and VAL_38 is not null) or (VAL_39 is not null and VAL_40 is not null)
		and (VAL_41 is not null and VAL_42 is not null) or (VAL_43 is not null and VAL_44 is not null) or (VAL_45 is not null and VAL_46 is not null) or (VAL_47 is not null and VAL_48 is not null)';
end else
	set @sqlDiscreteWhere = 'VAL_01 is not null and VAL_02 is not null and VAL_03 is not null and VAL_04 is not null and VAL_05 is not null and VAL_06 is not null and VAL_07 is not null and VAL_08 is not null and VAL_09 is not null and VAL_10 is not null
		and VAL_11 is not null and VAL_12 is not null and VAL_13 is not null and VAL_14 is not null and VAL_15 is not null and VAL_16 is not null and VAL_17 is not null and VAL_18 is not null and VAL_19 is not null and VAL_20 is not null
		and VAL_21 is not null and VAL_22 is not null and VAL_23 is not null and VAL_24 is not null and VAL_25 is not null and VAL_26 is not null and VAL_27 is not null and VAL_28 is not null and VAL_29 is not null and VAL_30 is not null
		and VAL_31 is not null and VAL_32 is not null and VAL_33 is not null and VAL_34 is not null and VAL_35 is not null and VAL_36 is not null and VAL_37 is not null and VAL_38 is not null and VAL_39 is not null and VAL_40 is not null
		and VAL_41 is not null and VAL_42 is not null and VAL_43 is not null and VAL_44 is not null and VAL_45 is not null and VAL_46 is not null and VAL_47 is not null and VAL_48 is not null';
end

set @SQLString1 = 'insert into #tmp
		select a.TI_ID, a.ChannelType,
		 a.DataSource_ID, MAX(EventDate) as MaxEventDateTime, 
		(select [Priority] from Expl_DataSource_PriorityList  where DataSource_ID = a.DataSource_ID 
			and [Year] = Year(MAX(EventDate)) and [Month] = Month(MAX(EventDate))) as [Priority]
		from #tis ti
		join '
set @SQLString2 = ' a on a.TI_ID = ti.TI_ID 
		where ChannelType in (1,2) and '+ @sqlDiscreteWhere +' and ti.titype = @titype
		group by a.TI_ID, a.ChannelType, a.DataSource_ID
		order by a.TI_ID, a.ChannelType, MaxEventDateTime desc, [Priority] desc; 
		insert into #result (TI_ID, PS_ID, TIType, ChannelType, dt, Value, ValidStatus, Coef_tp, Coeff)
		select r.TI_ID, ti.PS_ID, ti.TIType, dbo.usf2_ReverseTariffChannel(0, ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, r.TI_ID, EventDateTime, EventDateTime)
		,EventDateTime, Val, Valid,TPCoefOurSide, 1
		from
		(
			select r.TI_ID, ChannelType, SUM(Val) as Val
			, cast(sum(Valid & 8) as bit) * 8 + cast(sum(Valid & 4) as bit) * 4 --Накапливаем состояния неисправности
			+ cast(sum(Valid & 2) as bit) * 2 + cast(sum(Valid & 1) as bit) as Valid, 
			Min(EventDateTime) as EventDateTime from 
			(
				select ROW_NUMBER() over (partition by TI_ID, ChannelType order by ValueRow desc) as number
				,unp.TI_ID, ChannelType
				,case when Val < 0 then 0 else Val end
					* isnull((select top 1 COEFI*COEFU from Info_Transformators where TI_ID=unp.TI_ID and DateAdd(minute,(SUBSTRING(ValueRow,5,2)-1) * 30,EventDate) between StartDateTime and FinishDateTime), 1) as Val
				,DateAdd(minute,(SUBSTRING(ValueRow,5,2)-1) * 30,EventDate) as EventDateTime
				,dbo.sfclr_Utils_BitOperations2(ValidStatus,SUBSTRING(ValueRow,5,2)-1) as Valid
				from 
		(
			select a.* from #tmp t
			join ';

set @SQLString3 = ' a on a.TI_ID = t.TI_ID and a.ChannelType = t.ChannelType and a.EventDate = t.MaxEventDateTime and a.DataSource_ID = t.DataSource_ID
		) as sourceTable
		unpivot 
		(
			 [Val] for [ValueRow] in 
			([Val_01],[Val_02],[Val_03],[Val_04],[Val_05],[Val_06],[Val_07],[Val_08],[Val_09],[Val_10],
			[Val_11],[Val_12],[Val_13],[Val_14],[Val_15],[Val_16],[Val_17],[Val_18],[Val_19],[Val_20],
			[Val_21],[Val_22],[Val_23],[Val_24],[Val_25],[Val_26],[Val_27],[Val_28],[Val_29],[Val_30],
			[Val_31],[Val_32],[Val_33],[Val_34],[Val_35],[Val_36],[Val_37],[Val_38],[Val_39],[Val_40],
			[Val_41],[Val_42],[Val_43],[Val_44],[Val_45],[Val_46],[Val_47],[Val_48])
		) unp
		) r	where r.number <= @rowlimit
		group by r.TI_ID, ChannelType
	) r
	join #tis ti on ti.TI_ID = r.TI_ID';

	declare @rowlimit tinyint;
	set @rowlimit = (@DiscreteType + 1);

	--Результат
	create table #result
	(
		[dt] [smalldatetime] NOT NULL,
		[ValidStatus] [bit] NULL,
		[Value] [float] NULL,
		[PS_ID] [int] null,
		[Coeff] [int] null,
		[Coef_tp] [float] null,
		[ContrReplaceStatus] [bigint] null,
		[ManualEnterStatus] [bigint] null,
		[TI_ID] int NOT NULL,
		[ChannelType] tinyint NOT NULL,
		[TIType] [tinyint] NULL
	)

	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #tis
	open t;
	FETCH NEXT FROM t into @titype
	WHILE @@FETCH_STATUS = 0
	BEGIN
		--Для того чтобы выбрать значения из источника с максимальным приоритетом
		create table #tmp
		(
		 TI_ID int,
		 ChannelType tinyint,
		 DataSource_ID tinyint,
		 MaxEventDateTime DateTime,
		 [Priority] tinyint,
		 PRIMARY KEY CLUSTERED (TI_ID, ChannelType desc)
				WITH (IGNORE_DUP_KEY = ON) 
		)

		if (@TIType < 10) set @tableName = 'ArchCalc_30_Virtual'
		else set @tableName = 'ArchCalcBit_30_Virtual_' + ltrim(str(@TIType - 10,2));


		set @sqlExecute = @SQLString1 + @tableName + @SQLString2 + @tableName + @SQLString3;
		EXEC sp_executesql @sqlExecute, N'@titype tinyint, @rowlimit tinyint', @TIType, @rowlimit

		--print @sqlExecute

		drop table #tmp
		FETCH NEXT FROM t into @titype
	end;
	CLOSE t
	DEALLOCATE t

	select * from #result;

drop table #tis;
go
   grant EXECUTE on usp2_ArchComm_ReadArrayLastHalfHours to [UserCalcService]
go



