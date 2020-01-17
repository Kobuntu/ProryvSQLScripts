if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Replace_30_Virtual_By_MainProfile')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Replace_30_Virtual_By_MainProfile
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Replace_Integrals_Virtual_By_MainProfile')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Replace_Integrals_Virtual_By_MainProfile
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_MakeMainValuesNotCorrect')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_MakeMainValuesNotCorrect
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_MakeValuesValid')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_MakeValuesValid
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_ManualChangeValidStatus')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_ManualChangeValidStatus
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Arch_30_DeleteMainValues')
          and type in ('P','PC'))
   drop procedure usp2_Arch_30_DeleteMainValues
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_DeleteMainValues')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_DeleteMainValues
go

--Обновляем тип
IF  NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'TiChannelDtRange' AND ss.name = N'dbo') BEGIN
-- Создаем тип, если его нет
	CREATE TYPE [dbo].[TiChannelDtRange] AS TABLE 
	(
		TI_ID int NOT NULL, 
		ChannelType tinyint NOT NULL,
		StartDateTime datetime NOT NULL,
		FinishDateTime datetime NOT NULL,
		DataSourceType tinyint NULL
	)

	grant EXECUTE on TYPE::TiChannelDtRange to [UserCalcService]
	grant EXECUTE on TYPE::TiChannelDtRange to [UserMaster61968Service]
end
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
--		Замещение данных расчетного профиля данными с основных таблиц
--		,сбрасываем флаг ручного ввода, сбрасываем флаг замещения КА
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Replace_30_Virtual_By_MainProfile]
	@tiArray TiChannelDtRange READONLY,
	@DispatchDateTime DateTime, --Время изменений c сервера
	@CUS_ID tinyint, --Номер ЦУС сформировавший эту запись
	@UserName varchar(64), --Имя пользователя
	@CommentString varchar(1000), --Строка комментариев
	@IsWs bit = 0 --Удалять данные в промежуточной таблице (часовой пояс сервера был отмена летнего времени)

as
begin
	declare @User_ID ABS_NUMBER_TYPE_2;

	set @User_ID = (select top 1 User_ID from Expl_Users where UserName = @UserName);

	if (@User_ID is null) begin
		select 'Не найден пользователь - ' + @UserName;
		return;
	end

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	
	--Пишем в CAL_x  -2 это признак отката на основной профиль
	select pvt.TI_ID, ChannelType, EventDate, DataSource_ID, TIType,
	[0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],
	[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410],
	--Маска для сброса 64bit поля корректности данных
	case when [0] is null then	dbo.sfclr_Utils_BitOperations (0,0,1) else 0 end |
	case when [30] is null then	dbo.sfclr_Utils_BitOperations (0,1,1) else 0 end |
	case when [60] is null then	dbo.sfclr_Utils_BitOperations (0,2,1) else 0 end |
	case when [90] is null then	dbo.sfclr_Utils_BitOperations (0,3,1) else 0 end |
	case when [120] is null then dbo.sfclr_Utils_BitOperations (0,4,1) else 0 end |
	case when [150] is null then dbo.sfclr_Utils_BitOperations (0,5,1) else 0 end |
	case when [180] is null then dbo.sfclr_Utils_BitOperations (0,6,1) else 0 end |
	case when [210] is null then dbo.sfclr_Utils_BitOperations (0,7,1) else 0 end |
	case when [240] is null then dbo.sfclr_Utils_BitOperations (0,8,1) else 0 end |
	case when [270] is null then dbo.sfclr_Utils_BitOperations (0,9,1) else 0 end |
	case when [300] is null then dbo.sfclr_Utils_BitOperations (0,10,1) else 0 end |
	case when [330] is null then dbo.sfclr_Utils_BitOperations (0,11,1) else 0 end |
	case when [360] is null then dbo.sfclr_Utils_BitOperations (0,12,1) else 0 end |
	case when [390] is null then dbo.sfclr_Utils_BitOperations (0,13,1) else 0 end |
	case when [420] is null then dbo.sfclr_Utils_BitOperations (0,14,1) else 0 end |
	case when [450] is null then dbo.sfclr_Utils_BitOperations (0,15,1) else 0 end |
	case when [480] is null then dbo.sfclr_Utils_BitOperations (0,16,1) else 0 end |
	case when [510] is null then dbo.sfclr_Utils_BitOperations (0,17,1) else 0 end |
	case when [540] is null then dbo.sfclr_Utils_BitOperations (0,18,1) else 0 end |
	case when [570] is null then dbo.sfclr_Utils_BitOperations (0,19,1) else 0 end |
	case when [600] is null then dbo.sfclr_Utils_BitOperations (0,20,1) else 0 end |
	case when [630] is null then dbo.sfclr_Utils_BitOperations (0,21,1) else 0 end |
	case when [660] is null then dbo.sfclr_Utils_BitOperations (0,22,1) else 0 end |
	case when [690] is null then dbo.sfclr_Utils_BitOperations (0,23,1) else 0 end |
	case when [720] is null then dbo.sfclr_Utils_BitOperations (0,24,1) else 0 end |
	case when [750] is null then dbo.sfclr_Utils_BitOperations (0,25,1) else 0 end |
	case when [780] is null then dbo.sfclr_Utils_BitOperations (0,26,1) else 0 end |
	case when [810] is null then dbo.sfclr_Utils_BitOperations (0,27,1) else 0 end |
	case when [840] is null then dbo.sfclr_Utils_BitOperations (0,28,1) else 0 end |
	case when [870] is null then dbo.sfclr_Utils_BitOperations (0,29,1) else 0 end |
	case when [900] is null then dbo.sfclr_Utils_BitOperations (0,30,1) else 0 end |
	case when [930] is null then dbo.sfclr_Utils_BitOperations (0,31,1) else 0 end |
	case when [960] is null then dbo.sfclr_Utils_BitOperations (0,32,1) else 0 end |
	case when [990] is null then dbo.sfclr_Utils_BitOperations (0,33,1) else 0 end |
	case when [1020] is null then dbo.sfclr_Utils_BitOperations (0,34,1) else 0 end |
	case when [1050] is null then dbo.sfclr_Utils_BitOperations (0,35,1) else 0 end |
	case when [1080] is null then dbo.sfclr_Utils_BitOperations (0,36,1) else 0 end |
	case when [1110] is null then dbo.sfclr_Utils_BitOperations (0,37,1) else 0 end |
	case when [1140] is null then dbo.sfclr_Utils_BitOperations (0,38,1) else 0 end |
	case when [1170] is null then dbo.sfclr_Utils_BitOperations (0,39,1) else 0 end |
	case when [1200] is null then dbo.sfclr_Utils_BitOperations (0,40,1) else 0 end |
	case when [1230] is null then dbo.sfclr_Utils_BitOperations (0,41,1) else 0 end |
	case when [1260] is null then dbo.sfclr_Utils_BitOperations (0,42,1) else 0 end |
	case when [1290] is null then dbo.sfclr_Utils_BitOperations (0,43,1) else 0 end |
	case when [1320] is null then dbo.sfclr_Utils_BitOperations (0,44,1) else 0 end |
	case when [1350] is null then dbo.sfclr_Utils_BitOperations (0,45,1) else 0 end |
	case when [1380] is null then dbo.sfclr_Utils_BitOperations (0,46,1) else 0 end |
	case when [1410] is null then dbo.sfclr_Utils_BitOperations (0,47,1) else 0 end  as ValidMask
	into #tmp 
	from 
	(
		select a.TI_ID, c.ChannelType, ds.DataSource_ID, DATEPART(hh, dt) * 60 + DATEPART(n, dt) as [minute], 
		cast(cast(dt as date) as datetime) as EventDate, 
		-2.0 as Data, TIType
		from  @tiArray a
		join Info_TI ti on ti.TI_ID = a.TI_ID
		cross apply usf2_Utils_HHByPeriod(a.StartDateTime, a.FinishDateTime) p
		cross apply dbo.usf2_ArchCalcChannelInversionStatus(a.TI_ID, a.ChannelType, a.StartDateTime, a.FinishDateTime, case when ti.AIATSCode=2 then 1 else 0 end) c
		join Expl_DataSource_List ds on ds.DataSourceType = a.DataSourceType
		where p.dt between c.StartChannelStatus and c.FinishChannelStatus
	) p
	pivot 
	(
	 AVG(Data) for [minute] in ([0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410])
	) as pvt

	--select * from #tmp

	BEGIN TRY  BEGIN TRANSACTION

		declare	@titype tinyint,@sqlstring NVARCHAR(4000), @n smallint, @ns nvarchar(2), @parmDefinition nvarchar(500), @tableName nvarchar(200);
		SET @parmDefinition = N'@titype tinyint,@DispatchDateTime DateTime'

		declare cc_ cursor LOCAL for select distinct titype	from #tmp
		open cc_;
		FETCH NEXT FROM cc_ into @titype
		WHILE @@FETCH_STATUS = 0
		BEGIN

			if (@TIType < 10) begin
				set @tableName = 'ArchCalc_30_Virtual';
			end else begin
				set @tableName = 'ArchCalcBit_30_Virtual_' + ltrim(str(@TIType - 10,2));
			end
			
			set @SQLString = N'update '+@tableName+' set 
			CAL_01 = case when [0] is null then a.CAL_01 else [0] end,
			CAL_02 = (case when [30]  is null then a.CAL_02 else [30] end),
			CAL_03 = (case when [60]  is null then a.CAL_03 else  [60]  end),
			CAL_04 = (case when [90]  is null then a.CAL_04 else  [90]  end),
			CAL_05 = (case when [120] is null then a.CAL_05 else  [120] end),
			CAL_06 = (case when [150] is null then a.CAL_06 else  [150] end),
			CAL_07 = (case when [180] is null then a.CAL_07 else  [180] end),
			CAL_08 = (case when [210] is null then a.CAL_08 else  [210] end),
			CAL_09 = (case when [240] is null then a.CAL_09 else  [240] end),
			CAL_10 = (case when [270]  is null then a.CAL_10 else [270]  end), 
			CAL_11 = (case when [300]  is null then a.CAL_11 else [300]  end),
			CAL_12 = (case when [330]  is null then a.CAL_12 else [330]  end),
			CAL_13 = (case when [360]  is null then a.CAL_13 else [360]  end),
			CAL_14 = (case when [390]  is null then a.CAL_14 else [390]  end),
			CAL_15 = (case when [420]  is null then a.CAL_15 else [420]  end),
			CAL_16 = (case when [450]  is null then a.CAL_16 else [450]  end),
			CAL_17 = (case when [480]  is null then a.CAL_17 else [480]  end),
			CAL_18 = (case when [510]  is null then a.CAL_18 else [510]  end),
			CAL_19 = (case when [540]  is null then a.CAL_19 else [540]  end),
			CAL_20 = (case when [570]  is null then a.CAL_20 else [570]  end), 
			CAL_21 = (case when [600]  is null then a.CAL_21 else [600]  end),
			CAL_22 = (case when [630]  is null then a.CAL_22 else [630]  end),
			CAL_23 = (case when [660]  is null then a.CAL_23 else [660]  end),
			CAL_24 = (case when [690]  is null then a.CAL_24 else [690]  end),
			CAL_25 = (case when [720]  is null then a.CAL_25 else [720]  end),
			CAL_26 = (case when [750]  is null then a.CAL_26 else [750]  end),
			CAL_27 = (case when [780]  is null then a.CAL_27 else [780]  end),
			CAL_28 = (case when [810]  is null then a.CAL_28 else [810]  end),
			CAL_29 = (case when [840]  is null then a.CAL_29 else [840]  end),
			CAL_30 = (case when [870]  is null then a.CAL_30 else [870]  end),
			CAL_31 = (case when [900]  is null then a.CAL_31 else [900]  end),
			CAL_32 = (case when [930]  is null then a.CAL_32 else [930]  end),
			CAL_33 = (case when [960]  is null then a.CAL_33 else [960]  end),
			CAL_34 = (case when [990]  is null then a.CAL_34 else [990]  end),
			CAL_35 = (case when [1020] is null then a.CAL_35 else [1020] end),
			CAL_36 = (case when [1050] is null then a.CAL_36 else [1050] end),
			CAL_37 = (case when [1080] is null then a.CAL_37 else [1080] end),
			CAL_38 = (case when [1110] is null then a.CAL_38 else [1110] end),
			CAL_39 = (case when [1140] is null then a.CAL_39 else [1140] end),
			CAL_40 = (case when [1170] is null then a.CAL_40 else [1170] end),
			CAL_41 = (case when [1200] is null then a.CAL_41 else [1200] end),
			CAL_42 = (case when [1230] is null then a.CAL_42 else [1230] end),
			CAL_43 = (case when [1260] is null then a.CAL_43 else [1260] end),
			CAL_44 = (case when [1290] is null then a.CAL_44 else [1290] end),
			CAL_45 = (case when [1320] is null then a.CAL_45 else [1320] end),
			CAL_46 = (case when [1350] is null then a.CAL_46 else [1350] end),
			CAL_47 = (case when [1380] is null then a.CAL_47 else [1380] end),
			CAL_48 = (case when [1410] is null then a.CAL_48 else [1410] end),
			DispatchDateTime = @DispatchDateTime,
			ManualEnterStatus = ISNULL(ManualEnterStatus,0) & ValidMask
			from '+@tableName+' a
			join #tmp t on a.TI_ID = t.TI_ID and a.EventDate = t.EventDate and 
			a.ChannelType = t.channeltype and a.DataSource_ID = t.DataSource_ID
			where t.titype = @titype';

			EXEC sp_executesql @sqlstring, @parmDefinition, @titype,@DispatchDateTime;

			--Удаляем данные из промежуточной таблицы отмены зимнего времени
			if (@IsWs = 1) begin
				update ArchBit_30_Values_WS set CAL_03 = -2, CAL_04 = -2 
				from ArchBit_30_Values_WS a
				join Info_TI ti on ti.TI_ID = a.TI_ID
				join @tiArray t on a.TI_ID = t.TI_ID and a.EventDate = '20141026' 
				and a.ChannelType = dbo.usf2_ReverseTariffChannel(0, t.ChannelType, ti.AIATSCode, ti.AOATSCode,ti.RIATSCode,ti.ROATSCode, t.TI_ID, '20141026', '20141026 23:59')
				join Expl_DataSource_List ds on ds.DataSourceType = t.DataSourceType and a.DataSource_ID = ds.DataSource_ID
				--Неоднозначности не допускается!
				where ti.TIType = @titype  and '20141026 01:00' between t.StartDateTime and t.FinishDateTime
			end

		FETCH NEXT FROM cc_ into @titype
		END
		CLOSE cc_
		DEALLOCATE cc_

		--Обновляем журнал
		MERGE Expl_User_Journal_Replace_30_Virtual as vv
		USING @tiArray as ti ON vv.TI_ID = ti.TI_ID and vv.ChannelType = ti.ChannelType and vv.EventDate = convert(smalldatetime, ti.StartDateTime) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime
		WHEN MATCHED THEN 
		UPDATE SET EventParam = 3/*признак ввода из основного профиля*/,
			 CommentString = @CommentString, CUS_ID = @CUS_ID, ZamerDateTime = ti.FinishDateTime
		WHEN NOT MATCHED THEN 
		INSERT ([TI_ID],[ChannelType],[EventDate],[User_ID],[EventDateTime],[EventParam],[CommentString],[ZamerDateTime],[CUS_ID])
		VALUES (ti.[TI_ID],ti.[ChannelType],convert(smalldatetime, ti.[StartDateTime]),@User_ID,@DispatchDateTime,3,@CommentString,ti.FinishDateTime,@CUS_ID);

		COMMIT	
	END TRY
	BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK 
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 16
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
end

go
   grant EXECUTE on usp2_ArchComm_Replace_30_Virtual_By_MainProfile to [UserCalcService]
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
--		Замещение данных расчетного профиля данными с основных таблиц
--		,сбрасываем флаг ручного ввода, сбрасываем флаг замещения КА
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_Replace_Integrals_Virtual_By_MainProfile]
	@tiArray TiChannelDtRange READONLY,
	@DispatchDateTime DateTime, --Время изменений c сервера
	@CUS_ID tinyint, --Номер ЦУС сформировавший эту запись
	@UserName varchar(64), --Имя пользователя
	@CommentString varchar(1000) --Строка комментариев
	

as
begin
	--Пишем в ManualEnterData  -2 это признак отката на основной профиль

	--Как то комментируем что это перенесены интегральные данные (TODO)
	set @CommentString = @CommentString + ' (интегральные данные)';

	select usf.TI_ID, dbo.usf2_ReverseTariffChannel(0, ChannelType, AIATSCode, AOATSCode,RIATSCode,ROATSCode, usf.TI_ID, usf.StartDateTime, usf.FinishDateTime) as ChannelType
	, DataSource_ID, TIType, usf.StartDateTime as sd, usf.FinishDateTime as fd
	into #ti
	from @tiArray usf
	join Info_TI ti on ti.TI_ID = usf.TI_ID
	join Expl_DataSource_List ds on isnull(usf.DataSourceType,4) = ds.DataSourceType

	declare @User_ID ABS_NUMBER_TYPE_2;

	set @User_ID = (select top 1 User_ID from Expl_Users where UserName = @UserName);

	if (@User_ID is null) begin
		select 'Не найден пользователь - ' + @UserName;
		return;
	end


	BEGIN TRY  BEGIN TRANSACTION

		declare	@titype tinyint, @parmDefinition nvarchar(500), @tableName nvarchar(200), @sqlExec nvarchar(4000);
		SET @parmDefinition = N'@titype tinyint'

		declare cc_ cursor LOCAL for select distinct titype	from #ti
		open cc_;
		FETCH NEXT FROM cc_ into @titype
		WHILE @@FETCH_STATUS = 0
		BEGIN

			if (@TIType < 10) begin
				set @tableName = 'ArchCalc_Integrals_Virtual';
			end else begin
				set @tableName = 'ArchCalcBit_Integrals_Virtual_' + ltrim(str(@TIType - 10,2));
			end

			set @sqlExec = 'update ' + @tableName + ' set ManualEnterData = -2
			from ' + @tableName+ ' a inner join #ti ti
			on a.TI_ID = ti.TI_ID and a.ChannelType = ti.ChannelType and a.DataSource_ID = ti.DataSource_ID 
				and a.EventDateTime between ti.sd and ti.fd and ti.TIType = @titype';
			
			EXEC sp_executesql @sqlExec, @parmDefinition, @titype


		FETCH NEXT FROM cc_ into @titype
		END
		CLOSE cc_
		DEALLOCATE cc_

		--Обновляем журнал
		MERGE Expl_User_Journal_Replace_30_Virtual as vv
		USING #ti as ti ON vv.TI_ID = ti.TI_ID and vv.ChannelType = ti.ChannelType and vv.EventDate = convert(smalldatetime, ti.sd) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime
		WHEN MATCHED THEN 
		UPDATE SET EventParam = 3/*признак ввода из основного профиля*/,
			 CommentString = @CommentString, CUS_ID = @CUS_ID, ZamerDateTime = ti.fd
		WHEN NOT MATCHED THEN 
		INSERT ([TI_ID],[ChannelType],[EventDate],[User_ID],[EventDateTime],[EventParam],[CommentString],[ZamerDateTime],[CUS_ID])
		VALUES (ti.[TI_ID],ti.[ChannelType],convert(smalldatetime, ti.sd),@User_ID,@DispatchDateTime,3,@CommentString,ti.fd,@CUS_ID);

		COMMIT	
	END TRY
	BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK 
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 10 
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
end

go
   grant EXECUTE on usp2_ArchComm_Replace_Integrals_Virtual_By_MainProfile to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2013
--
-- Описание:
--
--		Удаляем основные (нерасчетные) данные в таблице 30 минутного профиля (пишем null)
--
-- ======================================================================================
create proc [dbo].[usp2_Arch_30_DeleteMainValues]
	@TI_ID int,
	@DateStart datetime,
	@DateEnd datetime,
	@ChannelType tinyint,
	@DataSourceType tinyint, -- Источника
	@ClosedPeriod_ID uniqueidentifier = null, --Закрытый период, если не указан, читаем из таблицы расчетного профиля
	@User_ID ABS_NUMBER_TYPE_2,
	@DispatchDateTime DateTime,
	@IsWs bit, --Удалять данные в промежуточной таблице
	@CommentString varchar(max),
	@isRemoveProfile bit, -- Удалять ли профиль
	@isRemoveIntegrals bit -- Удалять ли показания
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Начальные параметры
declare @DataSource_ID tinyint, @TypeTable tinyint;
select @DataSource_ID = (select top 1 DataSource_ID from Expl_DataSource_List where DataSourceType = @DataSourceType);
select @TypeTable = (select top 1 TIType from Info_TI where TI_ID = @TI_ID);

--Временная таблица для замещения
if (@DataSource_ID is null) select 'Не указан источник. Или неправильно задан.';

declare
@SqlTable NVARCHAR(200), @sqlTableNumber NVARCHAR(3), @SQLString NVARCHAR(4000), @ParmDefinition NVARCHAR(1000), 
@integralTable NVARCHAR(200), @SQLIntegralsString NVARCHAR(4000), @SqlTableMain NVARCHAR(200), @SQLStringMain NVARCHAR(4000);

SET @ParmDefinition = N'@TI_ID int,@ClosedPeriod_ID uniqueidentifier,@DataSource_ID int, @DateStart datetime, @DateEnd datetime,@ChannelType tinyint'

if (@TypeTable > 10) set @sqlTableNumber = ltrim(str(@TypeTable - 10,2));

if (@ClosedPeriod_ID is not null) begin --Чтение закрытого периода
		if (@TypeTable > 10) begin 
			set @SqlTable = 'ArchCalcBit_30_Closed_' + @sqlTableNumber;
			set @integralTable = 'ArchCalcBit_Integrals_Closed_' + @sqlTableNumber;
		end	else begin 
			set @SqlTable = 'ArchCalc_30_Virtual_Closed';
			set @integralTable = 'ArchCalc_Integrals_Closed';
		end
	end else begin
		if (@TypeTable > 10) begin 
			set @SqlTable = 'ArchCalcBit_30_Virtual_' + @sqlTableNumber;
			set @integralTable = 'ArchCalcBit_Integrals_Virtual_' + @sqlTableNumber;
			set @SqlTableMain = 'ArchBit_30_Values_' + @sqlTableNumber;
		end	else begin 
			set @SqlTable = 'ArchCalc_30_Virtual';
			set @integralTable = 'ArchCalc_Integrals_Virtual';
			set @SqlTableMain = 'ArchComm_30_Values';
		end
	end

if (@isRemoveProfile = 1) begin
--Удаляем профиль

	select EventDate, ChannelType, TIType,
	[0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],
	[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410],
	--Маска для сброса 64bit поля корректности данных
	case when [0] is null then	dbo.sfclr_Utils_BitOperations (0,0,1) else 0 end |
		case when [30] is null then	dbo.sfclr_Utils_BitOperations (0,1,1) else 0 end |
		case when [60] is null then	dbo.sfclr_Utils_BitOperations (0,2,1) else 0 end |
		case when [90] is null then	dbo.sfclr_Utils_BitOperations (0,3,1) else 0 end |
		case when [120] is null then dbo.sfclr_Utils_BitOperations (0,4,1) else 0 end |
		case when [150] is null then dbo.sfclr_Utils_BitOperations (0,5,1) else 0 end |
		case when [180] is null then dbo.sfclr_Utils_BitOperations (0,6,1) else 0 end |
		case when [210] is null then dbo.sfclr_Utils_BitOperations (0,7,1) else 0 end |
		case when [240] is null then dbo.sfclr_Utils_BitOperations (0,8,1) else 0 end |
		case when [270] is null then dbo.sfclr_Utils_BitOperations (0,9,1) else 0 end |
		case when [300] is null then dbo.sfclr_Utils_BitOperations (0,10,1) else 0 end |
		case when [330] is null then dbo.sfclr_Utils_BitOperations (0,11,1) else 0 end |
		case when [360] is null then dbo.sfclr_Utils_BitOperations (0,12,1) else 0 end |
		case when [390] is null then dbo.sfclr_Utils_BitOperations (0,13,1) else 0 end |
		case when [420] is null then dbo.sfclr_Utils_BitOperations (0,14,1) else 0 end |
		case when [450] is null then dbo.sfclr_Utils_BitOperations (0,15,1) else 0 end |
		case when [480] is null then dbo.sfclr_Utils_BitOperations (0,16,1) else 0 end |
		case when [510] is null then dbo.sfclr_Utils_BitOperations (0,17,1) else 0 end |
		case when [540] is null then dbo.sfclr_Utils_BitOperations (0,18,1) else 0 end |
		case when [570] is null then dbo.sfclr_Utils_BitOperations (0,19,1) else 0 end |
		case when [600] is null then dbo.sfclr_Utils_BitOperations (0,20,1) else 0 end |
		case when [630] is null then dbo.sfclr_Utils_BitOperations (0,21,1) else 0 end |
		case when [660] is null then dbo.sfclr_Utils_BitOperations (0,22,1) else 0 end |
		case when [690] is null then dbo.sfclr_Utils_BitOperations (0,23,1) else 0 end |
		case when [720] is null then dbo.sfclr_Utils_BitOperations (0,24,1) else 0 end |
		case when [750] is null then dbo.sfclr_Utils_BitOperations (0,25,1) else 0 end |
		case when [780] is null then dbo.sfclr_Utils_BitOperations (0,26,1) else 0 end |
		case when [810] is null then dbo.sfclr_Utils_BitOperations (0,27,1) else 0 end |
		case when [840] is null then dbo.sfclr_Utils_BitOperations (0,28,1) else 0 end |
		case when [870] is null then dbo.sfclr_Utils_BitOperations (0,29,1) else 0 end |
		case when [900] is null then dbo.sfclr_Utils_BitOperations (0,30,1) else 0 end |
		case when [930] is null then dbo.sfclr_Utils_BitOperations (0,31,1) else 0 end |
		case when [960] is null then dbo.sfclr_Utils_BitOperations (0,32,1) else 0 end |
		case when [990] is null then dbo.sfclr_Utils_BitOperations (0,33,1) else 0 end |
		case when [1020] is null then dbo.sfclr_Utils_BitOperations (0,34,1) else 0 end |
		case when [1050] is null then dbo.sfclr_Utils_BitOperations (0,35,1) else 0 end |
		case when [1080] is null then dbo.sfclr_Utils_BitOperations (0,36,1) else 0 end |
		case when [1110] is null then dbo.sfclr_Utils_BitOperations (0,37,1) else 0 end |
		case when [1140] is null then dbo.sfclr_Utils_BitOperations (0,38,1) else 0 end |
		case when [1170] is null then dbo.sfclr_Utils_BitOperations (0,39,1) else 0 end |
		case when [1200] is null then dbo.sfclr_Utils_BitOperations (0,40,1) else 0 end |
		case when [1230] is null then dbo.sfclr_Utils_BitOperations (0,41,1) else 0 end |
		case when [1260] is null then dbo.sfclr_Utils_BitOperations (0,42,1) else 0 end |
		case when [1290] is null then dbo.sfclr_Utils_BitOperations (0,43,1) else 0 end |
		case when [1320] is null then dbo.sfclr_Utils_BitOperations (0,44,1) else 0 end |
		case when [1350] is null then dbo.sfclr_Utils_BitOperations (0,45,1) else 0 end |
		case when [1380] is null then dbo.sfclr_Utils_BitOperations (0,46,1) else 0 end |
		case when [1410] is null then dbo.sfclr_Utils_BitOperations (0,47,1) else 0 end  as ValidMask
	into #tmp 
	from 
	(
		select DATEPART(hh, dt) * 60 + DATEPART(n, dt) as [minute], cast(floor(cast(dt as float)) as DateTime) as EventDate, -1. as Data, c.ChannelType, TIType
		from Info_TI ti 
		cross apply usf2_Utils_HHByPeriod(@DateStart, @DateEnd) p
		cross apply dbo.usf2_ArchCalcChannelInversionStatus(@TI_ID, @ChannelType, @DateStart, @DateEnd, case when ti.AIATSCode=2 then 1 else 0 end) c
		where ti.TI_ID = @TI_ID and p.dt between dbo.usf2_Utils_DateTimeRoundToHalfHour(c.StartChannelStatus, 1) and c.FinishChannelStatus
	) p
	pivot 
	(
	 AVG(Data) for [minute] in ([0],[30],[60],[90],[120],[150],[180],[210],[240],[270],[300],[330],[360],[390],[420],[450],[480],[510],[540],[570],[600],[630],[660],[690],[720],[750],[780],[810],[840],[870],[900],[930],[960],[990],[1020],[1050],[1080],[1110],[1140],[1170],[1200],[1230],[1260],[1290],[1320],[1350],[1380],[1410])
	) as pvt

	--Теперь обновляем таблицу (пишем null)
	
	set @SQLString = N'update '+@SqlTable+' set 
	VAL_01 = case when [0] is null then a.VAL_01 else null end,
	VAL_02 = (case when [30]  is null then a.VAL_02 else null end),
	VAL_03 = (case when [60]  is null then a.VAL_03 else null end),
	VAL_04 = (case when [90]  is null then a.VAL_04 else null end),
	VAL_05 = (case when [120] is null then a.VAL_05 else null end),
	VAL_06 = (case when [150] is null then a.VAL_06 else null end),
	VAL_07 = (case when [180] is null then a.VAL_07 else null end),
	VAL_08 = (case when [210] is null then a.VAL_08 else null end),
	VAL_09 = (case when [240] is null then a.VAL_09 else null end),
	VAL_10 = (case when [270]  is null then a.VAL_10 else null end), 
	VAL_11 = (case when [300]  is null then a.VAL_11 else null end),
	VAL_12 = (case when [330]  is null then a.VAL_12 else null end),
	VAL_13 = (case when [360]  is null then a.VAL_13 else null end),
	VAL_14 = (case when [390]  is null then a.VAL_14 else null end),
	VAL_15 = (case when [420]  is null then a.VAL_15 else null end),
	VAL_16 = (case when [450]  is null then a.VAL_16 else null end),
	VAL_17 = (case when [480]  is null then a.VAL_17 else null end),
	VAL_18 = (case when [510]  is null then a.VAL_18 else null end),
	VAL_19 = (case when [540]  is null then a.VAL_19 else null end),
	VAL_20 = (case when [570]  is null then a.VAL_20 else null end), 
	VAL_21 = (case when [600]  is null then a.VAL_21 else null end),
	VAL_22 = (case when [630]  is null then a.VAL_22 else null end),
	VAL_23 = (case when [660]  is null then a.VAL_23 else null end),
	VAL_24 = (case when [690]  is null then a.VAL_24 else null end),
	VAL_25 = (case when [720]  is null then a.VAL_25 else null end),
	VAL_26 = (case when [750]  is null then a.VAL_26 else null end),
	VAL_27 = (case when [780]  is null then a.VAL_27 else null end),
	VAL_28 = (case when [810]  is null then a.VAL_28 else null end),
	VAL_29 = (case when [840]  is null then a.VAL_29 else null end),
	VAL_30 = (case when [870]  is null then a.VAL_30 else null end),
	VAL_31 = (case when [900]  is null then a.VAL_31 else null end),
	VAL_32 = (case when [930]  is null then a.VAL_32 else null end),
	VAL_33 = (case when [960]  is null then a.VAL_33 else null end),
	VAL_34 = (case when [990]  is null then a.VAL_34 else null end),
	VAL_35 = (case when [1020] is null then a.VAL_35 else null end),
	VAL_36 = (case when [1050] is null then a.VAL_36 else null end),
	VAL_37 = (case when [1080] is null then a.VAL_37 else null end),
	VAL_38 = (case when [1110] is null then a.VAL_38 else null end),
	VAL_39 = (case when [1140] is null then a.VAL_39 else null end),
	VAL_40 = (case when [1170] is null then a.VAL_40 else null end),
	VAL_41 = (case when [1200] is null then a.VAL_41 else null end),
	VAL_42 = (case when [1230] is null then a.VAL_42 else null end),
	VAL_43 = (case when [1260] is null then a.VAL_43 else null end),
	VAL_44 = (case when [1290] is null then a.VAL_44 else null end),
	VAL_45 = (case when [1320] is null then a.VAL_45 else null end),
	VAL_46 = (case when [1350] is null then a.VAL_46 else null end),
	VAL_47 = (case when [1380] is null then a.VAL_47 else null end),
	VAL_48 = (case when [1410] is null then a.VAL_48 else null end),
	ValidStatus = a.ValidStatus & ValidMask
	from '+@SqlTable+' a
	join #tmp t on a.TI_ID=@TI_ID and a.EventDate=t.EventDate and a.ChannelType=t.ChannelType and a.DataSource_ID=@DataSource_ID' + case when @ClosedPeriod_ID is not null then ' and ClosedPeriod_ID = @ClosedPeriod_ID' else '' end;

	if (@DataSourceType = 0 and @SqlTableMain is not null) begin
	--Удаление данных из таблицы сбора
		set @SQLStringMain = N'update '+@SqlTableMain+' set 
		VAL_01 = case when [0] is null then a.VAL_01 else null end,
		VAL_02 = (case when [30]  is null then a.VAL_02 else null end),
		VAL_03 = (case when [60]  is null then a.VAL_03 else null end),
		VAL_04 = (case when [90]  is null then a.VAL_04 else null end),
		VAL_05 = (case when [120] is null then a.VAL_05 else null end),
		VAL_06 = (case when [150] is null then a.VAL_06 else null end),
		VAL_07 = (case when [180] is null then a.VAL_07 else null end),
		VAL_08 = (case when [210] is null then a.VAL_08 else null end),
		VAL_09 = (case when [240] is null then a.VAL_09 else null end),
		VAL_10 = (case when [270]  is null then a.VAL_10 else null end), 
		VAL_11 = (case when [300]  is null then a.VAL_11 else null end),
		VAL_12 = (case when [330]  is null then a.VAL_12 else null end),
		VAL_13 = (case when [360]  is null then a.VAL_13 else null end),
		VAL_14 = (case when [390]  is null then a.VAL_14 else null end),
		VAL_15 = (case when [420]  is null then a.VAL_15 else null end),
		VAL_16 = (case when [450]  is null then a.VAL_16 else null end),
		VAL_17 = (case when [480]  is null then a.VAL_17 else null end),
		VAL_18 = (case when [510]  is null then a.VAL_18 else null end),
		VAL_19 = (case when [540]  is null then a.VAL_19 else null end),
		VAL_20 = (case when [570]  is null then a.VAL_20 else null end), 
		VAL_21 = (case when [600]  is null then a.VAL_21 else null end),
		VAL_22 = (case when [630]  is null then a.VAL_22 else null end),
		VAL_23 = (case when [660]  is null then a.VAL_23 else null end),
		VAL_24 = (case when [690]  is null then a.VAL_24 else null end),
		VAL_25 = (case when [720]  is null then a.VAL_25 else null end),
		VAL_26 = (case when [750]  is null then a.VAL_26 else null end),
		VAL_27 = (case when [780]  is null then a.VAL_27 else null end),
		VAL_28 = (case when [810]  is null then a.VAL_28 else null end),
		VAL_29 = (case when [840]  is null then a.VAL_29 else null end),
		VAL_30 = (case when [870]  is null then a.VAL_30 else null end),
		VAL_31 = (case when [900]  is null then a.VAL_31 else null end),
		VAL_32 = (case when [930]  is null then a.VAL_32 else null end),
		VAL_33 = (case when [960]  is null then a.VAL_33 else null end),
		VAL_34 = (case when [990]  is null then a.VAL_34 else null end),
		VAL_35 = (case when [1020] is null then a.VAL_35 else null end),
		VAL_36 = (case when [1050] is null then a.VAL_36 else null end),
		VAL_37 = (case when [1080] is null then a.VAL_37 else null end),
		VAL_38 = (case when [1110] is null then a.VAL_38 else null end),
		VAL_39 = (case when [1140] is null then a.VAL_39 else null end),
		VAL_40 = (case when [1170] is null then a.VAL_40 else null end),
		VAL_41 = (case when [1200] is null then a.VAL_41 else null end),
		VAL_42 = (case when [1230] is null then a.VAL_42 else null end),
		VAL_43 = (case when [1260] is null then a.VAL_43 else null end),
		VAL_44 = (case when [1290] is null then a.VAL_44 else null end),
		VAL_45 = (case when [1320] is null then a.VAL_45 else null end),
		VAL_46 = (case when [1350] is null then a.VAL_46 else null end),
		VAL_47 = (case when [1380] is null then a.VAL_47 else null end),
		VAL_48 = (case when [1410] is null then a.VAL_48 else null end),
		ValidStatus = a.ValidStatus & ValidMask
		from '+@SqlTableMain+' a
		join #tmp t on a.TI_ID = @TI_ID and a.EventDate = t.EventDate and a.ChannelType = t.ChannelType';
	end
	-------Удаление профиля
end
--Удаление интегралов удаляем только из таблицы сбора, после пересбора обновлятся и виртуальные таблицы
if @isRemoveIntegrals = 1 begin
	--set @removeIntegrals = case when (select top 1 DataSourceType from Expl_DataSource_List where DataSource_ID = @DataSource_ID) = 0 then 1 else 0 end;
	--Удаляем интегралы только если указана таблица автоматизированного сбора
	--if (@removeIntegrals = 1) --Странно, но теперь это ограничение не нужно, можно удалять данные из любого источника
	set @SQLIntegralsString = N'delete i from '+@integralTable+' i join Info_TI ti on ti.TI_ID = i.TI_ID
		cross apply dbo.usf2_ArchCalcChannelInversionStatus(@TI_ID, @ChannelType, @DateStart, @DateEnd, case when ti.AIATSCode=2 then 1 else 0 end) c
		where i.TI_ID = @TI_ID and i.EventDateTime between @DateStart and @DateEnd and (i.ChannelType % 10) = c.ChannelType and i.EventDateTime between dbo.usf2_Utils_DateTimeRoundToHalfHour(c.StartChannelStatus, 1) 
		and c.FinishChannelStatus and i.DataSource_ID=@DataSource_ID'
end

--select @SQLIntegralsString

BEGIN TRY  BEGIN TRANSACTION
	
	if (@isRemoveProfile = 1) begin
		--Удаляем данные из расчетной таблицы
		EXEC sp_executesql @SQLString,@ParmDefinition,@TI_ID,@ClosedPeriod_ID,@DataSource_ID, @DateStart, @DateEnd,@ChannelType;
		--select * from #tmp

		--Удаляем только открытые данные
		if (@SQLStringMain is not null) begin
		--Удаляем данные из таблицы сбора
			--select @SQLStringMain
			EXEC sp_executesql @SQLStringMain,@ParmDefinition,@TI_ID,@ClosedPeriod_ID,@DataSource_ID,@DateStart,@DateEnd,@ChannelType;
		end

		--Удаляем данные из промежуточной таблицы отмены зимнего времени
		if (@IsWs = 1 and '20141026 01:00' between @DateStart and @DateEnd) begin
			update ArchBit_30_Values_WS set VAL_03 = null, VAL_04 = null 
			where TI_ID = @TI_ID and EventDate = '20141026' and ChannelType = @ChannelType 
			and DataSource_ID = @DataSource_ID
		end
	end

	--Удаляем данные из таблицы сбора интегралов
	if @isRemoveIntegrals = 1 begin
		EXEC sp_executesql @SQLIntegralsString,@ParmDefinition,@TI_ID,@ClosedPeriod_ID,@DataSource_ID, @DateStart, @DateEnd,@ChannelType;
		--select @SQLIntegralsString, @ParmDefinition,@TI_ID,@ClosedPeriod_ID,@DataSource_ID, @DateStart, @DateEnd,@ChannelType;
	end

	declare @EventParam tinyint, @CUS_ID CUS_ID_TYPE;

	set @CUS_ID = (select top 1 CUS_ID from Dict_CUS);

	if (@isRemoveProfile = 1 AND @isRemoveIntegrals = 1) set @EventParam = 130; -- Признак удаления всех данных
	else if (@isRemoveProfile = 1) set @EventParam = 134; -- Признак удаления только профиля
	else if (@isRemoveIntegrals = 1) set @EventParam = 135; -- Признак удаления только интегралов
	
	if not exists (select TI_ID,ChannelType,EventDate,[User_ID],EventDateTime from dbo.Expl_User_Journal_Replace_30_Virtual vv 
	where vv.ti_id = @ti_id and vv.ChannelType = @ChannelType and vv.EventDate = @DateStart and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime) begin 
		insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, CommentString, CUS_ID, ZamerDateTime)
		select @ti_ID, @ChannelType,@DateStart as EventDate, @User_ID, @DispatchDateTime, @EventParam, @CommentString, @CUS_ID, @DateEnd as ZamerDateTime
	end else begin 
		update dbo.Expl_User_Journal_Replace_30_Virtual
		set  ti_ID = @ti_ID, ChannelType = @ChannelType,EventDate = @DateStart, [User_ID] = @User_ID, EventDateTime = @DispatchDateTime, EventParam = @EventParam, 
		CommentString = @CommentString, CUS_ID = @CUS_ID, ZamerDateTime = @DateEnd 
		where Expl_User_Journal_Replace_30_Virtual.ti_id = @ti_id and Expl_User_Journal_Replace_30_Virtual.ChannelType = @ChannelType and Expl_User_Journal_Replace_30_Virtual.EventDate = @DateStart
			and Expl_User_Journal_Replace_30_Virtual.[User_ID] = @User_ID and Expl_User_Journal_Replace_30_Virtual.EventDateTime = @DispatchDateTime 
	end
COMMIT
	END TRY	
	BEGIN CATCH
		if (@isRemoveProfile = 1) begin
			drop table #TMP
		end

		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 -- На верху нужен exception
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
end

go
   grant EXECUTE on usp2_Arch_30_DeleteMainValues to [UserCalcService]
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		июнь, 2016
--
-- Описание:
--
--		Удаляем основные (нерасчетные) данные в таблице 30 минутного профиля (пишем null)
--		по группе точек 
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_DeleteMainValues]

	@TIArray TiChannelDtRange readonly, --ТИ, канал, признак стороны, идентификатор закрытого периода, если необходимо читать из закрытого периода
	@ClosedPeriod_ID uniqueidentifier = null, --Закрытый период, если не указан, читаем из таблицы расчетного профиля
	@User_ID ABS_NUMBER_TYPE_2,
	@DispatchDateTime DateTime,
	@IsWs bit, --Удалять данные в промежуточной таблице
	@CommentString varchar(max),
	@isRemoveProfile bit, -- Удалять ли профиль
	@isRemoveIntegrals bit -- Удалять ли показания

as

begin

if (@isRemoveIntegrals = 0 AND @isRemoveProfile = 0) return;

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
	@TI_ID int,
	@DateStart datetime,
	@DateEnd datetime,
	@ChannelType tinyint,
	@DataSourceType tinyint
	

	--Удаляем по одной ТИ
declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select TI_ID,ChannelType,StartDateTime,FinishDateTime,DataSourceType from @TIArray
  open t;
	FETCH NEXT FROM t into @TI_ID, @ChannelType, @DateStart, @DateEnd, @DataSourceType
	WHILE @@FETCH_STATUS = 0
	BEGIN

		exec usp2_Arch_30_DeleteMainValues @TI_ID,@DateStart,@DateEnd,@ChannelType,@DataSourceType,@ClosedPeriod_ID, 
			@User_ID,@DispatchDateTime,@IsWs,@CommentString,@isRemoveProfile,@isRemoveIntegrals 
		
	FETCH NEXT FROM t into @TI_ID, @ChannelType, @DateStart, @DateEnd, @DataSourceType
	end;
	CLOSE t
	DEALLOCATE t
end

go
   grant EXECUTE on usp2_ArchComm_DeleteMainValues to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
--		Меняем статус достоверности получасовых данных
--
-- ======================================================================================

create proc [dbo].[usp2_ArchComm_ManualChangeValidStatus]
	@tiArray TiChannelDtRange READONLY,
	@isCorrect bit, --1 - достоверные, 0 - недостоверные
	@DispatchDateTime DateTime, --Время изменений c сервера
	@CUS_ID tinyint, --Номер ЦУС сформировавший эту запись
	@UserName varchar(64), --Имя пользователя
	@CommentString varchar(1000) --Строка комментариев
	

as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	
	select usf.TI_ID, dbo.usf2_ReverseTariffChannel(0, ChannelType, AIATSCode, AOATSCode,RIATSCode,ROATSCode, usf.TI_ID, usf.StartDateTime, usf.FinishDateTime) as ChannelType
	,ISNULL(DataSource_ID,(select top 1 DataSource_ID from Expl_DataSource_PriorityList where Year = Year(hp.dt) and Month = Month(hp.dt) order by Priority desc)) as DataSource_ID,
	dbo.usf2_Utils_DateTimeRoundToHalfHour([StartDateTime], 1) as StartDateTime,
	dbo.usf2_Utils_DateTimeRoundToHalfHour([FinishDateTime], 1) as FinishDateTime, TIType,
	dbo.usf2_Utils_BitRange(case when dt = cast(FLOOR(cast(usf.StartDateTime as float)) as DateTime) then round((cast(dbo.usf2_Utils_DateTimeRoundToHalfHour([StartDateTime], 1) as float) - FLOOR(cast(usf.StartDateTime as float))) * 48, 3) else 0 end,
		 case when dt=cast(FLOOR(cast(usf.FinishDateTime as float)) as DateTime) then round((cast(dbo.usf2_Utils_DateTimeRoundToHalfHour([FinishDateTime], 1) as float) - FLOOR(cast(usf.FinishDateTime as float))) * 48, 3) else 47 end,0) as bitRange, 
	hp.dt
	into #ti
	from @tiArray usf
	cross apply usf2_Utils_HalfHoursByPeriod(cast(FLOOR(cast(usf.StartDateTime as float)) as DateTime), cast(FLOOR(cast(usf.FinishDateTime as float)) as DateTime)) hp 
	join Info_TI ti on ti.TI_ID = usf.TI_ID
	join Expl_DataSource_List ds on isnull(usf.DataSourceType,4) = ds.DataSourceType

	declare @User_ID ABS_NUMBER_TYPE_2;

	set @User_ID = (select top 1 User_ID from Expl_Users where UserName = @UserName);

	if (@User_ID is null) begin
		select 'Не найден пользователь - ' + @UserName;
		return;
	end
	
	declare @manualStatusString nvarchar(170);
	if (@isCorrect = 1) set @manualStatusString = '~s.bitRange & ISNULL(a.ManualValidStatus,0)';
	else set @manualStatusString = 's.bitRange | (ISNULL(a.ManualValidStatus,0))'

	declare @repTable table (dt DateTime, ManualValidStatus bigint, ti_id int, channelType tinyint, DataSource_ID tinyint)

	BEGIN TRY  BEGIN TRANSACTION

		declare	@titype tinyint,@sqlstring1 VARCHAR(max), @sqlstring2 VARCHAR(max), @sqlstring3 VARCHAR(max), @n smallint, @ns nvarchar(2), 
		@tableNameVirtual nvarchar(200),@tableNameMain nvarchar(200);
		
		declare cc_ cursor LOCAL for select distinct titype	from #ti
		open cc_;
		FETCH NEXT FROM cc_ into @titype
		WHILE @@FETCH_STATUS = 0
		BEGIN

			if (@TIType > 10) begin
				set @tableNameVirtual = 'ArchCalcBit_30_Virtual_' + ltrim(str(@TIType - 10,2));
			end else begin 
				set @tableNameVirtual = 'ArchCalc_30_Virtual';
			end

			--ti.TItype=' + str(@titype,3)

			set @sqlstring1 = N'MERGE ' + @tableNameVirtual+' a using #ti s 
				on a.TI_ID=s.TI_ID and a.ChannelType=s.ChannelType and a.EventDate=s.dt and a.DataSource_ID=s.DataSource_ID and s.titype = ' + str(@titype,3) +
			 ' WHEN MATCHED THEN UPDATE set ManualValidStatus=' + @manualStatusString + ', ManualEnterStatus=s.bitRange | (ISNULL(a.ManualEnterStatus,0)),
			 CAL_01=case when (bitRange & cast(1 as bigint)) =				1				then case when CAL_01 is null and VAL_01 is null then 0 when CAL_01 < 0 then NULL else CAL_01 end else CAL_01 end,
			 CAL_02=case when (bitRange & cast(2 as bigint)) =				2				then case when CAL_02 is null and VAL_02 is null then 0 when CAL_02 < 0 then NULL else CAL_02 end else CAL_02 end,
			 CAL_03=case when (bitRange & cast(4 as bigint)) =				4				then case when CAL_03 is null and VAL_03 is null then 0 when CAL_03 < 0 then NULL else CAL_03 end else CAL_03 end,
			 CAL_04=case when (bitRange & cast(8 as bigint)) =				8				then case when CAL_04 is null and VAL_04 is null then 0 when CAL_04 < 0 then NULL else CAL_04 end else CAL_04 end,
			 CAL_05=case when (bitRange & cast(16 as bigint)) =				16				then case when CAL_05 is null and VAL_05 is null then 0 when CAL_05 < 0 then NULL else CAL_05 end else CAL_05 end,
			 CAL_06=case when (bitRange & cast(32 as bigint)) =				32				then case when CAL_06 is null and VAL_06 is null then 0 when CAL_06 < 0 then NULL else CAL_06 end else CAL_06 end,
			 CAL_07=case when (bitRange & cast(64 as bigint)) =				64				then case when CAL_07 is null and VAL_07 is null then 0 when CAL_07 < 0 then NULL else CAL_07 end else CAL_07 end,
			 CAL_08=case when (bitRange & cast(128 as bigint)) =			128				then case when CAL_08 is null and VAL_08 is null then 0 when CAL_08 < 0 then NULL else CAL_08 end else CAL_08 end,
			 CAL_09=case when (bitRange & cast(256 as bigint)) =			256				then case when CAL_09 is null and VAL_09 is null then 0 when CAL_09 < 0 then NULL else CAL_09 end else CAL_09 end,
			 CAL_10=case when (bitRange & cast(512 as bigint)) =			512				then case when CAL_10 is null and VAL_10 is null then 0 when CAL_10 < 0 then NULL else CAL_10 end else CAL_10 end,
			 CAL_11=case when (bitRange & cast(1024 as bigint)) =			1024			then case when CAL_11 is null and VAL_11 is null then 0 when CAL_11 < 0 then NULL else CAL_11 end else CAL_11 end,
			 CAL_12=case when (bitRange & cast(2048 as bigint)) =			2048			then case when CAL_12 is null and VAL_12 is null then 0 when CAL_12 < 0 then NULL else CAL_12 end else CAL_12 end,
			 CAL_13=case when (bitRange & cast(4096 as bigint)) =			4096			then case when CAL_13 is null and VAL_13 is null then 0 when CAL_13 < 0 then NULL else CAL_13 end else CAL_13 end,
			 CAL_14=case when (bitRange & cast(8192 as bigint)) =			8192			then case when CAL_14 is null and VAL_14 is null then 0 when CAL_14 < 0 then NULL else CAL_14 end else CAL_14 end,
			 CAL_15=case when (bitRange & cast(16384 as bigint)) =			16384			then case when CAL_15 is null and VAL_15 is null then 0 when CAL_15 < 0 then NULL else CAL_15 end else CAL_15 end,
			 CAL_16=case when (bitRange & cast(32768 as bigint)) =			32768			then case when CAL_16 is null and VAL_16 is null then 0 when CAL_16 < 0 then NULL else CAL_16 end else CAL_16 end,
			 CAL_17=case when (bitRange & cast(65536 as bigint)) =			65536			then case when CAL_17 is null and VAL_17 is null then 0 when CAL_17 < 0 then NULL else CAL_17 end else CAL_17 end,'; set @sqlstring2 = N'
			 CAL_18=case when (bitRange & cast(131072 as bigint)) =			131072			then case when CAL_18 is null and VAL_18 is null then 0 when CAL_18 < 0 then NULL else CAL_18 end else CAL_18 end,
			 CAL_19=case when (bitRange & cast(262144 as bigint)) =			262144			then case when CAL_19 is null and VAL_19 is null then 0 when CAL_19 < 0 then NULL else CAL_19 end else CAL_19 end,
			 CAL_20=case when (bitRange & cast(524288 as bigint)) =			524288			then case when CAL_20 is null and VAL_20 is null then 0 when CAL_20 < 0 then NULL else CAL_20 end else CAL_20 end,
			 CAL_21=case when (bitRange & cast(1048576 as bigint)) =		1048576			then case when CAL_21 is null and VAL_21 is null then 0 when CAL_21 < 0 then NULL else CAL_21 end else CAL_21 end,
			 CAL_22=case when (bitRange & cast(2097152 as bigint)) =		2097152			then case when CAL_22 is null and VAL_22 is null then 0 when CAL_22 < 0 then NULL else CAL_22 end else CAL_22 end,
			 CAL_23=case when (bitRange & cast(4194304 as bigint)) =		4194304			then case when CAL_23 is null and VAL_23 is null then 0 when CAL_23 < 0 then NULL else CAL_23 end else CAL_23 end,
			 CAL_24=case when (bitRange & cast(8388608 as bigint)) =		8388608			then case when CAL_24 is null and VAL_24 is null then 0 when CAL_24 < 0 then NULL else CAL_24 end else CAL_24 end,
			 CAL_25=case when (bitRange & cast(16777216 as bigint)) =		16777216		then case when CAL_25 is null and VAL_25 is null then 0 when CAL_25 < 0 then NULL else CAL_25 end else CAL_25 end,
			 CAL_26=case when (bitRange & cast(33554432 as bigint)) =		33554432		then case when CAL_26 is null and VAL_26 is null then 0 when CAL_26 < 0 then NULL else CAL_26 end else CAL_26 end,
			 CAL_27=case when (bitRange & cast(67108864 as bigint)) =		67108864		then case when CAL_27 is null and VAL_27 is null then 0 when CAL_27 < 0 then NULL else CAL_27 end else CAL_27 end,
			 CAL_28=case when (bitRange & cast(134217728 as bigint)) =		134217728		then case when CAL_28 is null and VAL_28 is null then 0 when CAL_28 < 0 then NULL else CAL_28 end else CAL_28 end,
			 CAL_29=case when (bitRange & cast(268435456 as bigint)) =		268435456		then case when CAL_29 is null and VAL_29 is null then 0 when CAL_29 < 0 then NULL else CAL_29 end else CAL_29 end,
			 CAL_30=case when (bitRange & cast(536870912 as bigint)) =		536870912		then case when CAL_30 is null and VAL_30 is null then 0 when CAL_30 < 0 then NULL else CAL_30 end else CAL_30 end,
			 CAL_31=case when (bitRange & cast(1073741824 as bigint)) =		1073741824		then case when CAL_31 is null and VAL_31 is null then 0 when CAL_31 < 0 then NULL else CAL_31 end else CAL_31 end,
			 CAL_32=case when (bitRange & cast(2147483648 as bigint)) =		2147483648		then case when CAL_32 is null and VAL_32 is null then 0 when CAL_32 < 0 then NULL else CAL_32 end else CAL_32 end,
			 CAL_33=case when (bitRange & cast(4294967296 as bigint)) =		4294967296		then case when CAL_33 is null and VAL_33 is null then 0 when CAL_33 < 0 then NULL else CAL_33 end else CAL_33 end,
			 CAL_34=case when (bitRange & cast(8589934592 as bigint)) =		8589934592		then case when CAL_34 is null and VAL_34 is null then 0 when CAL_34 < 0 then NULL else CAL_34 end else CAL_34 end,
			 CAL_35=case when (bitRange & cast(17179869184 as bigint)) =	17179869184		then case when CAL_35 is null and VAL_35 is null then 0 when CAL_35 < 0 then NULL else CAL_35 end else CAL_35 end,
			 CAL_36=case when (bitRange & cast(34359738368 as bigint)) =	34359738368		then case when CAL_36 is null and VAL_36 is null then 0 when CAL_36 < 0 then NULL else CAL_36 end else CAL_36 end,
			 CAL_37=case when (bitRange & cast(68719476736 as bigint)) =	68719476736		then case when CAL_37 is null and VAL_37 is null then 0 when CAL_37 < 0 then NULL else CAL_37 end else CAL_37 end,
			 CAL_38=case when (bitRange & cast(137438953472 as bigint)) =	137438953472	then case when CAL_38 is null and VAL_38 is null then 0 when CAL_38 < 0 then NULL else CAL_38 end else CAL_38 end,
			 CAL_39=case when (bitRange & cast(274877906944 as bigint)) =	274877906944	then case when CAL_39 is null and VAL_39 is null then 0 when CAL_39 < 0 then NULL else CAL_39 end else CAL_39 end,
			 CAL_40=case when (bitRange & cast(549755813888 as bigint)) =	549755813888	then case when CAL_40 is null and VAL_40 is null then 0 when CAL_40 < 0 then NULL else CAL_40 end else CAL_40 end,
			 CAL_41=case when (bitRange & cast(1099511627776 as bigint)) =	1099511627776	then case when CAL_41 is null and VAL_41 is null then 0 when CAL_41 < 0 then NULL else CAL_41 end else CAL_41 end,
			 CAL_42=case when (bitRange & cast(2199023255552 as bigint)) =	2199023255552	then case when CAL_42 is null and VAL_42 is null then 0 when CAL_42 < 0 then NULL else CAL_42 end else CAL_42 end,
			 CAL_43=case when (bitRange & cast(4398046511104 as bigint)) =	4398046511104	then case when CAL_43 is null and VAL_43 is null then 0 when CAL_43 < 0 then NULL else CAL_43 end else CAL_43 end,
			 CAL_44=case when (bitRange & cast(8796093022208 as bigint)) =	8796093022208	then case when CAL_44 is null and VAL_44 is null then 0 when CAL_44 < 0 then NULL else CAL_44 end else CAL_44 end,
			 CAL_45=case when (bitRange & cast(17592186044416 as bigint)) = 17592186044416	then case when CAL_45 is null and VAL_45 is null then 0 when CAL_45 < 0 then NULL else CAL_45 end else CAL_45 end,
			 CAL_46=case when (bitRange & cast(35184372088832 as bigint)) = 35184372088832	then case when CAL_46 is null and VAL_46 is null then 0 when CAL_46 < 0 then NULL else CAL_46 end else CAL_46 end,
			 CAL_47=case when (bitRange & cast(70368744177664 as bigint)) = 70368744177664	then case when CAL_47 is null and VAL_47 is null then 0 when CAL_47 < 0 then NULL else CAL_47 end else CAL_47 end,
			 CAL_48=case when (bitRange & cast(140737488355328 as bigint))= 140737488355328 then case when CAL_48 is null and VAL_48 is null then 0 when CAL_48 < 0 then NULL else CAL_48 end else CAL_48 end 
			 WHEN NOT MATCHED THEN insert (TI_ID,EventDate,ChannelType,DataSource_ID,ManualValidStatus,DispatchDateTime,ContrReplaceStatus,ManualEnterStatus,CUS_ID,
			 CAL_01,CAL_02,CAL_03,CAL_04,CAL_05,CAL_06,CAL_07,CAL_08,CAL_09,CAL_10,CAL_11,CAL_12,CAL_13,CAL_14,CAL_15,CAL_16,CAL_17,CAL_18,CAL_19,CAL_20,CAL_21,CAL_22,
			 CAL_23,CAL_24,CAL_25,CAL_26,CAL_27,CAL_28,CAL_29,CAL_30,CAL_31,CAL_32,CAL_33,CAL_34,CAL_35,CAL_36,CAL_37,CAL_38,CAL_39,CAL_40,CAL_41,CAL_42,CAL_43,CAL_44,
			 CAL_45,CAL_46,CAL_47,CAL_48,ValidStatus)values (s.TI_ID,s.dt,s.ChannelType,isnull(s.DataSource_ID, 4),' + case when @isCorrect = 1 then 'null' else 's.bitRange' end + ',GetDate(),0,0,0,';set @sqlstring3 = N'
			 case when (bitRange & cast(1 as bigint)) =				1				then 0 else NULL end,
			 case when (bitRange & cast(2 as bigint)) =				2				then 0 else NULL end,
			 case when (bitRange & cast(4 as bigint)) =				4				then 0 else NULL end,
			 case when (bitRange & cast(8 as bigint)) =				8				then 0 else NULL end,
			 case when (bitRange & cast(16 as bigint)) =			16				then 0 else NULL end,
			 case when (bitRange & cast(32 as bigint)) =			32				then 0 else NULL end,
			 case when (bitRange & cast(64 as bigint)) =			64				then 0 else NULL end,
			 case when (bitRange & cast(128 as bigint)) =			128				then 0 else NULL end,
			 case when (bitRange & cast(256 as bigint)) =			256				then 0 else NULL end,
			 case when (bitRange & cast(512 as bigint)) =			512				then 0 else NULL end,
			 case when (bitRange & cast(1024 as bigint)) =			1024			 then 0 else NULL end,
			 case when (bitRange & cast(2048 as bigint)) =			2048			 then 0 else NULL end,
			 case when (bitRange & cast(4096 as bigint)) =			4096			 then 0 else NULL end,
			 case when (bitRange & cast(8192 as bigint)) =			8192			 then 0 else NULL end,
			 case when (bitRange & cast(16384 as bigint)) =			16384			 then 0 else NULL end,
			 case when (bitRange & cast(32768 as bigint)) =			32768			 then 0 else NULL end,
			 case when (bitRange & cast(65536 as bigint)) =			65536			 then 0 else NULL end,
			 case when (bitRange & cast(131072 as bigint)) =		131072			 then 0 else NULL end,
			 case when (bitRange & cast(262144 as bigint)) =		262144			 then 0 else NULL end,
			 case when (bitRange & cast(524288 as bigint)) =		524288			 then 0 else NULL end,
			 case when (bitRange & cast(1048576 as bigint)) =		1048576			 then 0 else NULL end,
			 case when (bitRange & cast(2097152 as bigint)) =		2097152			 then 0 else NULL end,
			 case when (bitRange & cast(4194304 as bigint)) =		4194304			 then 0 else NULL end,
			 case when (bitRange & cast(8388608 as bigint)) =		8388608			 then 0 else NULL end,
			 case when (bitRange & cast(16777216 as bigint)) =		16777216		 then 0 else NULL end,
			 case when (bitRange & cast(33554432 as bigint)) =		33554432		 then 0 else NULL end,
			 case when (bitRange & cast(67108864 as bigint)) =		67108864		 then 0 else NULL end,
			 case when (bitRange & cast(134217728 as bigint)) =		134217728		 then 0 else NULL end,
			 case when (bitRange & cast(268435456 as bigint)) =		268435456		 then 0 else NULL end,
			 case when (bitRange & cast(536870912 as bigint)) =		536870912		 then 0 else NULL end,
			 case when (bitRange & cast(1073741824 as bigint)) =	1073741824		 then 0 else NULL end,
			 case when (bitRange & cast(2147483648 as bigint)) =	2147483648		 then 0 else NULL end,
			 case when (bitRange & cast(4294967296 as bigint)) =	4294967296		 then 0 else NULL end,
			 case when (bitRange & cast(8589934592 as bigint)) =	8589934592		 then 0 else NULL end,
			 case when (bitRange & cast(17179869184 as bigint)) =	17179869184		 then 0 else NULL end,
			 case when (bitRange & cast(34359738368 as bigint)) =	34359738368		 then 0 else NULL end,
			 case when (bitRange & cast(68719476736 as bigint)) =	68719476736		 then 0 else NULL end,
			 case when (bitRange & cast(137438953472 as bigint)) =	137438953472	 then 0 else NULL end,
			 case when (bitRange & cast(274877906944 as bigint)) =	274877906944	 then 0 else NULL end,
			 case when (bitRange & cast(549755813888 as bigint)) =	549755813888	 then 0 else NULL end,
			 case when (bitRange & cast(1099511627776 as bigint)) =	1099511627776	 then 0 else NULL end,
			 case when (bitRange & cast(2199023255552 as bigint)) =	2199023255552	 then 0 else NULL end,
			 case when (bitRange & cast(4398046511104 as bigint)) =	4398046511104	 then 0 else NULL end,
			 case when (bitRange & cast(8796093022208 as bigint)) =	8796093022208	 then 0 else NULL end,
			 case when (bitRange & cast(17592186044416 as bigint)) = 17592186044416	 then 0 else NULL end,
			 case when (bitRange & cast(35184372088832 as bigint)) = 35184372088832	 then 0 else NULL end,
			 case when (bitRange & cast(70368744177664 as bigint)) = 70368744177664	 then 0 else NULL end,
			 case when (bitRange & cast(140737488355328 as bigint)) = 140737488355328  then 0 else NULL end, 0);'
				
				--print @sqlstring1 + @sqlstring2 + @sqlstring3
				EXEC (@sqlstring1 + @sqlstring2 + @sqlstring3);

				--select @sqlstring1 + @sqlstring2 + @sqlstring3

		FETCH NEXT FROM cc_ into @titype
		END
		CLOSE cc_
		DEALLOCATE cc_

		--select * from @repTable

		declare @EventParam tinyint

		if (@isCorrect = 0) set @EventParam = 129 --Основные данные признаны недостоверными
		else set @EventParam = 133 --Данные признаны достоверными

		--Обновляем журнал
		MERGE Expl_User_Journal_Replace_30_Virtual as vv
		USING 
			(
				select distinct TI_ID,ChannelType,cast(FLOOR(cast(StartDateTime as float)) as DateTime) as sd, StartDateTime, FinishDateTime 
				from #ti
			) as ti ON vv.TI_ID = ti.TI_ID and vv.ChannelType = ti.ChannelType and vv.EventDate = convert(smalldatetime, ti.sd) and vv.[User_ID] = @User_ID and vv.EventDateTime = @DispatchDateTime
		WHEN MATCHED THEN 
		UPDATE SET EventParam = @EventParam/*Данные признаны достоверными*/,
			 CommentString = @CommentString, CUS_ID = @CUS_ID, ZamerDateTime = DateAdd(minute, 29, ti.FinishDateTime) 
		WHEN NOT MATCHED THEN 
		INSERT ([TI_ID],[ChannelType],[EventDate],[User_ID],[EventDateTime],[EventParam],[CommentString],[ZamerDateTime],[CUS_ID])
		VALUES (ti.[TI_ID],ti.[ChannelType],convert(smalldatetime, ti.StartDateTime),@User_ID,@DispatchDateTime,@EventParam,@CommentString,DateAdd(minute, 29, ti.FinishDateTime),@CUS_ID);

		COMMIT	
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 10 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
end

go
   grant EXECUTE on usp2_ArchComm_ManualChangeValidStatus to [UserCalcService]
go