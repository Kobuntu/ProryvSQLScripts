if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_InsertPacket')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_InsertPacket
go

IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'FreeHierTreeItem' AND ss.name = N'dbo')
DROP TYPE [dbo].[FreeHierTreeItem]

-- Если нет, создаем
CREATE TYPE [dbo].[FreeHierTreeItem] AS TABLE(
	[FreeHierItem_ID] [int] NULL,
	[FreeHierTree_ID] [int] NULL,
	[ParentFreeHierItem_ID] [int] NULL,
	[StringName] [nvarchar](255) NULL,
	[FreeHierItemType] [tinyint] NOT NULL,
	[Expanded] [bit] NOT NULL,
	[FreeHierIcon_ID] int NULL,
	[ObjectStringID] [nvarchar](255) NULL,
	[IncludeObjectChildren] [bit] NOT NULL,
	[SqlSelectString_ID] [int] NULL,
	[OldFreeHierItem_ID] [int] NULL,
	[NodePath] [nvarchar](1000) NULL
)
go

grant EXECUTE on TYPE::FreeHierTreeItem to [UserCalcService]
go

grant EXECUTE on TYPE::FreeHierTreeItem to [UserDeclarator]
go

grant EXECUTE on TYPE::FreeHierTreeItem to [UserExportService]
go

grant EXECUTE on TYPE::FreeHierTreeItem to [UserImportService]
go

-- ======================================================================================
-- Автор:
--
--		Карпов
--
-- Дата создания:
--
--		2019
--
-- Описание:
--
--		добавление списка объектов в дерево свободных иерархий
--
-- ======================================================================================
create proc [dbo].[usp2_FreeHierarchyTree_InsertPacket] 
(
@FreeHierItems FreeHierTreeItem readonly,
@userId varchar(22)
)
AS
BEGIN
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
		
	--Проверка прав
	if not exists (select 1 from Expl_Users where User_ID like @userId and Expl_Users.UserRole=1) begin
		
		declare @objectUnRight nvarchar(max);
		set @objectUnRight = '';

		select distinct @objectUnRight = @objectUnRight + s.StringName + ', '
		from @FreeHierItems s
		where [dbo].[usf2_UserHasRight](@userId, 
		'48BE4C3B-2C74-4ec0-B5C4-1897220F9F6D' --EditStructure
		, s.ObjectStringID, null, s.FreeHierItemType, s.FreeHierTree_ID, null) = 0

		if (LEN(@objectUnRight) > 0) BEGIN

			DECLARE @notRightMsg nvarchar(4000)
			SET @notRightMsg = 'Недостаточно прав <Редактирование структуры>: ' + @objectUnRight;
			RAISERROR(@notRightMsg, 16, 1);

		END
	end

	DECLARE 
	@MaxFreeHierItem_ID int

	SELECT @MaxFreeHierItem_ID = max(FreeHierItem_ID)
		FROM [dbo].[Dict_FreeHierarchyTree] --Dict_FreeHierarchyTree_Description

	--Возвращаем информацию о родительских узлах, в которых будет обновлять/добавлять дочерние
	select distinct HierID as HierID, 
	ISNULL((select top 1 val from usf2_Utils_SplitNumbered(MaxDescendantHierID, '/') order by number desc), '') as MaxDescendantHierID, --Это максимальный номер существующего дочернего узла, будем добавлять после него
	FreeHierTree_ID
	into #ParentInfo
	from 
	(
		select distinct  HierID,
		(select Max(HierID) from Dict_FreeHierarchyTree where FreeHierTree_ID = t.FreeHierTree_ID and HierID.GetAncestor(1) =t.HierID).ToString() as MaxDescendantHierID,
		i.FreeHierTree_ID
		from 
		(
			select distinct ParentFreeHierItem_ID, FreeHierTree_ID from @FreeHierItems
		) i
		join Dict_FreeHierarchyTree t on t.FreeHierTree_ID = i.FreeHierTree_ID and t.FreeHierItem_ID = i.ParentFreeHierItem_ID
		
		union all --Записи, которые пойдут в рут
		select hierarchyid ::GetRoot() as HierID, 
		(select Max(HierID) from Dict_FreeHierarchyTree where FreeHierTree_ID = i.FreeHierTree_ID AND HierID.GetAncestor(1) =hierarchyid ::GetRoot()).ToString() as MaxDescendantHierID, 
		FreeHierTree_ID from @FreeHierItems i
		where ParentFreeHierItem_ID is null
	) i

	--select * from #ParentInfo --для отладки

	BEGIN TRY BEGIN TRANSACTION;

--------------Добавляем в Dict_FreeHierarchyTree------------------------------------------------------------------------------------------------

		--Таблица, где храним добавленные узлы
		DECLARE @InsertedFreeHierarchyTree TABLE 
			(
				[FreeHierTree_ID] [dbo].[FREEHIERTREE_ID_TYPE] NOT NULL,
				[HierID] [hierarchyid] NOT NULL,
				[FreeHierItem_ID] [dbo].[FREEHIERITEM_ID_TYPE] NOT NULL,
				[StringName] [nvarchar](255) NULL,
				[FreeHierItemType] [tinyint] NOT NULL,
				[Expanded] [bit] NOT NULL,
				[ObjectStringID] [nvarchar](255) NULL,
				[FreeHierIcon_ID] int NULL,

				[ParentFreeHierItem_ID] int NULL,
				[IncludeObjectChildren] [bit] NOT NULL,
				[SqlSelectString_ID] [int] NULL,
				[OldFreeHierItem_ID] [int] NULL
			)

		--Записываем через MERGE, чтобы получить записываемую таблицу и дальше ее использовать для обновления Dict_FreeHierarchyTree_Description
		MERGE dbo.Dict_FreeHierarchyTree  t
		USING
		(
			SELECT i.FreeHierTree_ID, 
			--Получаем номер иерархии HierID
			HIERARCHYID::Parse(
			(ISNULL(t.HierID, hierarchyid ::GetRoot()).ToString() 
			+ (CONVERT(VARCHAR(20), ROW_NUMBER() OVER (PARTITION BY t.HierID,i.[NodePath] order BY t.FreeHierTree_ID)  
			
			+ (select MaxDescendantHierID from #ParentInfo where FreeHierTree_ID = i.FreeHierTree_ID AND HierID = ISNULL(t.HierID, hierarchyid ::GetRoot())))) --Увеличиваем каждую запись на 1
			
			+ '/'
			+ ISNULL(i.NodePath, '') -- Это формируем на клиенте, путь до родителя который попадает в нужный узел

			)) --Добавляем смещение уже существующих дочерних узлов

			
			AS DescendantHierID, 
			--Получаем новый идентификатор FreeHierItem_ID
			@MaxFreeHierItem_ID + (ROW_NUMBER() OVER (ORDER BY t.HierID) --Увеличиваем каждую запись на 1 
			) as NewFreeHierItem_ID,
			i.StringName, i.FreeHierItemType, i.Expanded, i.FreeHierIcon_ID, i.ObjectStringID, i.IncludeObjectChildren, i.SqlSelectString_ID, i.ParentFreeHierItem_ID, i.FreeHierItem_ID as OldFreeHierItem_ID
			FROM @FreeHierItems i
			left join Dict_FreeHierarchyTree t on t.FreeHierTree_ID = i.FreeHierTree_ID and t.FreeHierItem_ID = i.ParentFreeHierItem_ID
		) u on (1=0)
		WHEN NOT MATCHED THEN INSERT (FreeHierTree_ID, HierID, FreeHierItem_ID, StringName, FreeHierItemType, Expanded, FreeHierIcon_ID)
		VALUES (u.FreeHierTree_ID, u.DescendantHierID, u.NewFreeHierItem_ID, u.StringName, u.FreeHierItemType, u.Expanded, u.FreeHierIcon_ID)
		--Далее записываемая таблица
		OUTPUT  INSERTED.FreeHierTree_ID, INSERTED.HierID, INSERTED.FreeHierItem_ID, INSERTED.StringName, INSERTED.FreeHierItemType, INSERTED.Expanded,
		u.ObjectStringID, INSERTED.FreeHierIcon_ID, u.ParentFreeHierItem_ID, u.IncludeObjectChildren, u.SqlSelectString_ID, u.OldFreeHierItem_ID
		INTO @InsertedFreeHierarchyTree;

		drop table #ParentInfo

	--------------Добавляем в Dict_FreeHierarchyTree_Description------------------------------------------------------------------------------------------------
		DECLARE @InsertedFreeHierarchyDescription TABLE 
			(
			[FreeHierItem_ID] [dbo].[FREEHIERITEM_ID_TYPE] NOT NULL,
			[HierLev1_ID] [dbo].[HIERLEV1_ID_TYPE] NULL,
			[HierLev2_ID] [dbo].[HIERLEV2_ID_TYPE] NULL,
			[HierLev3_ID] [dbo].[HIERLEV3_ID_TYPE] NULL,
			[PS_ID] [dbo].[PS_ID_TYPE] NULL,
			[TI_ID] [dbo].[TI_ID_TYPE] NULL,
			[Formula_UN] [dbo].[ABS_NUMBER_TYPE_2] NULL,
			[Section_ID] [dbo].[SECTION_ID_TYPE] NULL,
			[TP_ID] [dbo].[TP_ID_TYPE] NULL,
			[USPD_ID] [dbo].[USPD_ID_TYPE] NULL,
			[XMLSystem_ID] [dbo].[XML_SYSTEM_ID_TYPE] NULL,
			[JuridicalPerson_ID] [dbo].[JURIDICALPERSON_ID_TYPE] NULL,
			[JuridicalPersonContract_ID] [dbo].[JURIDICALPERSONCONTRACT_ID_TYPE] NULL,
			[DistributingArrangement_ID] [dbo].[DISTRIBUTINGARRANGEMENT_ID_TYPE] NULL,
			[BusSystem_ID] [dbo].[BUSSYSTEM_ID_TYPE] NULL,
			[UANode_ID] [dbo].[UANODE_ID_TYPE] NULL,
			[IncludeObjectChildren] [bit] NOT NULL,
			[OurFormula_UN] [dbo].[ABS_NUMBER_TYPE_2] NULL,
			[ForecastObject_UN] [dbo].[FORECASTOBJECT_UN_TYPE] NULL,
			[SqlSelectString_ID] [dbo].[SQLSELECTSTRING_ID_TYPE] NULL
			)

		--Тут у Дениса не совсем понятно, т.к. идентификаторы генерируются новые, видимо, update не будет никогда
		MERGE Dict_FreeHierarchyTree_Description as a
		USING (
			select n.FreeHierItem_ID,
			CASE WHEN n.FreeHierItemType = 1  THEN ObjectStringID ELSE NULL END	as HierLev1_ID,
			CASE WHEN n.FreeHierItemType = 2  THEN ObjectStringID ELSE NULL END	as HierLev2_ID,
			CASE WHEN n.FreeHierItemType = 3  THEN ObjectStringID ELSE NULL END	as HierLev3_ID,
			CASE WHEN n.FreeHierItemType = 4  THEN ObjectStringID ELSE NULL END	as PS_ID,
			CASE WHEN n.FreeHierItemType = 5  THEN ObjectStringID ELSE NULL END	as TI_ID,
			CASE WHEN n.FreeHierItemType = 6  THEN ObjectStringID ELSE NULL END	as Formula_UN,
			CASE WHEN n.FreeHierItemType = 7  THEN ObjectStringID ELSE NULL END	as Section_ID,
			CASE WHEN n.FreeHierItemType = 8  THEN ObjectStringID ELSE NULL END	as TP_ID,
			CASE WHEN n.FreeHierItemType = 9  THEN ObjectStringID ELSE NULL END	as USPD_ID,
			CASE WHEN n.FreeHierItemType = 12 THEN ObjectStringID ELSE NULL END	as XMLSystem_ID,
			CASE WHEN n.FreeHierItemType = 10 THEN ObjectStringID ELSE NULL END	as JuridicalPersonContract_ID,
			CASE WHEN n.FreeHierItemType = 13 THEN ObjectStringID ELSE NULL END	as JuridicalPerson_ID,
			CASE WHEN n.FreeHierItemType = 18 THEN ObjectStringID ELSE NULL END	as DistributingArrangement_ID,
			CASE WHEN n.FreeHierItemType = 19 THEN ObjectStringID ELSE NULL END	as BusSystem_ID,
			CASE WHEN n.FreeHierItemType = 23 THEN ObjectStringID ELSE NULL END	as UANode_ID,
			CASE WHEN n.FreeHierItemType = 14 THEN ObjectStringID ELSE NULL END	as OurFormula_UN,
			CASE WHEN n.FreeHierItemType = 29 THEN ObjectStringID ELSE NULL END	as ForecastObject_UN,
			IncludeObjectChildren,
			SqlSelectString_ID
			from @InsertedFreeHierarchyTree n) n
		ON a.FreeHierItem_ID = n.FreeHierItem_ID
		WHEN MATCHED THEN UPDATE SET 
			IncludeObjectChildren = n.IncludeObjectChildren,
			SqlSelectString_ID=n.SqlSelectString_ID,
			HierLev1_ID= n.HierLev1_ID, 
			HierLev2_ID= n.HierLev2_ID, 
			HierLev3_ID= n.HierLev3_ID, 
			PS_ID=n.PS_ID, 
			TI_ID= n.TI_ID,
			Formula_UN= n.Formula_UN, 
			Section_ID= n.Section_ID, 
			TP_ID= n.TP_ID,
			USPD_ID= n.USPD_ID,
			XMLSystem_ID= n.XMLSystem_ID,
			JuridicalPersonContract_ID=n.JuridicalPersonContract_ID,
			JuridicalPerson_ID=n.JuridicalPerson_ID,
			DistributingArrangement_ID = n.DistributingArrangement_ID,
			BusSystem_ID=n.BusSystem_ID,
			UANode_ID=n.UANode_ID,
			OurFormula_UN=n.OurFormula_UN,
			ForecastObject_UN=n.ForecastObject_UN
		WHEN NOT MATCHED THEN 
		INSERT (
			FreeHierItem_ID, 
			HierLev1_ID,
			HierLev2_ID,
			HierLev3_ID,
			PS_ID,
			TI_ID,
			Formula_UN,
			Section_ID,
			TP_ID,
			USPD_ID,
			XMLSystem_ID,
			JuridicalPersonContract_ID,
			JuridicalPerson_ID,
			DistributingArrangement_ID,
			BusSystem_ID,
			UANode_ID,
			IncludeObjectChildren,
			OurFormula_UN,
			ForecastObject_UN,							
			SqlSelectString_ID)
		VALUES (
			n.FreeHierItem_ID, 
			n.HierLev1_ID,
			n.HierLev2_ID,
			n.HierLev3_ID,
			n.PS_ID,
			n.TI_ID,
			n.Formula_UN,
			n.Section_ID,
			n.TP_ID,
			n.USPD_ID,
			n.XMLSystem_ID,
			n.JuridicalPersonContract_ID,
			n.JuridicalPerson_ID,
			n.DistributingArrangement_ID,
			n.BusSystem_ID,
			n.UANode_ID,
			n.IncludeObjectChildren,				
			n.OurFormula_UN,
			n.ForecastObject_UN,							
			n.SqlSelectString_ID)
	--Далее записываемая таблица
		OUTPUT 
			INSERTED.FreeHierItem_ID, 
			INSERTED.HierLev1_ID,
			INSERTED.HierLev2_ID,
			INSERTED.HierLev3_ID,
			INSERTED.PS_ID,
			INSERTED.TI_ID,
			INSERTED.Formula_UN,
			INSERTED.Section_ID,
			INSERTED.TP_ID,
			INSERTED.USPD_ID,
			INSERTED.XMLSystem_ID,
			INSERTED.JuridicalPersonContract_ID,
			INSERTED.JuridicalPerson_ID,
			INSERTED.DistributingArrangement_ID,
			INSERTED.BusSystem_ID,
			INSERTED.UANode_ID,
			INSERTED.IncludeObjectChildren, 
			INSERTED.OurFormula_UN,
			INSERTED.ForecastObject_UN,							
			INSERTED.SqlSelectString_ID
		INTO @InsertedFreeHierarchyDescription;

	--------------Отправляем уведомления в Rabit------------------------------------------------------------------------------------------------

		declare
			-- Поля от Dict_FreeHierarchyTree
			@FreeHierTree_ID [dbo].[FREEHIERTREE_ID_TYPE],
			@HierID [hierarchyid],
			@FreeHierItem_ID [dbo].[FREEHIERITEM_ID_TYPE],
			@StringName [nvarchar](255),
			@FreeHierItemType [tinyint],
			@Expanded [bit],
			@ObjectStringID [nvarchar](255),
				
			--Поля от Dict_FreeHierarchyTree_Description
			@HierLev1_ID [dbo].[HIERLEV1_ID_TYPE],
			@HierLev2_ID [dbo].[HIERLEV2_ID_TYPE],
			@HierLev3_ID [dbo].[HIERLEV3_ID_TYPE],
			@PS_ID [dbo].[PS_ID_TYPE],
			@TI_ID [dbo].[TI_ID_TYPE],
			@Formula_UN [dbo].[ABS_NUMBER_TYPE_2],
			@Section_ID [dbo].[SECTION_ID_TYPE],
			@TP_ID [dbo].[TP_ID_TYPE],
			@USPD_ID [dbo].[USPD_ID_TYPE],
			@XMLSystem_ID [dbo].[XML_SYSTEM_ID_TYPE],
			@JuridicalPerson_ID [dbo].[JURIDICALPERSON_ID_TYPE],
			@JuridicalPersonContract_ID [dbo].[JURIDICALPERSONCONTRACT_ID_TYPE],
			@DistributingArrangement_ID [dbo].[DISTRIBUTINGARRANGEMENT_ID_TYPE],
			@BusSystem_ID [dbo].[BUSSYSTEM_ID_TYPE],
			@UANode_ID [dbo].[UANODE_ID_TYPE],
			@IncludeObjectChildren [bit],
			@SqlSelectString_ID [dbo].[SQLSELECTSTRING_ID_TYPE],
			@OurFormula_UN [dbo].[ABS_NUMBER_TYPE_2],
			@ForecastObject_UN [dbo].[FORECASTOBJECT_UN_TYPE]


		declare @routing nvarchar(4000), @jsonObject nvarchar(max)

		declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select FreeHierTree_ID, HierID, t.FreeHierItem_ID, StringName, FreeHierItemType,
			Expanded, ObjectStringID, HierLev1_ID, HierLev2_ID,	HierLev3_ID, PS_ID, TI_ID,
			Formula_UN, Section_ID, TP_ID, USPD_ID, XMLSystem_ID,JuridicalPerson_ID, JuridicalPersonContract_ID,
			DistributingArrangement_ID, BusSystem_ID, UANode_ID, t.IncludeObjectChildren, t.SqlSelectString_ID, OurFormula_UN, ForecastObject_UN
			from @InsertedFreeHierarchyTree t
			join @InsertedFreeHierarchyDescription d on d.FreeHierItem_ID = t.FreeHierItem_ID 

		open t;
		FETCH NEXT FROM t into @FreeHierTree_ID, @HierID, @FreeHierItem_ID, @StringName, @FreeHierItemType,
			@Expanded, @ObjectStringID, @HierLev1_ID, @HierLev2_ID,	@HierLev3_ID, @PS_ID, @TI_ID,
			@Formula_UN, @Section_ID, @TP_ID, @USPD_ID, @XMLSystem_ID,@JuridicalPerson_ID, @JuridicalPersonContract_ID,
			@DistributingArrangement_ID, @BusSystem_ID,	@UANode_ID,	@IncludeObjectChildren,	@SqlSelectString_ID, @OurFormula_UN, @ForecastObject_UN
		WHILE @@FETCH_STATUS = 0
		BEGIN

			SET @jsonObject='Dict_FreeHierarchyTree:'+
					 dbo.usf_ConvertSQL_To_JSON(
					  (
						SELECT @FreeHierTree_ID as FreeHierTree_ID, @HierID as HierID, @FreeHierItem_ID as FreeHierItem_ID, @StringName as StringName, @FreeHierItemType as FreeHierItemType, @Expanded as Expanded
						FOR XML path, root)
					  )
					  +',Dict_FreeHierarchyTree_Description:'+
					  dbo.usf_ConvertSQL_To_JSON(
					  (
						SELECT @FreeHierItem_ID as FreeHierItem_ID, @HierLev1_ID as HierLev1_ID, @HierLev2_ID as HierLev2_ID, @HierLev3_ID asHierLev3_ID, @PS_ID as PS_ID, @TI_ID as TI_ID,
							@Formula_UN as Formula_UN, @Section_ID as Section_ID, @TP_ID as TP_ID, @USPD_ID as USPD_ID, @XMLSystem_ID as XMLSystem_ID, @JuridicalPerson_ID as JuridicalPerson_ID, @JuridicalPersonContract_ID as JuridicalPersonContract_ID,
							@DistributingArrangement_ID as DistributingArrangement_ID, @BusSystem_ID as BusSystem_ID, @UANode_ID as UANode_ID, @IncludeObjectChildren as IncludeObjectChildren,	@SqlSelectString_ID as SqlSelectString_ID, 
							@OurFormula_UN as OurFormula_UN, @ForecastObject_UN as ForecastObject_UN
						FOR XML path, root)
					  )

			SET @routing = 'Dict_FreeHierarchyTree.Insert.' + convert(varchar(200),@FreeHierTree_ID) + '.' + @HierID.ToString() 
			exec spclr_MQ_TryPostMessage @jsonObject, @routing, null

		FETCH NEXT FROM t into @FreeHierTree_ID, @HierID, @FreeHierItem_ID, @StringName, @FreeHierItemType,
			@Expanded, @ObjectStringID, @HierLev1_ID, @HierLev2_ID,	@HierLev3_ID, @PS_ID, @TI_ID,
			@Formula_UN, @Section_ID, @TP_ID, @USPD_ID, @XMLSystem_ID,@JuridicalPerson_ID, @JuridicalPersonContract_ID,
			@DistributingArrangement_ID, @BusSystem_ID,	@UANode_ID,	@IncludeObjectChildren,	@SqlSelectString_ID, @OurFormula_UN, @ForecastObject_UN
		end;
		CLOSE t
		DEALLOCATE t

		--Возвращаем добавленные идентификаторы
		select FreeHierTree_ID, FreeHierItem_ID, StringName, FreeHierItemType, Expanded, FreeHierIcon_ID, ObjectStringID, IncludeObjectChildren, 
			SqlSelectString_ID, ParentFreeHierItem_ID, OldFreeHierItem_ID
		from @InsertedFreeHierarchyTree

	COMMIT
	END TRY		
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 
		--SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH
END

go
   grant EXECUTE on usp2_FreeHierarchyTree_InsertPacket to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_InsertPacket to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTree_InsertPacket to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_InsertPacket to [UserExportService]
go