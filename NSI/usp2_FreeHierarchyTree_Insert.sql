if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_Insert')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_Insert
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- Автор:
--
--		Карпов
--
-- Дата создания:
--
--		2014
--
-- Описание:
--
--		добавление объекта в дерево свободных иерархий
--
-- ======================================================================================


create proc [dbo].[usp2_FreeHierarchyTree_Insert] 
(
@FreeHierTree_ID	int,
@ParentFreeHierItem_ID	int,
@StringName	nvarchar(255),
@FreeHierItemType	tinyint,
@Expanded	bit,
@ObjectStringID nvarchar(255), 
@IncludeObjectChildren bit,
@SqlSelectString_ID int
)
AS
BEGIN
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	
	set transaction isolation level read uncommitted
	


IF (@SqlSelectString_ID<=0) SET @SqlSelectString_ID = NULL

IF (@ParentFreeHierItem_ID<=0) SET @ParentFreeHierItem_ID = NULL
	
	DECLARE 
	@ParentHierID  as hierarchyid, 
	@LastNode  as hierarchyid, 
	@NewNode   as hierarchyid,
	@MaxFreeHierItem_ID int,
	@ObjectID int

	--если есть такой узел не добавляем (пока только для UA)
	select @MaxFreeHierItem_ID=Dict_FreeHierarchyTree.FreeHierItem_ID
	from Dict_FreeHierarchyTree join Dict_FreeHierarchyTree_Description
	 on Dict_FreeHierarchyTree.FreeHierItem_ID=Dict_FreeHierarchyTree_Description.FreeHierItem_ID
	 where 
	 Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID
	 and @FreeHierItemType=23 
	 and Dict_FreeHierarchyTree.FreeHierItemType=23
	 and  Dict_FreeHierarchyTree_Description.UANode_ID=@ObjectStringID

	if (@MaxFreeHierItem_ID is not null)
	begin	
		select result = @MaxFreeHierItem_ID
		return
	end

	
	IF isnull(@ObjectStringID,'')=''
	BEGIN
		SET @ObjectStringID = NULL
		SET @ObjectID = NULL
	END
	
	IF (@FreeHierItemType<> 6 and @FreeHierItemType<> 14 and @FreeHierItemType<> 29 and isnull(@ObjectStringID,'')!='') SET @ObjectID = convert(INT, @ObjectStringID)
	
	

	--находим максимальные идентификаторы и сохраняем запись в таблицу
	--SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;	
		BEGIN TRY BEGIN TRANSACTION;
		
		--находим род идентификатор
		select @ParentHierID = HierID 
		from Dict_FreeHierarchyTree
		where 
		FreeHierTree_ID = @FreeHierTree_ID 
		and FreeHierItem_ID=@ParentFreeHierItem_ID
		
		--выбираем мин уровень
		declare @MinLvl int

		if (@ParentHierID is null) begin 
			select @MinLvl = MIN(Dict_FreeHierarchyTree.HierID.GetLevel())
			from Dict_FreeHierarchyTree
			where 
			FreeHierTree_ID=@FreeHierTree_ID
		end
							
		SELECT @LastNode = max(HierID)
		FROM Dict_FreeHierarchyTree
		WHERE 
		Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID and
		(
			(@ParentHierID is null and Dict_FreeHierarchyTree.HierID.GetLevel()=@MinLvl)
			or
			(@ParentHierID is not null and Dict_FreeHierarchyTree.HierID.GetAncestor(1) =@ParentHierID)		
		)
			
		--если родитель не указан то ставим parent=0
		IF (@ParentHierID is null) SET @ParentHierID = hierarchyid ::GetRoot()
			
		--если корень=0 есть и добваляют еще корень то добавляем с уровнем 1
		if (@LastNode!=@ParentHierID)
			SET @NewNode = @ParentHierID.GetDescendant(@LastNode, NULL);	
		else 
		begin
			SELECT @LastNode = max(HierID)
			FROM Dict_FreeHierarchyTree
			WHERE 
				Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID and Dict_FreeHierarchyTree.HierID.GetAncestor(1) =@LastNode

			SET @NewNode = @ParentHierID.GetDescendant(@LastNode, NULL);	
		end			
		
		SELECT @MaxFreeHierItem_ID = max(FreeHierItem_ID)
		FROM [dbo].[Dict_FreeHierarchyTree] --Dict_FreeHierarchyTree_Description

		SET @MaxFreeHierItem_ID = isnull(@MaxFreeHierItem_ID, 0) + 1
		--print @MaxFreeHierItem_ID
		
		--select @FreeHierTree_ID, @NewNode, @MaxFreeHierItem_ID, @StringName, @FreeHierItemType, @Expanded

		DECLARE @InsertedFreeHierarchyTree TABLE 
		(
			[FreeHierTree_ID] [dbo].[FREEHIERTREE_ID_TYPE] NOT NULL,
			[HierID] [hierarchyid] NOT NULL,
			[FreeHierItem_ID] [dbo].[FREEHIERITEM_ID_TYPE] NOT NULL,
			[StringName] [nvarchar](255) NULL,
			[FreeHierItemType] [tinyint] NOT NULL,
			[Expanded] [bit] NOT NULL
		)

		INSERT INTO dbo.Dict_FreeHierarchyTree 
		(FreeHierTree_ID, HierID, FreeHierItem_ID, StringName, FreeHierItemType, Expanded)
		OUTPUT INSERTED.FreeHierTree_ID, INSERTED.HierID, INSERTED.FreeHierItem_ID, INSERTED.StringName, INSERTED.FreeHierItemType, INSERTED.Expanded into @InsertedFreeHierarchyTree
		VALUES (@FreeHierTree_ID, @NewNode, @MaxFreeHierItem_ID, @StringName, @FreeHierItemType, @Expanded)


	declare
	@HierLev1_ID tinyint,
	@HierLev2_ID int,
	@HierLev3_ID int,
	@PS_ID int,
	@TI_ID int,
	@Formula_UN varchar(22),
	@Section_ID int,
	@TP_ID int,
	@USPD_ID int,
	@XMLSystem_ID tinyint,
	@JuridicalPerson_ID int,
	@JuridicalPersonContract_ID int,
	@DistributingArrangement_ID int,
	@BusSystem_ID int,
	@UANode_ID bigint,
	@OurFormula_UN varchar(22),
	@ForecastObject_UN varchar(22)

	select @HierLev1_ID =				CASE WHEN @FreeHierItemType = 1 THEN @ObjectID ELSE NULL END,
		@HierLev2_ID =					CASE WHEN @FreeHierItemType = 2 THEN @ObjectID ELSE NULL END,
		@HierLev3_ID =					CASE WHEN @FreeHierItemType = 3 THEN @ObjectID ELSE NULL END,
		@PS_ID =						CASE WHEN @FreeHierItemType = 4 THEN @ObjectID ELSE NULL END,
		@TI_ID =						CASE WHEN @FreeHierItemType = 5 THEN @ObjectID ELSE NULL END,
		@Formula_UN =					CASE WHEN @FreeHierItemType = 6 THEN @ObjectStringID ELSE NULL END,
		@Section_ID =					CASE WHEN @FreeHierItemType = 7 THEN @ObjectID ELSE NULL END,
		@TP_ID =						CASE WHEN @FreeHierItemType = 8 THEN @ObjectID ELSE NULL END,
		@USPD_ID =						CASE WHEN @FreeHierItemType = 9 THEN @ObjectID ELSE NULL END,
		@XMLSystem_ID =					CASE WHEN @FreeHierItemType = 12 THEN @ObjectID ELSE NULL END,
		@JuridicalPersonContract_ID =	CASE WHEN @FreeHierItemType = 10 THEN @ObjectID ELSE NULL END,
		@JuridicalPerson_ID =			CASE WHEN @FreeHierItemType = 13 THEN @ObjectID ELSE NULL END,
		@DistributingArrangement_ID =	CASE WHEN @FreeHierItemType = 18 THEN @ObjectID ELSE NULL END,
		@BusSystem_ID =					CASE WHEN @FreeHierItemType = 19 THEN @ObjectID ELSE NULL END,
		@UANode_ID =					CASE WHEN @FreeHierItemType = 23 THEN @ObjectID ELSE NULL END,
		@OurFormula_UN =				CASE WHEN @FreeHierItemType = 14 THEN @ObjectStringID ELSE NULL END, 
		@ForecastObject_UN =			CASE WHEN @FreeHierItemType = 29 THEN @ObjectStringID ELSE NULL END 
	
	--добавляем описание 
	IF NOT EXISTS (SELECT 1 FROM Dict_FreeHierarchyTree_Description	 WHERE	 FreeHierItem_ID = @MaxFreeHierItem_ID) BEGIN 

		INSERT INTO dbo.Dict_FreeHierarchyTree_Description 
					(FreeHierItem_ID
					, IncludeObjectChildren
					, HierLev1_ID
					, HierLev2_ID
					, HierLev3_ID
					, PS_ID
					, TI_ID
					, Formula_UN
					, Section_ID
					, TP_ID
					, USPD_ID	
					,XMLSystem_ID	
					, JuridicalPersonContract_ID
					, JuridicalPerson_ID
					, DistributingArrangement_ID
					, BusSystem_ID
					, UANode_ID	
					,OurFormula_UN	
					,ForecastObject_UN										
					, SqlSelectString_ID)
			VALUES (@MaxFreeHierItem_ID,
					 @IncludeObjectChildren, 
					 @HierLev1_ID,
					 @HierLev2_ID,
					 @HierLev3_ID,
					 @PS_ID,
					 @TI_ID,
					 @Formula_UN,
					 @Section_ID,
					 @TP_ID,
					 @USPD_ID,
					 @XMLSystem_ID,
					 @JuridicalPersonContract_ID,
					 @JuridicalPerson_ID,
					 @DistributingArrangement_ID,
					 @BusSystem_ID,
					 @UANode_ID,
					 @OurFormula_UN,	
					 @ForecastObject_UN,	
					 @SqlSelectString_ID)
		
	END ELSE BEGIN 

		update 		
		dbo.Dict_FreeHierarchyTree_Description 
		set 
		IncludeObjectChildren=  @IncludeObjectChildren,
		Dict_FreeHierarchyTree_Description.SqlSelectString_ID=@SqlSelectString_ID,					
		HierLev1_ID= @HierLev1_ID, 
		HierLev2_ID= @HierLev2_ID, 
		HierLev3_ID= @HierLev3_ID, 
		PS_ID=@PS_ID, 
		TI_ID= @TI_ID,
		Formula_UN= @Formula_UN, 
		Section_ID= @Section_ID, 
		TP_ID= @TP_ID,
		USPD_ID= @USPD_ID,
		XMLSystem_ID= @XMLSystem_ID,
		JuridicalPersonContract_ID=@JuridicalPersonContract_ID,
		JuridicalPerson_ID=@JuridicalPerson_ID,
		DistributingArrangement_ID=@DistributingArrangement_ID,
		BusSystem_ID=@BusSystem_ID,
		UANode_ID=@UANode_ID,
		OurFormula_UN=@OurFormula_UN,
		ForecastObject_UN=@ForecastObject_UN
		where FreeHierItem_ID= @MaxFreeHierItem_ID
	END


	--if (@MaxFreeHierItem_ID is not null) begin

		declare @routing nvarchar(4000)

		declare @jsonObject nvarchar(max)
		select @jsonObject='Dict_FreeHierarchyTree:'+
				 dbo.usf_ConvertSQL_To_JSON(
				  (SELECT top 1 
					n.*
				   FROM @InsertedFreeHierarchyTree n 
				   --WHERE FreeHierTree_ID=@FreeHierTree_ID and n.FreeHierItem_ID=@MaxFreeHierItem_ID
				   FOR XML path, root)
				  )
				  +',Dict_FreeHierarchyTree_Description:'+
				  dbo.usf_ConvertSQL_To_JSON(
				  (SELECT top 1 
					n.*
				   FROM Dict_FreeHierarchyTree_Description n 
				   WHERE n.FreeHierItem_ID=@MaxFreeHierItem_ID
				   FOR XML path, root)
				  )

		--print @jsonObject

		set @routing =  'Dict_FreeHierarchyTree.Insert.' + convert(varchar(200),@FreeHierTree_ID)+'.'+@NewNode.ToString() 
		exec spclr_MQ_TryPostMessage @jsonObject, @routing, null

	--end

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

	

	select result = @MaxFreeHierItem_ID 
END

go
   grant EXECUTE on usp2_FreeHierarchyTree_Insert to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Insert to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Insert to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Insert to [UserExportService]
go






 