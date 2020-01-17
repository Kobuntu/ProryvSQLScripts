if exists (select 1
          from sysobjects
          where  id = object_id('usp_FreeHierarchyTree_Update')
          and type in ('P','PC'))
   drop procedure usp_FreeHierarchyTree_Update
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
--		2013
--
-- Описание:
--
--		Declarator Win
--
-- ======================================================================================


create proc [dbo].usp_FreeHierarchyTree_Update 
(
@SHierID nvarchar(4000),
@StringName	nvarchar(255),
@FreeHierItemType	tinyint,
@Expanded	bit,
@FreeHierTree_ID	int,
@FreeHierItem_ID int,
@SqlSelectString_ID int,
@ObjectStringID nvarchar(255),
@IncludeObjectChildren bit
)
AS
BEGIN
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	declare @HierID hierarchyid, @ObjectID int
	IF isnull(@ObjectStringID,'')=''
	BEGIN
		SET @ObjectStringID = NULL
		SET @ObjectID = NULL
	END
	IF (@FreeHierItemType<> 6 and @FreeHierItemType<> 14  and @FreeHierItemType<> 29 and isnull(@ObjectStringID,'')!='') SET @ObjectID = convert(INT, @ObjectStringID)
	SET @HierID = convert(HIERARCHYID, @SHierID)
	IF (@SqlSelectString_ID=0) set @SqlSelectString_ID=NULL
--обновляем таблицу
	UPDATE Dict_FreeHierarchyTree 
	SET	StringName = @StringName, 
		FreeHierItemType = @FreeHierItemType, 
		Expanded = @Expanded
	FROM
		Dict_FreeHierarchyTree
	WHERE
		HierID = @HierID 
		AND	FreeHierTree_ID = @FreeHierTree_ID
--обновляем ссылки на объекты		
IF NOT EXISTS(SELECT 1 FROM	 Dict_FreeHierarchyTree_Description	 WHERE	 FreeHierItem_ID = @FreeHierItem_ID) 
			INSERT INTO dbo.Dict_FreeHierarchyTree_Description (
					FreeHierItem_ID
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
					, XMLSystem_ID
					, JuridicalPersonContract_ID
					, JuridicalPerson_ID
					, DistributingArrangement_ID
					, BusSystem_ID
					, UANode_ID	
					, OurFormula_UN
					, ForecastObject_UN
					, SqlSelectString_ID
					)
			VALUES( @FreeHierItem_ID, 
					@IncludeObjectChildren,
					CASE WHEN @FreeHierItemType = 1 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 2 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 3 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 4 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 5 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 6 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectStringID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 7 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 8 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 9 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 12 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END,  
					CASE WHEN @FreeHierItemType = 10 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 13 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END,
					CASE WHEN @FreeHierItemType = 18 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 19 and isnull(@ObjectStringID,'')<>'' THEN @ObjectID	ELSE	NULL	END,
					CASE WHEN @FreeHierItemType = 23 THEN @ObjectID ELSE NULL END,
					CASE WHEN @FreeHierItemType = 14 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectStringID	ELSE	NULL	END, 
					CASE WHEN @FreeHierItemType = 29 and isnull(@ObjectStringID,'')<>'' THEN	@ObjectStringID	ELSE	NULL	END, 
					@SqlSelectString_ID )	
		ELSE
		UPDATE Dict_FreeHierarchyTree_Description
		SET
				IncludeObjectChildren = @IncludeObjectChildren, 
				HierLev1_ID		= CASE		WHEN @FreeHierItemType = 1 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				HierLev2_ID		= CASE		WHEN @FreeHierItemType = 2 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				HierLev3_ID		= CASE		WHEN @FreeHierItemType = 3 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				PS_ID			= CASE		WHEN @FreeHierItemType = 4 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				TI_ID			= CASE		WHEN @FreeHierItemType = 5 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				Formula_UN		= CASE		WHEN @FreeHierItemType = 6 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectStringID	ELSE			NULL	END, 
				Section_ID		= CASE		WHEN @FreeHierItemType = 7 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				TP_ID			= CASE		WHEN @FreeHierItemType = 8 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END, 
				USPD_ID			= CASE		WHEN @FreeHierItemType = 9 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				XMLSystem_ID	= CASE		WHEN @FreeHierItemType = 12 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				JuridicalPersonContract_ID	= CASE		WHEN @FreeHierItemType = 10 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				JuridicalPerson_ID			= CASE		WHEN @FreeHierItemType = 13 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				DistributingArrangement_ID	= CASE		WHEN @FreeHierItemType = 18 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				BusSystem_ID				= CASE		WHEN @FreeHierItemType = 19 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
				UANode_ID				= CASE		WHEN @FreeHierItemType = 23 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectID		ELSE			NULL	END,
			
				OurFormula_UN		= CASE		WHEN @FreeHierItemType = 14 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectStringID	ELSE			NULL	END, 
				ForecastObject_UN		= CASE		WHEN @FreeHierItemType = 29 and isnull(@ObjectStringID,'')<>'' THEN			@ObjectStringID	ELSE			NULL	END, 
				SqlSelectString_ID=@SqlSelectString_ID
		WHERE
			FreeHierItem_ID = @FreeHierItem_ID
END
go
   grant exec on usp_FreeHierarchyTree_Update to UserDeclarator
go