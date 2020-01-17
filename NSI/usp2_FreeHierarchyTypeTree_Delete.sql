if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTypeTree_Delete')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTypeTree_Delete
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- �����:
--
--		������
--
-- ���� ��������:
--
--		2014
--
-- ��������:
--
--		�������� ������ ��������� ��������
--
-- ======================================================================================


create proc [dbo].[usp2_FreeHierarchyTypeTree_Delete] 
(
@FreeHierTree_ID int 
)
AS
BEGIN
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	
	
declare @sHierID varchar(200)
select @sHierID= HierID.ToString()
 from Dict_FreeHierarchyTypes 
where @FreeHierTree_ID=FreeHierTree_ID


--���� ���� �� ������ �� .. �����
if (not exists (select 1 from Dict_FreeHierarchyTypes where @FreeHierTree_ID=FreeHierTree_ID))
begin
	RAISERROR ('��������� ������ ����������� � ���� ������, �������� ��� ������������ �������������.%s',16, 1, '');	  
  return ;
end 


			
--��������� ������� ���� ���� �������� ����
if  exists
(
select top 1  1
FROM Dict_FreeHierarchyTypes 
where HierID.GetAncestor(1) = @sHierID
)
begin
		RAISERROR ('��������� ������� ��������� ������ �.�. ������� �������� �������%s',16, 1, '');
		return ;
end


if  exists
(
select top 1  1
FROM Dict_FreeHierarchyTree
where FreeHierTree_ID=@FreeHierTree_ID
)
begin
		RAISERROR ('��������� ������� ��������� ������, �.�. ������� ����%s',16, 1, '');
		return ;
end


DELETE FROM Dict_FreeHierarchyTypes where FreeHierTree_ID=@FreeHierTree_ID


select @FreeHierTree_ID

end

go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Delete to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Delete to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Delete to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Delete to [UserExportService]
go
