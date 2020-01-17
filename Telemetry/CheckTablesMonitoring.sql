DECLARE @curr_db varchar  (100);
DECLARE @sql varchar (500);
DECLARE @tname varchar (100) ;
SET @curr_db = DB_NAME();

create table #monit_tables (tname varchar  (100));
insert into #monit_tables values ('Expl_Users')
insert into #monit_tables values ('Expl_User_UserGroup')
insert into #monit_tables values ('Expl_UserGroup_Right')
insert into #monit_tables values ('Expl_Users_DBObjects')
insert into #monit_tables values ('Dict_FreeHierarchyTree')
insert into #monit_tables values ('Dict_FreeHierarchyTree_Description')
insert into #monit_tables values ('Dict_FreeHierarchyTypes')
insert into #monit_tables values ('FIAS_FullAddress')
insert into #monit_tables values ('FIAS_FullAddressToHierarchy')
insert into #monit_tables values ('UA_Nodes')
insert into #monit_tables values ('UA_Nodes_Attributes_DataType')
insert into #monit_tables values ('UA_Nodes_Attributes_Method')
insert into #monit_tables values ('UA_Nodes_Attributes_Object')
insert into #monit_tables values ('UA_Nodes_Attributes_ObjectType')
insert into #monit_tables values ('UA_Nodes_Attributes_ReferenceType')
insert into #monit_tables values ('UA_Nodes_Attributes_Variable')
insert into #monit_tables values ('UA_Nodes_Attributes_VariableType')
insert into #monit_tables values ('UA_Nodes_Attributes_View')
--    'UA_Refs' ----- НЕТ КЛЮЧЕЙ - нельзя включить мониторинг
insert into #monit_tables values ('UA_Servers')
insert into #monit_tables values ('UA_Servers_NamespaceUris')


SET @sql = 
         'if not exists(select * from sys.change_tracking_databases where [database_id]=db_id('+''''+@curr_db+'''))
          ALTER DATABASE '+@curr_db+'
          SET CHANGE_TRACKING = ON 
          (CHANGE_RETENTION = 1 DAYS, AUTO_CLEANUP = ON)';

EXECUTE(@Sql);

DECLARE @tablecount int;
select @tablecount = count(*) from #monit_tables;

while @tablecount > 0 
begin
  select TOP 1 @tname = tname from #monit_tables;

  SET @sql = 
      'if not exists(select * from sys.change_tracking_tables where object_id=OBJECT_ID('+''''+@tname+'''))
      ALTER TABLE '+ @tname +' ENABLE CHANGE_TRACKING'
  EXECUTE(@Sql);


  delete #monit_tables where tname = @tname;
  select @tablecount = count(*) from #monit_tables;
end

drop table #monit_tables
