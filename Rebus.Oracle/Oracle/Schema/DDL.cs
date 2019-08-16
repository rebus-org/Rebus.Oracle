using System;

namespace Rebus.Oracle.Schema
{
    // This file contains DDL scripts to create all objects required by Rebus.Oracle
    static class DDL
    {
        public static readonly Func<DbName, string[]> transport = table => new[] {
$@"CREATE TABLE {table}
(
    id NUMBER(20) NOT NULL,
    recipient VARCHAR2(255) NOT NULL,
    priority NUMBER(20) NOT NULL,
    expiration timestamp with time zone NOT NULL,
    visible timestamp with time zone NOT NULL,
    headers blob NOT NULL,
    body blob NOT NULL,

    CONSTRAINT {table.Name}_pk PRIMARY KEY (recipient, priority, id)
)",

$"CREATE SEQUENCE {table}_seq",

$@"CREATE OR REPLACE TRIGGER {table}_on_insert
    BEFORE INSERT ON {table}
    FOR EACH ROW
BEGIN
    if :new.Id is null then
        :new.id := {table.Name}_seq.nextval;
    END IF;
END;",

$@"CREATE INDEX {table.Prefix}idx_receive_{table.Name} ON {table}
(
    recipient ASC, 
    expiration ASC, 
    visible ASC
)",

$@"CREATE OR REPLACE PROCEDURE {table.Prefix}rebus_dequeue_{table.Name}(recipientQueue IN varchar, now IN timestamp with time zone, output OUT SYS_REFCURSOR) AS
    messageId number;
    readCursor SYS_REFCURSOR; 
BEGIN
    open readCursor for 
    select id
    from {table.Name}
    where recipient = recipientQueue
      and visible < now
      and expiration > now
    order by priority ASC, visible ASC, id ASC
    for update skip locked;
    
    fetch readCursor into messageId;
    close readCursor;

    open output for select * from {table.Name} where id = messageId;

    delete from {table.Name} where id = messageId;
END;"
        };
    }
}