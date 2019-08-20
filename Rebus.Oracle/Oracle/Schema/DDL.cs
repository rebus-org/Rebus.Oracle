using System;

namespace Rebus.Oracle.Schema
{
    // This file contains DDL scripts to create all objects required by Rebus.Oracle
    static class DDL
    {
        public static readonly Func<DbName, string[]> transport = table => new[] {
$@"CREATE TABLE {table}
(
    id number(20) NOT NULL,
    recipient varchar2(255) NOT NULL,
    priority number(20) NOT NULL,
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
    end if;
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

        public static readonly Func<DbName, string[]> timeout = table => new[] {
$@"CREATE TABLE {table} (
    id number(10) NOT NULL CONSTRAINT {table.Name}_pk PRIMARY KEY,
    due_time timestamp(7) with time zone NOT NULL,
    headers CLOB,
    body BLOB
)",

$"CREATE SEQUENCE {table}_seq",

$@"CREATE OR REPLACE TRIGGER {table}_on_insert
    BEFORE INSERT ON {table}
    FOR EACH ROW
BEGIN
    if :new.Id is null then
        :new.id := {table.Name}_seq.nextval;
    end if;
END;",

$"CREATE INDEX {table}_due_idx ON {table} (due_time)"
        };

        public static readonly Func<DbName, string[]> subscription = table => new[] {
$@"CREATE TABLE {table} (
    topic varchar(200) NOT NULL,
    address varchar(200) NOT NULL,
    PRIMARY KEY (topic, address)
)"
        };

        public static readonly Func<DbName, string[]> sagaData = table => new[] {
$@"CREATE TABLE {table} (
    id raw(16) CONSTRAINT {table.Name}_pk PRIMARY KEY,
    revision number(10) NOT NULL,
    data blob NOT NULL
)"
        };

        public static readonly Func<DbName, string[]> sagaIndex = table => new[] {
$@"CREATE TABLE {table} (
    saga_type nvarchar2(500) NOT NULL,
    key nvarchar2(500) NOT NULL,
    value nvarchar2(2000) NOT NULL,
    saga_id raw(16) NOT NULL,

    CONSTRAINT {table.Name}_pk PRIMARY KEY (key, value, saga_type)
)",

$"CREATE INDEX {table}_idx ON {table} (saga_id)"
        };

        public static readonly Func<DbName, string[]> sagaSnapshot = table => new[] {
$@"CREATE TABLE {table} (
    id raw(16) NOT NULL,
    revision number(10) NOT NULL,
    metadata clob NOT NULL,
    data blob NOT NULL,

    CONSTRAINT {table.Name}_pk PRIMARY KEY (id, revision)
)"
        };

        public static readonly Func<DbName, string[]> dataBus = table => new[] {
$@"CREATE TABLE {table} (
    id varchar2(200) CONSTRAINT {table}_pk PRIMARY KEY,
    meta blob,
    data blob NOT NULL,
    creationTime timestamp with time zone NOT NULL,
    lastReadTime timestamp with time zone
)"
        };
    }
}