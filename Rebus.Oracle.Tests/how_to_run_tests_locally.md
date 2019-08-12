## How to run the test suite locally

You don't need to install an Oracle database just to be able to run the test suite. The only prerequisite is that you have Docker installed and have an account on [Docker Hub](https://hub.docker.com/).

1. Navigate to the [Oracle database image](https://hub.docker.com/_/oracle-database-enterprise-edition) on Docker Hub, click "proceed to checkout" and accept the terms and licenses of the **Oracle database** image

1. Open a command window and make sure you're logged in on Docker Hub.
    ```
    > docker login
    ```

1. Run the docker image in a local container and give it a name (e.g. `rebus-oracle-docker`)
    ```
    > docker run -d -it -p 1521:1521 -p 5500:5500 --name rebus-oracle-docker store/oracle/database-enterprise:12.2.0.1
    ```

1. Wait for the container to finish starting up (this can take up to 5 minutes)
You can follow the internal log of the docker via `docker logs rebus-oracle-docker -f`

1. In Oracle a schema is coupled to a user. By default there's only an internal `SYS` schema, but we can't use that one for our tests, so we first need to create a new user. You therefor need to connect to the database with an tool like [SQL Developer](https://www.oracle.com/database/technologies/appdev/sql-developer.html) (which is free but requires you to create an Oracle account) or [Toad](https://www.quest.com/products/toad-for-oracle/) (not free).

1. Connect to your database using the following information:
   - Username: SYS
   - Password: Oradoc_db1
   - Role: SysDba
   - Hostname: localhost
   - Port: 1521
   - Service name: ORCLCDB.LOCALDOMAIN

1. Run the following script to create the `rebus` user with password `rebus`:
   ```
   alter session set "_ORACLE_SCRIPT"=true;  
   CREATE USER rebus IDENTIFIED BY rebus;
   GRANT CONNECT, RESOURCE, DBA TO rebus;
   GRANT CREATE SESSION TO rebus;
   GRANT UNLIMITED TABLESPACE TO rebus;
   grant create table to rebus;
   grant create view to rebus;
   grant create any trigger to rebus;
   grant create any procedure to rebus;
   grant create any sequence to rebus;
   ```

1. All done! 🎉

### Useful Docker commands
##### See which containers are running
`docker ps`

##### See which containers are defined (either stopped or running)
`docker ps -a`

##### Stop a container
`docker stop rebus-oracle-docker`
_Remark: you won't lose the data store in the container by simply stopping and starting it again_

##### Start a container
`docker start rebus-oracle-docker`

##### Remove a stopped container
`docker rm rebus-oracle-docker`

##### Tail log a running container
`docker logs rebus-oracle-docker -f`