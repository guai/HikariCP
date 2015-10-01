![](https://github.com/brettwooldridge/HikariCP/wiki/Hikari.png) original HikariCP can be found [here](https://github.com/brettwooldridge/HikariCP)
==========
----------------------------------------------------

***What is it?***<br/>
High availability solution for two databases based on HikariCP-2.3.8

----------------------------------------------------

***What is it for?***<br/>
I developed it for use with [activiti](http://activiti.org/)<br/>
Idea is similar to [pgpool](http://www.pgpool.net/mediawiki/index.php/Main_Page) replication but only for 2 nodes<br/>
The pool now can be configured to have 2 underlying datasources<br/>
SQL commands are sent to them both<br/>
First is master. If it loses connection you will get exception as always<br/>
Second is slave. We first try to send command to it directly. And if we could not, then commands begin to be stored in special table named invocation_queue inside master DB<br/>
So you can make two such twin connections to two DBs where master for first is slave for second and vice-versa<br/>
When one of DBs goes down one pool will fail (and can start trying to reconnect) and second just start reroute and save commands in the table<br/>
When that DB wakes up the pool for which this DB is master (which successfully done retrying to reconnect) first reads everything from invocation_queue table from it's slave connect and replay it to it's master DB<br/>
So DBs come into sync state.<br/>
While syncing, twin pool being suspended via HikariCP's JMX suspend/resume feature<br/>
<br/>
Activiti allows us to make several instances of engine on top of one DB<br/>
Using two of this high availability pools it can outlive one of two DBs failure<br/>
Still alive instance of activiti will take incomplete jobs and will run them after some lock expiration time. I also plan to put heartbeats between this two applications and reduce this lock expiration times once twin no more reachable. Or maybe there would be enough to just fire some event when we lose slave connection - needs more thinking.<br/>

----------------------------------------------------

***TODO***<br/>
1. filter selects<br/>
2. filter statements that were successfully written to DB<br/>
3. I suspect there and redundant drainQueue() calls<br/>
4. Savepoints not supported yet. Not sure what to do on abort(), setNetworkTimeout(), etc.<br/>
5. not all BLOB cases tested; BLOBs go through byte arrays<br/>
6. Modifiable ResultSets not supported. And I afraid can never be supported - make sure<br/>
7. more tests; automate them somehow<br/>
8. reset ID after invocation_queue applied and cleared<br/>