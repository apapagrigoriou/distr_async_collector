# Distributed Asynchronus Collector (DAC)

## Description
An first my first try to create an Asychronus Network/Systems/Services Perfomance Collector in Python, but with a design in mind that it would be possible to extended it also to distributed too.
It is writen in Python 3.11+. 
Initial version, supports only URL status fetching in predifined intervals (min 1 sec (added a small random offset for more evenly distribution of the requests) ) per URL, and uses Postgsql DB  to save the stats & url list
It supports asychronus DB transactions and asychronus URL status fetching. 
It also supports multiproccess initialy to maximaze multicores systems but also as an proto version to move on to a fully distributed asychronous collector.
Some initial tests exists can be found in the app_tests but more major tests are pending
The application uses multiple tasks in each collector proccess it spawns, to perfom load ballancing on the number of URLs on proccess and on task level.
The load ballancing rule at the momment is very basic plit the number of the urls between the Collector Procceses with the last Collector proccess taking added any leftovers URLs if not equal spliting exists
This is an initial idea for an application, which needs a lot of code refinement, testing and documentation. (hoppfuly more to come)

## Supporting Plattforms
Any linux distribution with Python 3.11+. 

## Instalation / First time actions.
Before running The following steps must be followed:
1. Check the requirements.txt file for required libraries
2. Initilize the Database using the provided SQL file (db_schema.sql) (you can use your own user & schema to create the tables under).
3. Add Your Urls to the urls table. Example data can be found in the file (urls_small.csv)
4. Update the provided sample configuration (collector.ini in config folder) with your own prefernces (DB connction info, logging options etc.)
   
After that we ready to go....

## Execution Notes
To start up the collector, just open a terminal, change directory to the root dir, and run the application.
`python3 ./async_url_collector.py -c ./config/collector.ini`
By default the application spawns 3 subprocceses, a logging proccessfor logging, a DB proccess for inserting the stats in DB (utillizing DB connection pool) and one Collector Proccess (with it's own scheduler) and with 2 asychronus Tasks for executing asychronys url fetchs
You can change the configuration file (name & location or use multiple ones)
If you want to execute more Collector procceses you can user the -p <NUM> argument in command line
If you want to have more polling tasks per each collector proccess you can use the -t <NUM> argument in command line
You can view the above help by using the -h in command line.

## Future extensions
1. Moving from MultiProces Queues to other Queue Engine (like 0mq/RabbitMQ/Kafka) and moving the platform to a complite distributed one from a MultiPorccess one.
2. Support for SNMP protocol (asychronus)  for collection of more network information
3. Support for Network Device Telemetry
4. GUI for Configuration/Data representation
   
Again this is a draft version for presenting the operation idea, hoppfuly more to come :-)

All Feedback/help is welcome, to move this project forward.
