[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/david-ohayon-907b85138/)
# Data Loader 
## Motivation
This project uses different technologies to automate the ingestion and modelling of data. 

It is important to note that technologies such as Airflow or other orchestration technologies were purposefully not
used. The project uses containerised setup for deploying the required resources.

Doing this project, I'm following the assumption that the data I have is always in that structure, and therefore I employed
no schema validations to the data.     


## Problem
The problem that is dealt with in this project is the ingestion of JSON formatted data into a relational database 
structure. The datasets used for this project are event data and organization data.
 
## Requirements
In order to run this project, Docker needs to be installed and running.


## Project Composition
This project employs two different architectures. The first and main architecture this project focuses on is depicted in
the following graph:

#### Architecture A
![picture](https://app.lucidchart.com/publicSegments/view/39772281-05d2-47c9-9119-25d53f06cf93/image.png)

Where the data loading application sits on a Python based container, extracts the necessary data and puts it in a location
shared by a volume with the PostgreSQL container. It is then loaded to the database and SQL modelling runs to process the data.


#### Architecture B
The second variant uses RabbitMQ, and follows a decoupled structure described in the following graph:

![picture](https://app.lucidchart.com/publicSegments/view/48486cd7-3ab3-499a-b93a-231cdfbfa4ba/image.png)

This structure is as well fully containerised, where each of the components sits on its own container. The producer
syncs over a specific path where JSON files are stored, these are then parsed and sent over to a queue. A consumer application
sits on the other side listening for those events and loading them to PostgreSQL as they arrive.



## Quickstart 
The repository's root directory contains an executable `run.sh`.
The quickstart executable can be used with 4 different flags:
* `deploy` - runs Architecture A
* `destroy` - Destroys all resources created by Architecture A
* `deploy-rabbitmq` - runs Architecture B
* `destroy-rabbitmq` - Destroys all resources created by Architecture B

While being in the root directory execute the following to initialise the project:
```
./run.sh deploy
```

Do note that once you deploy the application, unittests/integration tests will also run and logs would be 
printed onto the terminal. On success, a CLI interface will be launched with a split screen (using TMUX) to communicate with the application
and to be able and query the database to view the results.

Using the CLI, in Architecture A you have the option of loading the data in full or selecting a specific data to load. 
in Architecture B, you have the option to use the CLI spawned to drop the same events file over and over again and have it 
be loaded.  


#### Tmux
[tmux](https://github.com/tmux/tmux/wiki) is a terminal multiplexer. This project uses it to allow a convenient communication
with the architecture deployed. On the deployment of the application, the `run.sh` script enters the application container
and executes a tmux shell to split the terminal into two.

![picture](https://app.lucidchart.com/publicSegments/view/892d9b2a-68ec-4724-9464-ce4c7f1a3b8a/image.png)

In order switch between the split screens, use (ctrl+B) + O. Make sure to click ctrl+B at the same time and then O. You
should see the green lining switch between the screens. 

In order to scroll up and down on selected screen, use (ctrl + B) + [, and then scroll with the arrows. 
 
### Modelling
The following graph depicts the database structure composed of two schemas: raw, modelled.
The raw schemas includes all data loaded as it is. 

Note, in Architecture A, data is loaded in deltas, meaning that if a specific date was chosen out of the dataset
for loading, then it will be loaded into the delta table, and then upserted into the master table. That way, it is easier
to browse the latest data that was loaded (assuming we aren't backing up somewhere at the moment).

In Architecture B, the data is loaded in full. 

![picture](https://app.lucidchart.com/publicSegments/view/66553e5e-2318-41d8-8ec6-c5200a374944/image.png)

The modelled data displays aggregates by the received_at date. Measures such as total events by date, total unique events by date,
total events by date per organisation, total events by date per user type can be explored.  


## Remarks
It is important to note that some practices taken in this project may not be fit for production use.

## Fun fact
For closure of this project, using Architecture B with RabbitMQ, I packed in an inner queue to parse the directory 
that stores events using [watchdog](https://pypi.org/project/watchdog/). You can give it a shot and dump a JSON
file similar in structure to events_sample.json and named as `custom_sample.json`  inside of `images/base_python_image/raw_data/events` (make sure it ends with .json).
The file is then mounted into the cli container. Select `(2)` for loading the custom file and watch it live in action. ;)

You can browse the data loaded in the Postgres database.
