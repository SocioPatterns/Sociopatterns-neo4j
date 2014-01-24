#Sociopatterns-neo4j

##contactadder-neo4j.py

###class Neo4jContactAdder - Description

__Neo4jContactAdder__ adds contacts (defined in sociopatterns.loader) in a neo4j graph database modeled like specified in [neo4j-dynagraph]( https://github.com/ccattuto/neo4j-dynagraph/wiki/Representing-time-dependent-graphs-in-Neo4j) extended with labels on frames (first_frame, last_frame, year, month, day, hour) and index on actors' name. It uses db drivers from [py2neo 1.6.1]( http://book.py2neo.org/en/latest/ ), and the version of [neo4j and cypher is 2.0.0]( http://docs.neo4j.org/chunked/milestone/ ). <br />  

The main concept is influenced by the need to pay attention to performance, so _py2neo_'s batches to send a bulk of insertion to DB, reducing REST overhead, are necessary. _Py2neo_'s batches provide nice methods that reflect the power of Cypher 2.0.0 new functionalities, like __get_or_create_path__ (=> `CREATE UNIQUE`), but those do not permit local references in the same batch. To juggle the problem three couples of data structures are used to store useful references and KEYs to identify the elements that are going to be added:

- <strong> `actors_dict`, `interactions_dict` (_dict_) </strong>: store actors and interactions references, are updated every new frame in the same batch and contain only real references
    - `actors_dict`: KEY, `actor_id`; VALUE, actor reference to DB
    - `interactions_dict`: KEY, `tuple(actor_id1, actor_id2)`; VALUE, interaction reference to DB  

- <strong>`actor_new`, `interaction_new` (_set_)</strong>: store the KEYs of the actors and the interactions, are used to know wich new actor/interaction have been encountered during the frame, so it is possible to create them in the database and store them into actors_dict/interactions_dict ready to be used for other purposes.

- <strong>`frame_actors`, `frame_interactions` (_dict_)</strong>: store all the actors/interactions present in the current frame, it's evident that it's independent from other two data structures
    - `frame_actors`: KEY, `actor_id`; VALUE, number of times in which an actor appears in the current frame
    - `frame_interactions`: KEY, `tuple(actor_id1, actor_id2)`; VALUE, number of times in which an interaction appears in the current frame

So the idea is to store in `*_new` and `frame_*` data structures everything concerning to the current frame, at creation of a new frame DB is updated according to those referencing with `*_dict` structures: creating new frames `*_new` will be used to create new nodes stored in `*_dict`, then will be used `frame_*` to use `*_dict` and creating new non-temporal relationships.

__Neo4jContactAdder__ also provide warm restart with some query to DB during initialization, so recreates `*_dict` structures and few other datas, the fact that a frame is created after a hot restart is notified by a property of frame nodes called session, incremented every hot restart.  

_Notes_ : batch commands are processed like transactions.


- _Parameters_:
    - `run_name` (_str_): name to identify the run in the DB
    - `start_time` (_int_): the time when the capture begins expressed in seconds since the epoch
    - `deltat` (positive _int_) : time granularity of the capture
    - `neo4j_rest` (_str_): address of the neo4j db
<br />  

- _Public methods to add contacts_:

    - __store_contact(self, contact)__ :
    
        It's intended for real use, when a script receives packets from network, parses it in a _Contact_ object and passes it to __store_contact__ function. It unpacks the _Contact_ object and for each contact contained calls __add_single_contact__ to add it to DB.
        - _Parameter_ :  
            - `contact` (_Contact object_ as defined in sociopatterns.loader)
        - _Returns_ : 
            - `None`
    <br />  
    
    - __add_single_contact(self, timestamp, actor_id1, actor_id2)__ :
    
        This method allows to add single real contacts, intended as two actors that met in a certain instant and no more like _Contact_ objects as __store_contact__ does. So it's useful also in a simulation when usually no metadatas and are needed unlike _Contact_ objects.
        - _Parameters_: 
            - `timestamp` (_int_): increasing timestamp, expressed in seconds since the epoch, identifying the instant of contact.
            - `actor_id1`, `actor_id2` (`__repr__`able objects): usually integer 16 bit IDs for the nature of _Contact_ objects.
        - _Returns_ :
            - `None`.

<h3>Improvements </h3>
- Handle insertions with timestamp not subsequent, this problem is real usually around frames switch and depends on real network latency. Now all inserions between the first timestamp received of a frame and the first timestamp of the subsequent are stored in the current frame, no matter if the timestamp indicates that contact deals with previous frame.
- Adding index support to speed up queries
- Increase the speed of the hot restart, ideas:
    - Use indexes to retrieve actors IDs
- Use two different threads to query the database and to mantain data structures
