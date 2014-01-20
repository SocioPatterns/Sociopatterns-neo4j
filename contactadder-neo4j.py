import time
from py2neo import neo4j


class Neo4jContactAdder:
    
    def __init__(self, run_name="", start_time=0, deltat=20, neo4j_rest="http://localhost:7474/db/data/", log_file_url=""):     
        self.RUN_NAME = run_name
        self.START_TIME = start_time
        self.DELTAT = deltat
        self.NEO4J_REST = neo4j_rest
        
        self.gdb = neo4j.GraphDatabaseService(neo4j_rest)
        self.batch = neo4j.WriteBatch(self.gdb)
        self.REF_NODE = self.gdb.get_or_create_index(neo4j.Node, "REF").get_or_create("ref_node", 0, {"DB creation": time.ctime(time.time())})
        self.RUN = self.REF_NODE.get_or_create_path("HAS_RUN", 
                                                    {"name": run_name, 
                                                     "type": "RUN"}).nodes[1]
        self.RUN.set_labels("Run")
        
        tline = self.gdb.match_one(start_node=self.RUN, rel_type="HAS_TIMELINE")
        
        
        if tline is None:
            self.TLINE = self.RUN.get_or_create_path("HAS_TIMELINE", 
                                                     {"start": start_time, 
                                                      "stop": start_time+deltat, 
                                                      "name": "TIMELINE", 
                                                      "type": "TIMELINE"}).nodes[1]
        else:
            self.TLINE = tline.end_node
            
        self.actors_dict = dict()
        self.interactions_dict = dict()
        
        self.actor_new = set()
        self.interaction_new = set()
        
        self.frame_actors = dict()
        self.frame_interactions = dict()
        
        query_str = "START run = node%s \
             MATCH run -[:RUN_FRAME]-> frames \
             RETURN max(frames.session), \
                    count(frames), \
                    last(collect(frames)), \
                    head(collect(frames)).timestamp" % self.RUN
        query = neo4j.CypherQuery(self.gdb, query_str)
        
        q = query.execute()
            
        if not q.data:
            self.session = 0
            self.frame_count = 0
            self.last_frame = ""
            self.last_timestamp = 0
        else:
            print "Reloading ",
            if not log_file_url:
                reload_time = self.__reload(q)
            else:
                with open(log_file_url,"a+") as log_file:
                    reload_time = self.__reload(q)              
                    log_file.write("%s, Actors, %d, Interactions, %d, %f, %f\n" % (time.ctime(time.time()), len(self.actors_dict), len(self.interactions_dict), reload_time, (len(self.actors_dict) + len(self.interactions_dict)) / reload_time))
            print "Reload completed in %d seconds, with %d actors and %d interactions." % (reload_time, len(self.actors_dict), len(self.interactions_dict))
        
    def __reload(self, q):
        start_reload = time.time()
        self.session = 1 + q[0][0]
        self.frame_count = q[0][1]
        self.last_frame = q[0][2]
        self.last_timestamp = q[0][3]        
        print ". ",
        query_str = "MATCH (actor:Actor) \
                     RETURN actor.actor, actor"
        query = neo4j.CypherQuery(self.gdb, query_str)
        for actor in query.stream():
            self.actors_dict[actor[0]] = actor[1]
        print ". ",
        query_str = "MATCH (interaction:Interaction)  \
                     RETURN interaction.actor1, \
                            interaction.actor2, \
                            interaction"
        query = neo4j.CypherQuery(self.gdb, query_str)
        for interaction in query.stream():
            self.interactions_dict[(interaction[0], interaction[1])] = interaction[2]
        print ". ",
        return time.time() - start_reload
    
    
    def __timestamp2frame_time(self, timestamp):
        return self.START_TIME + (timestamp - self.START_TIME) // self.DELTAT * self.DELTAT
    
    def __interaction_abs(self, actor_id1, actor_id2):
        return {"actor1": actor_id1,
                "actor2": actor_id2, 
                "type": "INTERACTION",
                "name": "INTERACTION_%s_%s" % (actor_id1, actor_id2)}
    
    def __actor_abs(self, actor_id):
        return {"actor": actor_id,
                "type" : "ACTOR",
                "name" : "actor_%s" % actor_id}
    
    def __create_new_nodes(self, batch):
        for interaction in self.interaction_new:
            batch.get_or_create_in_index(neo4j.Node,
                                           "Interactions",
                                            "interaction",
                                            "_".join(str(i) for i in interaction),
                                            self.__interaction_abs(*interaction))
        for actor in self.actor_new:
            batch.get_or_create_in_index(neo4j.Node, 
                                          "Actors", 
                                          "name", 
                                           actor, 
                                           self.__actor_abs(actor))
        
        new_batch = neo4j.WriteBatch(self.gdb)
        
        for response in batch.stream():
            if response['type'] == "INTERACTION":
                self.interactions_dict[(response['actor1'], response['actor2'])] = response
                new_batch.add_labels(response, "Interaction")
            elif response['type'] == "ACTOR":
                self.actors_dict[response['actor']] = response
                new_batch.add_labels(response, "Actor")
            else:
                print "ERROR: trying to insert into dicts %s type (instead INTERACTION or ACTOR)." % response['type']
        return new_batch
            
            
    
    def __create_new_relationships(self, batch):
        for frame_interaction in self.frame_interactions:
            batch.create_path(self.last_frame,
                              ("FRAME_INTERACTION", {"weight": self.frame_interactions[frame_interaction]}),
                              self.interactions_dict[frame_interaction])
        
        for interaction in self.interaction_new:
            batch.create_path(self.RUN, "RUN_INTERACTION", self.interactions_dict[interaction])
            batch.create_path(self.interactions_dict[interaction], "INTERACTION_ACTOR", self.actors_dict[interaction[0]])
            batch.create_path(self.interactions_dict[interaction], "INTERACTION_ACTOR", self.actors_dict[interaction[1]])
        
        for frame_actor in self.frame_actors:
            batch.create_path(self.last_frame, 
                              ("FRAME_ACTOR", {"presence": self.frame_actors[frame_actor]}), 
                              self.actors_dict[frame_actor])
        
        for actor in self.actor_new:
            batch.create_path(self.RUN, "RUN_ACTOR", self.actors_dict[actor])
        
        return batch
    
    def __reset_frame_data_struct(self):
        self.actor_new = set()
        self.interaction_new = set()
        
        self.frame_actors = dict()
        self.frame_interactions = dict()
        
    def __send_batch(self):
        batch = neo4j.WriteBatch(self.gdb)
        batch = self.__create_new_nodes(batch)
        batch = self.__create_new_relationships(batch)
        batch.run()
        self.__reset_frame_data_struct()   
        
        
    def __create_new_frame(self, frame_time):
        self.frame_count += 1
        self.last_timestamp = frame_time
        
        (year, month, day, hour, minute, second) = time.localtime(frame_time)[:6]
        frame_abs = {'type': 'FRAME',
                     'frame_id': 'FRAME_'+str(self.frame_count),
                     'year': year,
                     'month': month,
                     'day': day,
                     'hour': hour,
                     'minute': minute,
                     'second': second,
                     'time': time.ctime(frame_time),
                     'session': self.session,
                     'timestamp': frame_time,
                     'timestamp_end': frame_time + self.DELTAT,
                     'length': self.DELTAT}
        
        frame = self.TLINE.get_or_create_path(
                        ("YEAR",{"value": year}),  {"value": year, "type": "TIMELINE", "name": "YEAR"},
                        ("MONTH",{"value": month}), {"value": month, "type": "TIMELINE", "name": "MONTH"},
                        ("DAY",{"value": day}),   {"value": day, "type": "TIMELINE", "name": "DAY"},
                        ("HOUR",{"value": hour}),  {"value": hour, "type": "TIMELINE", "name": "HOUR"},
                        ("TIMELINE_INSTANCE",{"value": frame_time}), frame_abs
                        ).nodes[5]
        
        batch_frame = neo4j.WriteBatch(self.gdb)
        batch_frame.create_path(self.RUN, "RUN_FRAME", frame)
        if self.frame_count == 1:
            batch_frame.set_labels(frame, "first_frame")
        else:
            batch_frame.remove_label(self.last_frame, "last_frame")
            batch_frame.get_or_create_path(self.last_frame, "FRAME_NEXT", frame)
        
        self.last_frame = frame
        batch_frame.set_property(self.TLINE, "stop", frame_time + self.DELTAT)
        batch_frame.add_labels(frame, "y%s" % year, "m%s" % month, "d%s" % day, "h%s" % hour, "last_frame")
        batch_frame.run()
        
    def __add_interaction(self,actor_id1, actor_id2):
        if (actor_id1, actor_id2) not in self.interactions_dict:
            self.interaction_new.add((actor_id1, actor_id2))
        
        if (actor_id1, actor_id2) not in self.frame_interactions:
            self.frame_interactions[(actor_id1, actor_id2)] = 1
        else:
            self.frame_interactions[(actor_id1, actor_id2)] += 1
        
    def __add_actor(self,actor_id):
        if actor_id not in self.actors_dict:
            self.actor_new.add(actor_id)
            
        if actor_id not in self.frame_actors:
            self.frame_actors[actor_id] = 1
        else:
            self.frame_actors[actor_id] += 1
                
    def store_contact(self, contact):
        timestamp = contact.t
        id1 = contact.id
        for id2 in contact.seen_id:
            self.add_single_contact(timestamp, id1, id2)
    
    def add_single_contact(self, timestamp, actor_id1, actor_id2):
        actor_id1, actor_id2 = sorted([actor_id1, actor_id2])
        frame_time = self.__timestamp2frame_time(timestamp)
        if frame_time > self.last_timestamp:
            self.__send_batch()
            self.__create_new_frame(frame_time)
            
        self.__add_interaction(actor_id1, actor_id2)
        
        self.__add_actor(actor_id1)
        self.__add_actor(actor_id2)            
        

