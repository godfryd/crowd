#![crate_name = "crowd"]

/// Hello World server in Rust
/// Binds REP socket to tcp://*:5555
/// Expects "Hello" from client, replies with "World"

extern crate rustc_serialize;
extern crate zmq;
extern crate rmp as msgpack;
//extern crate time;

//use std::io;
//use std::time::duration::Duration;
use std::collections::HashMap;
//use std::collections::BTreeMap;
//use std::collections::linked_list::LinkedList;
use std::collections::{VecDeque};
//use std::time::Duration;
use msgpack::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};
use rustc_serialize::json;

pub mod msg;

struct DirectedCrowdMsg {
    client_id: Vec<u8>,
    msg: msg::CrowdMsg
}

struct Session {
    client_id: Vec<u8>
}

struct Engine {
    sessions: HashMap<Vec<u8>, Session>,
    locks: HashMap<String, Vec<u8>>,
    waiters: HashMap<String, VecDeque<Vec<u8>>>,
    //time_queue: TreeMap<time::Timespec, DirectedCrowdMsg>,
    out_queue: VecDeque<DirectedCrowdMsg>
}

impl Engine {
    fn enqueue(&mut self, client_id: Vec<u8>, resp: u32) {
        self.out_queue.push_back(DirectedCrowdMsg { client_id: client_id.clone(), msg: msg::CrowdMsg::Response(resp) });
    }
    fn handle_hello(&mut self, client_id: Vec<u8>, name: String) {
        self.sessions.insert(client_id.clone(), Session { client_id: client_id.clone() });
        println!("{:?}: name {}", String::from_utf8(client_id.clone()).unwrap(), name);
        self.enqueue(client_id, 0);
    }

    fn handle_lock(&mut self, client_id: Vec<u8>, path: String) {
        if !self.locks.contains_key(&path) {
            self.locks.insert(path.clone(), client_id.clone());
            println!("{:?}: locked path {}", String::from_utf8(client_id.clone()).unwrap(), path);
            self.enqueue(client_id, 0);
        } else {
            println!("{:?}: blocked lock on {} - already locked", String::from_utf8(client_id.clone()).unwrap(), path);
            //let mut t = time::get_time();
            //t.add(Duration::seconds(1))
            //self.time_queue.insert(t, DirectedCrowdMsg { client_id: client_id, msg: msg });
            if !self.waiters.contains_key(&path) {
                let mut l = VecDeque::new();
                l.push_back(client_id.clone());
                self.waiters.insert(path.clone(), l);
            } else {
                self.waiters.get_mut(&path).unwrap().push_back(client_id.clone());
            }
            //self.enqueue(client_id, 1);
        }
    }

    fn handle_trylock(&mut self, client_id: Vec<u8>, path: String) {
        if !self.locks.contains_key(&path) {
            self.locks.insert(path.clone(), client_id.clone());
            println!("{:?}: trylocked path {}", String::from_utf8(client_id.clone()).unwrap(), path);
            self.enqueue(client_id, 0);
        } else {
            println!("{:?}: failed trylock on {}", String::from_utf8(client_id.clone()).unwrap(), path);
            self.enqueue(client_id, 1);
        }
    }

    fn handle_unlock(&mut self, client_id: Vec<u8>, path: String) {
        let entry = self.locks.get(&path).cloned();
        match entry {
            Some(ref cid) => {
                if *cid != client_id {
                    println!("{:?}: EEEE1 failed unlock on {} - locked by {:?}", String::from_utf8(client_id.clone()).unwrap(), path, cid);
                    self.enqueue(client_id, 1);
                } else {
                    self.locks.remove(&path);
                    println!("{:?}: unlocked path {}", String::from_utf8(client_id.clone()).unwrap(), path);
                    self.enqueue(client_id, 0);

                    // notify first waiter
                    if self.waiters.contains_key(&path) {
                        let c = self.waiters.get_mut(&path).unwrap().pop_front();
                        if c.is_some() {
                            let new_client_id = c.unwrap();
                            self.locks.insert(path.clone(), new_client_id.clone());
                            println!("{:?}: lock path {} just after unlock", String::from_utf8(new_client_id.clone()).unwrap(), path);
                            self.enqueue(new_client_id, 0);
                        }
                    }
                }
            }
            None => {
                println!("{:?}: EEEE2 failed unlock on {}", String::from_utf8(client_id.clone()).unwrap(), path);
                self.enqueue(client_id, 1);
            }
        }
    }

    fn handle_keepalive(&mut self, client_id: Vec<u8>) {
        println!("{:?}: keepalive", String::from_utf8(client_id.clone()).unwrap());
        self.enqueue(client_id, 0);
    }

    fn handle_bye(&mut self, client_id: Vec<u8>) {
        self.sessions.insert(client_id.clone(), Session { client_id: client_id.clone() });
    }

    fn check_pending(&mut self) {
    }
}


fn main() {
    let mut eng = Engine {
        sessions: HashMap::new(),
        locks: HashMap::new(),
        //time_queue: TreeMap::new(),
        waiters: HashMap::new(),
        out_queue: VecDeque::new()
    };

    //let now = time::get_time(); // -> Timespec
    //time_queue.insert(now, "bar");

    let mut context = zmq::Context::new();
    let mut responder = context.socket(zmq::ROUTER).unwrap();

    assert!(responder.bind("tcp://*:5555").is_ok());

    let mut msg_addr = zmq::Message::new().unwrap();
    let mut msg_empty = zmq::Message::new().unwrap();
    let mut msg_data = zmq::Message::new().unwrap();
    let mut pis = [responder.as_poll_item(zmq::POLLIN)];
    loop {
        zmq::poll(&mut pis, 100).ok().unwrap();

        //println!("waiting for msg 1");
        responder.recv(&mut msg_addr, 0).unwrap();
        responder.recv(&mut msg_empty, 0).unwrap();
        responder.recv(&mut msg_data, 0).unwrap();
        //println!("id {:?}", &msg_addr[..]);
        //println!("data {:?}", &msg_data[..]);
        let client_id = msg_addr.to_vec();

        let mut decoder = Decoder::new(&msg_data[..]);
        let res = Decodable::decode(&mut decoder);
        if res.is_err() {
            println!("{:?}: erred msg {:?}", String::from_utf8(client_id.clone()).unwrap(), res.unwrap_err());
            continue;
        }
        let dec: msg::CrowdMsg = res.ok().unwrap();
        //println!("3");

        match dec {
            msg::CrowdMsg::Hello(name) => eng.handle_hello(client_id, name),
            msg::CrowdMsg::Lock(path) => eng.handle_lock(client_id, path),
            msg::CrowdMsg::TryLock(path) => eng.handle_trylock(client_id, path),
            msg::CrowdMsg::Unlock(path) => eng.handle_unlock(client_id, path),
            msg::CrowdMsg::KeepAlive => eng.handle_keepalive(client_id),
            msg::CrowdMsg::Response(_) => eng.handle_keepalive(client_id),
            msg::CrowdMsg::Bye => eng.handle_bye(client_id)
        };

        eng.check_pending();

        for dm in eng.out_queue.iter() {
            responder.send(&dm.client_id[..], zmq::SNDMORE).unwrap();
            responder.send(&msg_empty[..], zmq::SNDMORE).unwrap();
            let mut payload: Vec<u8> = Vec::new();
            let enc = dm.msg.encode(&mut Encoder::new(&mut payload)).unwrap();
            //let enc = json::encode(&dm.msg).unwrap();
            responder.send(&payload[..], 0).unwrap();
        }
        eng.out_queue.clear();

    }
}
