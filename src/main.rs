#![crate_name = "crowd"]

/// Hello World server in Rust
/// Binds REP socket to tcp://*:5555
/// Expects "Hello" from client, replies with "World"

extern crate zmq;
extern crate msgpack;
extern crate time;

//use std::io;
//use std::time::duration::Duration;
use std::collections::HashMap;
use std::collections::TreeMap;
use std::collections::dlist::DList;
use std::collections::{RingBuf, Deque};
use std::time::Duration;

pub mod msg;

struct DirectedCrowdMsg {
    client_id: String,
    msg: msg::CrowdMsg
}

struct Session {
    client_id: String
}

struct Engine {
    sessions: HashMap<String, Session>,
    locks: HashMap<String, String>,
    waiters: HashMap<String, RingBuf<String>>,
    //time_queue: TreeMap<time::Timespec, DirectedCrowdMsg>,
    out_queue: DList<DirectedCrowdMsg>
}

impl Engine {
    fn enqueue(&mut self, client_id: &String, resp: uint) {
        self.out_queue.push(DirectedCrowdMsg { client_id: client_id.clone(), msg: msg::Response(resp) });
    }
    fn handle_hello(&mut self, client_id: &String, name: &String) {
        self.sessions.insert(client_id.clone(), Session { client_id: client_id.clone() });
        self.enqueue(client_id, 0);
        println!("{}: name {}", client_id, name);
    }

    fn handle_lock(&mut self, client_id: &String, path: &String) {
        if !self.locks.contains_key(path) {
            self.locks.insert(path.clone(), client_id.clone());
            self.enqueue(client_id, 0);
            println!("{}: locked path {}", client_id, path);
        } else {
            println!("{}: delayed lock on {} - already locked", client_id, path);
            //let mut t = time::get_time();
            //t.add(Duration::seconds(1))
            //self.time_queue.insert(t, DirectedCrowdMsg { client_id: client_id, msg: msg });
            if !self.waiters.contains_key(path) {
                let mut l = RingBuf::new();
                l.push(client_id.clone());
                self.waiters.insert(path.clone(), l);
            } else {
                self.waiters.get_mut(path).push(client_id.clone());
            }
            //self.enqueue(client_id, 1);
        }
    }

    fn handle_trylock(&mut self, client_id: &String, path: &String) {
        if !self.locks.contains_key(path) {
            self.locks.insert(path.clone(), client_id.clone());
            self.enqueue(client_id, 0);
            println!("{}: trylocked path {}", client_id, path);
        } else {
            self.enqueue(client_id, 1);
            println!("{}: failed trylock on {}", client_id, path);
        }
    }

    fn handle_unlock(&mut self, client_id: &String, path: &String) {
        let entry = self.locks.find_copy(path);
        match entry {
            Some(cid) => {
                if cid.as_slice() != client_id.as_slice() {
                    self.enqueue(client_id, 1);
                    println!("{}: EEEE1 failed unlock on {} - locked by {}", client_id, path, cid);
                } else {
                    self.locks.remove(path);
                    self.enqueue(client_id, 0);
                    println!("{}: unlocked path {}", client_id, path);

                    // notify first waiter
                    if self.waiters.contains_key(path) {
                        let c = self.waiters.get_mut(path).pop_front();
                        if c.is_some() {
                            let new_client_id = c.unwrap();
                            self.locks.insert(path.clone(), new_client_id.clone());
                            self.enqueue(&new_client_id, 0);
                            println!("{}: lock path {} just after unlock", new_client_id, path);
                        }
                    }
                }
            }
            None => {
                self.enqueue(client_id, 1);
                println!("{}: EEEE2 failed unlock on {}", client_id, path);
            }
        }
    }

    fn handle_keepalive(&mut self, client_id: &String) {
        self.enqueue(client_id, 0);
        println!("{}: keepalive", client_id);
    }

    fn handle_bye(&mut self, client_id: &String) {
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
        out_queue: DList::new()
    };

    //let now = time::get_time(); // -> Timespec
    //time_queue.insert(now, "bar");

    let mut context = zmq::Context::new();
    let mut responder = context.socket(zmq::ROUTER).unwrap();

    assert!(responder.bind("tcp://*:5555").is_ok());

    let mut msg = zmq::Message::new();
    loop {
        let pi = responder.as_poll_item(zmq::POLLIN);
        zmq::poll([pi], 100).ok().unwrap();

        //println!("waiting for msg");
        let msg_id = responder.recv_msg(0).unwrap();
        //println!("id {}", msg_id.to_string());
        let client_id = msg_id.to_string();
        msg = responder.recv_msg(0).unwrap();
        let b = msg.to_bytes();
        //println!("msg {}", b);
        let dec: msg::CrowdMsg = msgpack::from_msgpack(b).ok().unwrap();
        //println!("3");

        match dec {
            msg::Hello(name) => eng.handle_hello(&client_id, &name),
            msg::Lock(path) => eng.handle_lock(&client_id, &path),
            msg::TryLock(path) => eng.handle_trylock(&client_id, &path),
            msg::Unlock(path) => eng.handle_unlock(&client_id, &path),
            msg::KeepAlive => eng.handle_keepalive(&client_id),
            msg::Response(_) => eng.handle_keepalive(&client_id),
            msg::Bye => eng.handle_bye(&client_id)
        };

        eng.check_pending();

        for dm in eng.out_queue.iter() {
            responder.send_str(dm.client_id.as_slice(), zmq::SNDMORE).unwrap();
            //let msg = msg::Response(resp);
            let payload = msgpack::Encoder::to_msgpack(&dm.msg).ok().unwrap();
            //println!("send {}", payload.as_slice());
            responder.send(payload.as_slice(), 0).unwrap();
        }
        eng.out_queue.clear();

    }
}
