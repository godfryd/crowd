#![crate_name = "crowd-client"]

extern crate zmq;
extern crate msgpack;

extern crate sync;
extern crate collections;

use std::rand;
use sync::comm::{Receiver, Sender};
use std::io;
use std::time::Duration;
use std::rand::{task_rng, Rng};

pub mod msg;

struct ConnTask {
    name: String,
    context: zmq::Context,
    requester: zmq::Socket,
    //poll_item: zmq::PollItem,
    rx_chan: sync::comm::Receiver<msg::CrowdMsg>,
    tx_chan: sync::comm::Sender<msg::CrowdMsg>
}

impl ConnTask {
    fn new(name: String, rx_chan: sync::comm::Receiver<msg::CrowdMsg>, tx_chan: sync::comm::Sender<msg::CrowdMsg>) -> ConnTask {
        let mut ctx = zmq::Context::new();
        let sock = ctx.socket(zmq::DEALER).unwrap();
        let x: uint = rand::random();
        let mut identity = String::from_str("worker-");
        identity = identity + x.to_string();
        println!("id {}", identity);
        sock.set_identity(identity.as_bytes());
        ConnTask {
            name: name,
            context: ctx,
            requester: sock,
            rx_chan: rx_chan,
            tx_chan: tx_chan
        }
    }
    fn connect(&mut self) {
        assert!(self.requester.connect("tcp://localhost:5555").is_ok());
        println!("Sending Hello");
        let msg = msg::Hello(self.name.clone());
        self.send(msg);
        self.recv();
        // TODO: raise errorr of conn failure
    }

    fn recv(&mut self) -> msg::CrowdMsg {
        let pi = self.requester.as_poll_item(zmq::POLLIN);
        let r: int = zmq::poll([pi], 100).ok().unwrap();

        let mut payload = zmq::Message::new();
        self.requester.recv(&mut payload, 0).unwrap();
        let b = payload.to_bytes();
        //println!("recv {}", b);
        let msg: msg::CrowdMsg = msgpack::from_msgpack(b).ok().unwrap();
        msg
    }

    fn send(&mut self, msg: msg::CrowdMsg) {
        let payload = msgpack::Encoder::to_msgpack(&msg).ok().unwrap();
        self.requester.send(payload.as_slice(), 0).unwrap();
        //println!("sent");
    }
}

impl Drop for ConnTask {
    fn drop(&mut self) {
        self.requester.close();
    }
}

struct CrowdClient {
    rx_chan: sync::comm::Receiver<msg::CrowdMsg>,
    tx_chan: sync::comm::Sender<msg::CrowdMsg>
}

fn run_connection(name: String, rx_chan: sync::comm::Receiver<msg::CrowdMsg>, tx_chan: sync::comm::Sender<msg::CrowdMsg>) {
    let mut conn = ConnTask::new(name, rx_chan, tx_chan);
    conn.connect();
    let mut stop = false;
    loop {
        let smsg = conn.rx_chan.try_recv();
        match smsg {
            Ok(m) => {
                match m {
                    msg::Bye => stop = true,
                    _ => stop = false
                }
                conn.send(m);
                let rmsg = conn.recv();
                conn.tx_chan.send(rmsg);
            }
            _ => ()
        }
        if stop {
            break;
        }
    }
}

impl CrowdClient {
    fn new(name: String) -> CrowdClient {
        let (tx1, rx1) = channel();
        let (tx2, rx2) = channel();

        spawn(proc() {
            run_connection(name, rx2, tx1);
        });

        CrowdClient {
            rx_chan: rx1,
            tx_chan: tx2
        }
    }

    fn rpc(&mut self, msg: msg::CrowdMsg) -> Result<(), String> {
        self.tx_chan.send(msg);
        match self.rx_chan.recv() {
            msg::Response(0) => Ok(()),
            _ => Err("failed".to_string())
        }
    }

    fn lock(&mut self, path: String) -> Result<(), String> {
        println!("do lock");
        let msg = msg::Lock(path.clone());
        self.rpc(msg)
    }

    fn unlock(&mut self, path: String) -> Result<(), String>  {
        println!("do unlock");
        let msg = msg::Unlock(path.clone());
        self.rpc(msg)
    }

    fn keep_alive(&mut self) -> Result<(), String>  {
        println!("do keep alive");
        let msg = msg::KeepAlive;
        self.rpc(msg)
    }

    fn bye(&mut self) -> Result<(), String>  {
        println!("do bye");
        let msg = msg::Bye;
        self.rpc(msg)
    }
}


fn main() {
    println!("Connecting to crowd server...\n");

    let mut client = CrowdClient::new(String::from_str("myname"));
    let mut rng = task_rng();

    for x in range(0i, 1000i) {
        let r = client.lock(String::from_str("/my/path"));
        println!("lock {}", r);
        let ms: i64 = rng.gen_range(100u, 1000u).to_i64().unwrap();
        io::timer::sleep(Duration::milliseconds(ms));
        let r = client.unlock(String::from_str("/my/path"));
        println!("unlock {}", r);
        //client.keep_alive();
    }
    client.bye();
}
