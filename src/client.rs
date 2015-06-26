#![crate_name = "crowd_client"]

extern crate zmq;
extern crate rmp as msgpack;
extern crate rustc;
extern crate rustc_serialize;

extern crate collections;

use std::rand;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc::channel;
use std::io;
use std::old_io::timer;
use std::time::Duration;
use std::thread;
//use std::rand::{task_rng, Rng};

pub mod msg;

struct ConnTask {
    name: String,
    context: zmq::Context,
    requester: zmq::Socket,
    //poll_item: zmq::PollItem,
    rx_chan: Receiver<msg::CrowdMsg>,
    tx_chan: Sender<msg::CrowdMsg>
}

impl ConnTask {
    fn new(name: String, rx_chan: Receiver<msg::CrowdMsg>, tx_chan: Sender<msg::CrowdMsg>) -> ConnTask {
        let mut ctx = zmq::Context::new();
        let sock = ctx.socket(zmq::DEALER).unwrap();
        let x: u32 = rand::random();
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
        let msg = msg::CrowdMsg::Hello(self.name.clone());
        self.send(msg);
        self.recv();
        // TODO: raise errorr of conn failure
    }

    fn recv(&mut self) -> msg::CrowdMsg {
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
    rx_chan: Receiver<msg::CrowdMsg>,
    tx_chan: Sender<msg::CrowdMsg>
}

fn run_connection(name: String, rx_chan: Receiver<msg::CrowdMsg>, tx_chan: Sender<msg::CrowdMsg>) {
    let mut conn = ConnTask::new(name, rx_chan, tx_chan);
    conn.connect();
    let mut stop = false;
    loop {
        // check RX channel from master process
        println!("ch rx");
        let smsg = conn.rx_chan.try_recv();
        match smsg {
            Ok(m) => {
                match m {
                    msg::CrowdMsg::Bye => stop = true,
                    _ => stop = false
                }
                conn.send(m);
                //let rmsg = conn.recv();
                //conn.tx_chan.send(rmsg);
            }
            _ => ()
        }

        // check incoming msgs on connection to server
        let pi = conn.requester.as_poll_item(zmq::POLLIN);
        println!(">poll");
        let rc = zmq::poll([pi], 1000).ok().unwrap();
        if rc >= 0 {
            println!("poll {}", rc);
            println!("poll rev {}", pi.get_revents());
            if pi.get_revents() == zmq::POLLIN {
                let rmsg = conn.recv();
                println!("conn sent");
                conn.tx_chan.send(rmsg);
                println!("ch tx");
            }
        } else {
            println!("poll error {}", rc);
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

        thread::spawn(move || {
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
            msg::CrowdMsg::Response(0) => Ok(()),
            _ => Err("failed".to_string())
        }
    }

    fn lock(&mut self, path: String) -> Result<(), String> {
        println!("do lock");
        let msg = msg::CrowdMsg::Lock(path.clone());
        self.rpc(msg)
    }

    fn unlock(&mut self, path: String) -> Result<(), String>  {
        println!("do unlock");
        let msg = msg::CrowdMsg::Unlock(path.clone());
        self.rpc(msg)
    }

    fn keep_alive(&mut self) -> Result<(), String>  {
        println!("do keep alive");
        let msg = msg::CrowdMsg::KeepAlive;
        self.rpc(msg)
    }

    fn bye(&mut self) -> Result<(), String>  {
        println!("do bye");
        let msg = msg::CrowdMsg::Bye;
        self.rpc(msg)
    }
}


fn main() {
    println!("Connecting to crowd server...\n");

    let mut client = CrowdClient::new(String::from_str("myname"));
    let mut rng = rand::thread_rng();

    for x in range(0, 1000) {
        let r = client.lock(String::from_str("/my/path"));
        println!("lock {}", r);
        let ms: i64 = rng.gen_range(100, 1000).to_i64().unwrap();
        timer::sleep(Duration::milliseconds(ms));
        let r = client.unlock(String::from_str("/my/path"));
        println!("unlock {}", r);
        //client.keep_alive();
    }
    client.bye();
}
