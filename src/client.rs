#![crate_name = "crowd_client"]

extern crate zmq;
extern crate rmp as msgpack;
extern crate rustc_serialize;
extern crate rand;

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc::channel;
use std::thread::sleep_ms;
use std::thread;

use msgpack::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};

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
        //let sock = ctx.socket(zmq::DEALER).unwrap();
        let sock = ctx.socket(zmq::REQ).unwrap();
        let x = rand::random::<u32>();
        let identity = format!("worker-{}", x);
        println!("id {}", identity);
        sock.set_identity(identity.as_bytes()).unwrap();
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
        let mut payload = zmq::Message::new().unwrap();
        self.requester.recv(&mut payload, 0).unwrap();
        //let b = payload.to_bytes();
        //println!("recv {}", b);
        //let msg: msg::CrowdMsg = msgpack::from_msgpack(b).ok().unwrap();
        let mut decoder = Decoder::new(&payload[..]);
        let msg: msg::CrowdMsg = Decodable::decode(&mut decoder).ok().unwrap();
        msg
    }

    fn send(&mut self, msg: msg::CrowdMsg) {
        //let payload = msgpack::Encoder::to_msgpack(&msg).ok().unwrap();
        //self.requester.send(payload.as_slice(), 0).unwrap();
        let mut payload = [0u8; 13];
        msg.encode(&mut Encoder::new(&mut &mut payload[..])).unwrap();
        self.requester.send(&payload[..], 0).unwrap();
        //println!("sent");
    }
}

impl Drop for ConnTask {
    fn drop(&mut self) {
        self.requester.close().unwrap();
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
        let mut pis = [pi];
        println!(">poll");
        let rc = zmq::poll(&mut pis, 1000).ok().unwrap();
        if rc >= 0 {
            println!("poll {}", rc);
            println!("poll rev {}", pis[0].get_revents());
            if pis[0].get_revents() == zmq::POLLIN {
                let rmsg = conn.recv();
                println!("conn sent");
                conn.tx_chan.send(rmsg).unwrap();
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
        self.tx_chan.send(msg).unwrap();
        match self.rx_chan.recv().unwrap() {
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

    let mut client = CrowdClient::new("myname".to_string());
    //let mut rng = rand::thread_rng();

    for x in 0..1000 {
        let r = client.lock("/my/path".to_string()).unwrap();
        println!("lock {:?}", r);
        let ms = 200;//rng.gen_range(100, 1000);//.unwrap();
        sleep_ms(ms);
        let r = client.unlock("/my/path".to_string()).unwrap();
        println!("unlock {:?}", r);
        //client.keep_alive();
    }
    client.bye().unwrap();
}
