//#![allow(unused_must_use, dead_code)]
//#![feature(io, core, rustc_private)]
//extern crate serialize;

//extern crate rustc;
//extern crate "rustc-serialize" as rustc_serialize;
extern crate msgpack;

use rustc_serialize::{Encodable, Decodable, Encoder, Decoder};
//use serialize::{Encodable, Decodable, Encoder, Decoder};
use std::io::{Result, Error, ErrorKind};
//use msgpack::{Decoder};


//#[derive(RustcDecodable)]
//#[derive(RustcEncodable)]
#[derive(Debug)]
pub enum CrowdMsg {
    Hello(String),
    Lock(String),
    TryLock(String),
    Unlock(String),
    KeepAlive,
    Response(u32),
    Bye
}

//impl<R: Reader> rustc_serialize::Decodable<msgpack::Decoder<R>, Error> for CrowdMsg {
//impl<R: Reader> rustc_serialize::Decodable<R> for CrowdMsg {
//    fn decode<D, R: Reader>(s: &mut rustc_serialize::Decoder<R>) -> Result<CrowdMsg>
//        where D: rustc_serialize::Decoder<R> {
impl<R: Reader> Decodable<R> for CrowdMsg {
    fn decode<D, R: Reader>(s: &mut msgpack::Decoder<R>) -> Result<CrowdMsg> {
        //let cmd = try!(rustc_serialize::Decodable::decode(s));
        let cmd = try!(s.decode());
        match cmd {
            0 => {
                let arg = try!(Decodable::decode(s));
                Ok(CrowdMsg::Hello(arg))
            },
            1 => {
                let arg = try!(Decodable::decode(s));
                Ok(CrowdMsg::Lock(arg))
            },
            2 => {
                let arg = try!(Decodable::decode(s));
                Ok(CrowdMsg::TryLock(arg))
            },
            3 => {
                let arg = try!(Decodable::decode(s));
                Ok(CrowdMsg::Unlock(arg))
            },
            4 => Ok(CrowdMsg::KeepAlive),
            5 => {
                let arg = try!(Decodable::decode(s));
                Ok(CrowdMsg::Response(arg))
            },
            6 => Ok(CrowdMsg::Bye),
            _ => Err(Error{kind: ErrorKind::InvalidInput, desc: "some problem", detail: None})
        }
    }
}

impl Encodable for CrowdMsg {
    fn encode(&self, s: &mut msgpack::Encoder) -> Result<()> {
        match *self {
            CrowdMsg::Hello(ref name) => {
                try!((0).encode(s));
                name.encode(s)
            }
            CrowdMsg::Lock(ref path) => {
                try!((1).encode(s));
                path.encode(s)
            }
            CrowdMsg::TryLock(ref path) => {
                try!((2).encode(s));
                path.encode(s)
            }
            CrowdMsg::Unlock(ref path) => {
                try!((3).encode(s));
                path.encode(s)
            }
            CrowdMsg::KeepAlive => (4).encode(s),
            CrowdMsg::Response(ref code) => {
                try!((5).encode(s));
                code.encode(s)
            }
            CrowdMsg::Bye => (6).encode(s)
        }
    }
}
