extern crate msgpack;
extern crate serialize;

use std::io::{IoResult, IoError, InvalidInput};

//#[deriving(Decodable)]
//#[deriving(Encodable)]
pub enum CrowdMsg {
    Hello(String),
    Lock(String),
    TryLock(String),
    Unlock(String),
    KeepAlive,
    Response(uint),
    Bye
}

impl<'a> serialize::Decodable<msgpack::Decoder<'a>, IoError> for CrowdMsg {
    fn decode(s: &mut msgpack::Decoder<'a>) -> IoResult<CrowdMsg> {
        let cmd = try!(serialize::Decodable::decode(s));
        match cmd {
            0u => {
                let arg = try!(serialize::Decodable::decode(s));
                Ok(Hello(arg))
            },
            1u => {
                let arg = try!(serialize::Decodable::decode(s));
                Ok(Lock(arg))
            },
            2u => {
                let arg = try!(serialize::Decodable::decode(s));
                Ok(TryLock(arg))
            },
            3u => {
                let arg = try!(serialize::Decodable::decode(s));
                Ok(Unlock(arg))
            },
            4u => Ok(KeepAlive),
            5u => {
                let arg = try!(serialize::Decodable::decode(s));
                Ok(Response(arg))
            },
            6u => Ok(Bye),
            _ => Err(IoError{kind: InvalidInput, desc: "some problem", detail: None})
        }
    }
}

impl<'a> serialize::Encodable<msgpack::Encoder<'a>, IoError> for CrowdMsg {
    fn encode(&self, s: &mut msgpack::Encoder<'a>) -> IoResult<()> {
        match *self {
            Hello(ref name) => {
                try!(msgpack::Unsigned(0).encode(s));
                name.encode(s)
            }
            Lock(ref path) => {
                try!(msgpack::Unsigned(1).encode(s));
                path.encode(s)
            }
            TryLock(ref path) => {
                try!(msgpack::Unsigned(2).encode(s));
                path.encode(s)
            }
            Unlock(ref path) => {
                try!(msgpack::Unsigned(3).encode(s));
                path.encode(s)
            }
            KeepAlive => msgpack::Unsigned(4).encode(s),
            Response(ref code) => {
                try!(msgpack::Unsigned(5).encode(s));
                code.encode(s)
            }
            Bye => msgpack::Unsigned(6).encode(s)
        }
    }
}
