use crate::bitmex_message::MarketDataSubscriptionRequest;
use llws::handshake::HandshakeError;
use std::io::{Read, Write};

pub struct BitmexMdHandler {
    symbols: Vec<String>,
}

impl BitmexMdHandler {
    pub fn new() -> Self {
        BitmexMdHandler { symbols: vec![] }
    }

    pub fn add_symbol(&mut self, symbol: &str) {
        self.symbols.push(String::from(symbol))
    }

    pub fn get_subscription_request(&self) -> String {
        let mut md_request = MarketDataSubscriptionRequest {
            op: String::from("subscribe"),
            args: vec![],
        };
        for symbol in self.symbols.iter() {
            // md_request.args.push(String::from("orderBookL2:") + symbol);
            md_request.args.push(String::from("trade:") + symbol);
        }
        serde_json::to_string(&md_request).unwrap()
    }

    // initiate client handshake over the given stream
    pub fn client<Stream>(
        &self,
        host: &str,
        path: &str,
        stream: Stream,
    ) -> Result<Stream, HandshakeError>
    where
        Stream: Read + Write,
    {
        llws::handshake::do_handshake(host, path, stream)
    }
}
