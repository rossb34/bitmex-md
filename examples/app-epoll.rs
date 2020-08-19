use bitmex_md::bitmex_md_handler::BitmexMdHandler;
use bitmex_md::bitmex_message::parse;
use epoll_rs::{epoll_create1, epoll_ctl, epoll_wait, EpollEvent};
use llws::{generate_mask, FrameAssembler, FrameHeader, FrameWriter, OpCode};
use native_tls::TlsConnector;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::time::Instant;
use url::Url;

extern crate libc;

fn main() {
    // allocate bitmex handler
    let mut handler = BitmexMdHandler::new();

    // add symbol filter to consume data
    handler.add_symbol("XBTUSD");

    // connect
    // TODO: connection function which does all of this
    //  the client function is useful because it does the handshake with any arbitrary stream that
    //  implements the Read + Write traits
    let req = Url::parse("wss://www.bitmex.com/realtime").unwrap();

    let addrs = req
        .socket_addrs(|| match req.scheme() {
            "wss" => Some(443),
            _ => Some(80),
        })
        .unwrap();

    let sock = TcpStream::connect(addrs.as_slice()).unwrap();
    sock.set_nodelay(true);
    let connector = TlsConnector::builder().build().unwrap();
    let stream = connector.connect("bitmex.com", sock).unwrap();

    // do handshake
    let host = "www.bitmex.com";
    let path = "/realtime";
    let mut socket = match handler.client(host, path, stream) {
        Ok(stream) => stream,
        Err(_e) => {
            panic!("Handshake failed");
        }
    };
    socket.get_mut().set_nodelay(true).unwrap();
    socket.get_mut().set_nonblocking(true).unwrap();

    // send subscribe message
    let mut out_buffer = [0 as u8; 8192];

    let subscription_request = handler.get_subscription_request();
    let request_len = subscription_request.len();

    // prepare frame to send md subscription request
    let mut writer = FrameWriter::wrap(&mut out_buffer[..]);
    let frame_header = FrameHeader {
        is_final: true,
        op_code: OpCode::Text,
        mask: Some(generate_mask()),
        payload_length: request_len,
    };
    writer.push_back_header(&frame_header);
    writer.push_back_payload(subscription_request.as_bytes());

    let message_len = writer.frame_len();

    let epfd = epoll_create1(0).unwrap();

    // define event for read and write interest
    let event = EpollEvent {
        events: libc::EPOLLIN | libc::EPOLLOUT,
        data: socket.get_mut().as_raw_fd() as u64,
    };

    // register socket with epoll instance
    match epoll_ctl(
        epfd,
        libc::EPOLL_CTL_ADD,
        socket.get_mut().as_raw_fd(),
        event,
    ) {
        Err(e) => panic!("Unable to add socket to epoll instance {}", e),
        _ => {}
    }

    // vector of events to pass to epoll_wait
    let mut events: Vec<EpollEvent> = Vec::with_capacity(10);

    let mut flag = true;
    while flag {
        let nfds = epoll_wait(epfd, events.as_mut_slice(), 10, 0);
        match nfds {
            Ok(n) => {
                // println!("num events {}", n);
                unsafe { events.set_len(n as usize) };
                for _event in events.iter() {
                    // write the subscribe message
                    match socket.write(&out_buffer[0..message_len]) {
                        Ok(n) => {
                            println!("bytes written: {}", n);
                            flag = false;
                        }
                        Err(ref e) => {
                            println!("error sending subscription request message: {:?}", e)
                        }
                    }
                }
            }
            Err(e) => {
                println!("error {}", e);
            }
        }
    }

    // read loop
    // buffer to read data from socket
    let mut buffer = [0 as u8; 8192];
    let mut frame_assembler = FrameAssembler::new();

    println!("start read loop");
    loop {
        let nfds = epoll_wait(epfd, events.as_mut_slice(), 10, 0);
        match nfds {
            Ok(n) => {
                // println!("num events {}", n);
                unsafe { events.set_len(n as usize) };
                for _event in events.iter() {
                    match socket.read(&mut buffer[..]) {
                        Ok(0) => {}
                        Ok(n) => {
                            // frame_assembler.read(&buffer[0..n], frame_printer);

                            let start = Instant::now();
                            frame_assembler.read(&buffer[0..n], on_message);
                            let parse_end = Instant::now();
                            let parse_elapsed = parse_end.duration_since(start);
                            println!(
                                "elapsed duration {} nanos to read message",
                                parse_elapsed.as_nanos()
                            );
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
            }
            Err(e) => {
                println!("error {}", e);
            }
        }
    }
}

fn frame_printer(op_code: u8, payload: &[u8]) {
    println!("op code: {:?}", llws::OpCode::from(op_code));
    println!("{:?}", String::from_utf8_lossy(payload));
}

// fn frame_noop(_op_code: u8, _payload: &[u8]) {}

fn on_message(_op_code: u8, payload: &[u8]) {
    let _btmx_msg = parse(payload);
}
