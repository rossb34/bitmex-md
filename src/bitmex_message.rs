
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateEntry {
    symbol: String,
    id: i64,
    side: String,
    size: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateMessage {
    table: String,
    action: String,
    data: Vec<UpdateEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteEntry {
    symbol: String,
    id: i64,
    side: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteMessage {
    table: String,
    action: String,
    data: Vec<DeleteEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InsertEntry {
    symbol: String,
    id: i64,
    side: String,
    size: i64,
    price: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InsertMessage {
    table: String,
    action: String,
    data: Vec<InsertEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Types {
    symbol: String,
    id: String,
    side: String,
    size: String,
    price: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ForeignKeys {
    symbol: String,
    side: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Attributes {
    symbol: String,
    id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Filter {
    symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotMessage {
    table: String,
    action: String,
    keys: Vec<String>,
    types: Types,
    #[serde(rename = "foreignKeys")]
    foreign_keys: ForeignKeys,
    attributes: Attributes,
    filter: Filter,
    data: Vec<InsertEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Limit {
    remaining: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfoMessage {
    info: String,
    version: String,
    timestamp: String,
    docs: String,
    limit: Limit,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    op: String,
    args: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubscribeMessage {
    success: bool,
    subscribe: String,
    request: Request,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TradeSnapshotMessage {
    table: String,
    action: String,
    keys: Vec<String>,
    types: TradeTypes,
    #[serde(rename = "foreignKeys")]
    foreign_keys: ForeignKeys,
    attributes: TradeAttributes,
    filter: Filter,
    data: Vec<TradeEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TradeMessage {
    table: String,
    action: String,
    data: Vec<TradeEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TradeTypes {
    timestamp: String,
    symbol: String,
    side: String,
    size: String,
    price: String,
    #[serde(rename = "tickDirection")]
    tick_direction: String,
    #[serde(rename = "trdMatchID")]
    trd_match_id: String,
    #[serde(rename = "grossValue")]
    gross_value: String,
    #[serde(rename = "homeNotional")]
    home_notional: String,
    #[serde(rename = "foreignNotional")]
    foreign_notional: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TradeAttributes {
    timestamp: String,
    symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TradeEntry {
    timestamp: String,
    symbol: String,
    side: String,
    size: f64,
    price: f64,
    #[serde(rename = "tickDirection")]
    tick_direction: String,
    #[serde(rename = "trdMatchID")]
    trd_match_id: String,
    #[serde(rename = "grossValue")]
    gross_value: f64,
    #[serde(rename = "homeNotional")]
    home_notional: f64,
    #[serde(rename = "foreignNotional")]
    foreign_notional: f64,
}

pub enum BitmexMessage {
    Update(UpdateMessage),
    Delete(DeleteMessage),
    Insert(InsertMessage),
    Snapshot(SnapshotMessage),
    TradeSnapshot(TradeSnapshotMessage),
    Trade(TradeMessage),
    Info(InfoMessage),
    Subscribe(SubscribeMessage),
}

#[derive(Debug)]
pub enum ParseError {
    Invalid,
    InvalidAction,
    InvalidTable
}

fn parse(message: &str) -> Result<BitmexMessage, ParseError> {
    // peek at the type found at the beginning of the message
    let peek_type = &message[2..5];
    match peek_type {
        // info: info message type received when connection is established
        "inf" => {
            let info_msg: InfoMessage = serde_json::from_str(&message).unwrap();
            Ok(BitmexMessage::Info(info_msg))
        }
        // success: success message received when a subscription request is successful
        "suc" => {
            let subscribe_msg: SubscribeMessage = serde_json::from_str(&message).unwrap();
            Ok(BitmexMessage::Subscribe(subscribe_msg))
        }
        // table: table message received for a channel (e.g. orderBookL2, trade)
        "tab" => {
            // peek at the table type
            let peek_table = &message[10..13];
            match peek_table {
                // trade table
                "tra" => {
                    let peek_action = & message[27..30];
                    match peek_action {
                        // partial: trade snapshot (schema + last trade)
                        "par" => {
                            let trade_snapshot: TradeSnapshotMessage =
                                serde_json::from_str(&message).unwrap();
                            Ok(BitmexMessage::TradeSnapshot(trade_snapshot))
                        }
                        // insert: trade
                        "ins" => {
                            let trade: TradeMessage = serde_json::from_str(&message).unwrap();
                            Ok(BitmexMessage::Trade(trade))
                        }
                        _ => {
                            // invalid action for trade table
                            Err(ParseError::InvalidAction)
                        }
                    }
                }
                // order book l2 table
                "ord" => {
                    // peek at the action type to determine the message type to parse
                    let peek_action = &message[33..36];
                    match peek_action {
                        // partial: order book snapshot message
                        "par" => {
                            let snapshot: SnapshotMessage = serde_json::from_str(&message).unwrap();
                            Ok(BitmexMessage::Snapshot(snapshot))
                        }
                        // update
                        "upd" => {
                            let update: UpdateMessage = serde_json::from_str(&message).unwrap();
                            Ok(BitmexMessage::Update(update))
                        }
                        // insert
                        "ins" => {
                            let insert: InsertMessage = serde_json::from_str(&message).unwrap();
                            Ok(BitmexMessage::Insert(insert))
                        }
                        // delete
                        "del" => {
                            let delete: DeleteMessage = serde_json::from_str(&message).unwrap();
                            Ok(BitmexMessage::Delete(delete))
                        }
                        _ => {
                            // invalid action for order book l2 table
                            Err(ParseError::InvalidAction)
                        }
                    }
                }
                _ => {
                    // invalid table
                    Err(ParseError::InvalidTable)
                }
            }
        }
        _ => Err(ParseError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use crate::bitmex_message::{BitmexMessage, parse};

    #[test]
    fn parse_info_message() {
        let text = "{\"info\":\"Welcome to the BitMEX Realtime API.\",\"version\":\"2020-06-30T21:03:12.000Z\",\"timestamp\":\"2020-07-08T11:00:02.855Z\",\"docs\":\"https://www.bitmex.com/app/wsAPI\",\"limit\":{\"remaining\":39}}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Info(info_msg) => {
                    assert_eq!(info_msg.info, "Welcome to the BitMEX Realtime API.");
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_subscribe_message() {
        let text = "{\"success\":true,\"subscribe\":\"orderBookL2:XBTUSD\",\"request\":{\"op\":\"subscribe\",\"args\":[\"orderBookL2:XBTUSD\"]}}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Subscribe(subscribe_msg) => {
                    assert_eq!(subscribe_msg.success, true);
                    assert_eq!(subscribe_msg.subscribe, "orderBookL2:XBTUSD");
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_snapshot_message() {
        let text = "{\"table\":\"orderBookL2\",\"action\":\"partial\",\"keys\":[\"symbol\",\"id\",\"side\"],\"types\":{\"symbol\":\"symbol\",\"id\":\"long\",\"side\":\"symbol\",\"size\":\"long\",\"price\":\"float\"},\"foreignKeys\":{\"symbol\":\"instrument\",\"side\":\"side\"},\"attributes\":{\"symbol\":\"parted\",\"id\":\"sorted\"},\"filter\":{\"symbol\":\"XBTUSD\"},\"data\":[{\"symbol\":\"XBTUSD\",\"id\":8799070500,\"side\":\"Sell\",\"size\":384243,\"price\":9295},{\"symbol\":\"XBTUSD\",\"id\":8799070550,\"side\":\"Sell\",\"size\":62442,\"price\":9294.5},{\"symbol\":\"XBTUSD\",\"id\":8799070600,\"side\":\"Sell\",\"size\":162802,\"price\":9294},{\"symbol\":\"XBTUSD\",\"id\":8799070650,\"side\":\"Sell\",\"size\":67377,\"price\":9293.5},{\"symbol\":\"XBTUSD\",\"id\":8799070700,\"side\":\"Sell\",\"size\":19978,\"price\":9293},{\"symbol\":\"XBTUSD\",\"id\":8799070750,\"side\":\"Sell\",\"size\":56948,\"price\":9292.5},{\"symbol\":\"XBTUSD\",\"id\":8799070800,\"side\":\"Sell\",\"size\":82020,\"price\":9292},{\"symbol\":\"XBTUSD\",\"id\":8799070850,\"side\":\"Sell\",\"size\":832,\"price\":9291.5},{\"symbol\":\"XBTUSD\",\"id\":8799070900,\"side\":\"Sell\",\"size\":1186665,\"price\":9291},{\"symbol\":\"XBTUSD\",\"id\":8799070950,\"side\":\"Buy\",\"size\":1023444,\"price\":9290.5},{\"symbol\":\"XBTUSD\",\"id\":8799071000,\"side\":\"Buy\",\"size\":23490,\"price\":9290},{\"symbol\":\"XBTUSD\",\"id\":8799071050,\"side\":\"Buy\",\"size\":155749,\"price\":9289.5},{\"symbol\":\"XBTUSD\",\"id\":8799071100,\"side\":\"Buy\",\"size\":10723,\"price\":9289},{\"symbol\":\"XBTUSD\",\"id\":8799071150,\"side\":\"Buy\",\"size\":2113,\"price\":9288.5}]}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Snapshot(snapshot_message) => {
                    assert_eq!(snapshot_message.action, "partial");
                    assert_eq!(snapshot_message.data.len(), 14);
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_insert_message() {
        let text = "{\"table\":\"orderBookL2\",\"action\":\"insert\",\"data\":[{\"symbol\":\"XBTUSD\",\"id\":8798141850,\"side\":\"Sell\",\"size\":1,\"price\":18581.5}]}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Insert(insert_message) => {
                    assert_eq!(insert_message.action, "insert");
                    assert_eq!(insert_message.data.len(), 1);
                    let entry = insert_message.data.get(0).unwrap();
                    assert_eq!(entry.symbol, "XBTUSD");
                    assert_eq!(entry.id, 8798141850);
                    assert_eq!(entry.side, "Sell");
                    assert_eq!(entry.size, 1);
                    assert_eq!(entry.price, 18581.5);
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_delete_message() {
        let text = "{\"table\":\"orderBookL2\",\"action\":\"delete\",\"data\":[{\"symbol\":\"XBTUSD\",\"id\":8799594200,\"side\":\"Buy\"}]}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Delete(delete_message) => {
                    assert_eq!(delete_message.action, "delete");
                    assert_eq!(delete_message.data.len(), 1);
                    let entry = delete_message.data.get(0).unwrap();
                    assert_eq!(entry.symbol, "XBTUSD");
                    assert_eq!(entry.id, 8799594200);
                    assert_eq!(entry.side, "Buy");
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_update_message() {
        let text = "{\"table\":\"orderBookL2\",\"action\":\"update\",\"data\":[{\"symbol\":\"XBTUSD\",\"id\":8799065200,\"side\":\"Sell\",\"size\":182112},{\"symbol\":\"XBTUSD\",\"id\":8799065250,\"side\":\"Sell\",\"size\":19575}]}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Update(update_message) => {
                    assert_eq!(update_message.action, "update");
                    assert_eq!(update_message.data.len(), 2);
                    let entry1 = update_message.data.get(0).unwrap();
                    assert_eq!(entry1.symbol, "XBTUSD");
                    assert_eq!(entry1.id, 8799065200);
                    assert_eq!(entry1.side, "Sell");
                    assert_eq!(entry1.size, 182112);

                    let entry2 = update_message.data.get(1).unwrap();
                    assert_eq!(entry2.symbol, "XBTUSD");
                    assert_eq!(entry2.id, 8799065250);
                    assert_eq!(entry2.side, "Sell");
                    assert_eq!(entry2.size, 19575);
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_trade_snapshot() {
        let text = "{\"table\":\"trade\",\"action\":\"partial\",\"keys\":[],\"types\":{\"timestamp\":\"timestamp\",\"symbol\":\"symbol\",\"side\":\"symbol\",\"size\":\"long\",\"price\":\"float\",\"tickDirection\":\"symbol\",\"trdMatchID\":\"guid\",\"grossValue\":\"long\",\"homeNotional\":\"float\",\"foreignNotional\":\"float\"},\"foreignKeys\":{\"symbol\":\"instrument\",\"side\":\"side\"},\"attributes\":{\"timestamp\":\"sorted\",\"symbol\":\"grouped\"},\"filter\":{\"symbol\":\"XBTUSD\"},\"data\":[{\"timestamp\":\"2020-07-19T19:42:57.047Z\",\"symbol\":\"XBTUSD\",\"side\":\"Sell\",\"size\":446,\"price\":9155.5,\"tickDirection\":\"MinusTick\",\"trdMatchID\":\"3a90d7b2-8b2b-556f-0dc5-bfde052e240b\",\"grossValue\":4871212,\"homeNotional\":0.04871212,\"foreignNotional\":446}]}";
        // \"trdMatchID\":\"3a90d7b2-8b2b-556f-0dc5-bfde052e240b\",\"grossValue\":4871212,\"homeNotional\":0.04871212,\"foreignNotional\":446}]}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::TradeSnapshot(trade_snapshot_message) => {
                    assert_eq!(trade_snapshot_message.table, "trade");
                    assert_eq!(trade_snapshot_message.action, "partial");
                    let entry = trade_snapshot_message.data.get(0).unwrap();
                    assert_eq!(entry.timestamp, "2020-07-19T19:42:57.047Z");
                    assert_eq!(entry.symbol, "XBTUSD");
                    assert_eq!(entry.side, "Sell");
                    assert_eq!(entry.size, 446.0);
                    assert_eq!(entry.price, 9155.5);
                    assert_eq!(entry.tick_direction, "MinusTick");
                    assert_eq!(entry.trd_match_id, "3a90d7b2-8b2b-556f-0dc5-bfde052e240b");
                    assert_eq!(entry.gross_value, 4871212.0);
                    assert_eq!(entry.home_notional, 0.04871212);
                    assert_eq!(entry.foreign_notional, 446.0);
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }

    #[test]
    fn parse_trade_update() {
        let text = "{\"table\":\"trade\",\"action\":\"insert\",\"data\":[{\"timestamp\":\"2020-07-19T19:43:21.401Z\",\"symbol\":\"XBTUSD\",\"side\":\"Sell\",\"size\":16000,\"price\":9155.5,\"tickDirection\":\"ZeroMinusTick\",\"trdMatchID\":\"ec06df7b-0dc0-8181-f693-c9f39fb57e56\",\"grossValue\":174752000,\"homeNotional\":1.74752,\"foreignNotional\":16000}]}";
        let parsed_message = parse(&text);
        match parsed_message {
            Ok(m) => match m {
                BitmexMessage::Trade(trade_message) => {
                    assert_eq!(trade_message.table, "trade");
                    assert_eq!(trade_message.action, "partial");
                    let entry = trade_message.data.get(0).unwrap();
                    assert_eq!(entry.timestamp, "2020-07-19T19:43:21.401Z");
                    assert_eq!(entry.symbol, "XBTUSD");
                    assert_eq!(entry.side, "Sell");
                    assert_eq!(entry.size, 16000.0);
                    assert_eq!(entry.price, 9155.5);
                    assert_eq!(entry.tick_direction, "ZeroMinusTick");
                    assert_eq!(entry.trd_match_id, "ec06df7b-0dc0-8181-f693-c9f39fb57e56");
                    assert_eq!(entry.gross_value, 174752000.0);
                    assert_eq!(entry.home_notional, 1.74752);
                    assert_eq!(entry.foreign_notional, 16000.0);
                }
                _ => panic!("wrong message type"),
            },
            _ => panic!("message parser error"),
        }
    }
}
