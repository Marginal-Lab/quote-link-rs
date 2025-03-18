use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::File;
use std::io::Write;
use link::convert::Quotation;
use link::quotation;
use tokio::select;


fn generate_filename(file_type: &str, timestamp: i64, freq: i64) -> String {
    let days = timestamp / (24 * 3600);
    let start_second = (timestamp % (24 * 3600)) / (freq * 60) * (freq * 60);
    let end_second = start_second + (freq * 60);
    format!("{}_{}_{}_{}.txt", file_type, days, start_second, end_second)
}


fn open_file(trade_type: &str, timestamp: i64, freq: i64) -> (String, File) {
    let filename = generate_filename(trade_type, timestamp, freq);
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&filename)
        .unwrap();
    (filename, file)
}


#[tokio::main]
async fn main() {
    // init record files
    let freq = 5;
    let mut global_timestamp = 0;
    let (mut filename_price, mut file_price) = open_file("Price", global_timestamp, freq);
    let (mut filename_trade, mut file_trade) = open_file("Trade", global_timestamp, freq);
    let (mut filename_depth, mut file_depth) = open_file("Depth", global_timestamp, freq);
    let (mut filename_others, mut file_others) = open_file("Others", global_timestamp, freq);

    // 初始化一个 HashMap
    let mut symbol_2_trade: HashMap<String, quotation::TradePrice> = HashMap::new();
    let mut symbol_2_depth: HashMap<String, quotation::Depths> = HashMap::new();

    tracing_subscriber::fmt::init();

    let client_config = link::config::get_from_filepath("tests/config.yaml").unwrap();
    let cli = link::client::Client::new(client_config);
    let push_rx = cli
        .connect(
            link::client::QuicheConfigBuilder::new()
                .build_in_recommend()
                .unwrap(),
        )
        .unwrap();
    let mut quotation_push_rx = link::convert::to_quotation(push_rx, 1024).await;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(500));
        loop {
            select! {
                t = ticker.tick() => {
                    tracing::debug!("async select ticker active at: {t:?}");
                }
                p = quotation_push_rx.recv() => {
                    if let Ok(quotation) = p {                 
                        // tracing::info!("async select recv quotation: {quotation:?}");       
                        match quotation {
                            Quotation::Trade(trade_price) => {
                                // 根据时间戳生成日志文件名称
                                global_timestamp = trade_price.timestamp; // 更新global_timestamp用于其他quotation的切片
                                // 检查字典中是否存在该符号
                                if let Some(last_trade_price) = symbol_2_trade.get(&trade_price.symbol) {
                                    // 新的时间戳大于旧的时间戳，写入旧数据
                                    if trade_price.timestamp > last_trade_price.timestamp {
                                        // 将last_trade_price记录下来
                                        let next_filename_trade = generate_filename("Trade", last_trade_price.timestamp, freq);
                                        // 判断文件名称是否一致
                                        if filename_trade == next_filename_trade {
                                        } else {
                                            file_trade = OpenOptions::new()
                                                .append(true)
                                                .create(true)
                                                .open(next_filename_trade.as_str())
                                                .unwrap();
                                            filename_trade = next_filename_trade;
                                        }
                                        // 写入文件
                                        // tracing::info!("write: {last_trade_price:?}");
                                        if let Err(e) = writeln!(file_trade,
                                            "TradePrice ( symbol: {}, timestamp: {}, price: {} , amount: {}, side: {}, total_amount: {}, total_balance: {}, exchange_id: {} )",
                                            last_trade_price.symbol, last_trade_price.timestamp, last_trade_price.price, last_trade_price.amount, last_trade_price.side, 
                                            last_trade_price.total_amount, last_trade_price.total_balance, last_trade_price.exchange_id, ) {
                                            eprintln!("Failed to write to file: {}", e);
                                        }
                                        symbol_2_trade.insert(trade_price.symbol.clone(), trade_price.clone());
                                        // tracing::info!("pushback: {trade_price:?}");
                                    }
                                    else
                                    {
                                        symbol_2_trade.insert(trade_price.symbol.clone(), trade_price.clone());
                                        // tracing::info!("update: {trade_price:?}");
                                    }
                                }
                                else
                                {
                                    symbol_2_trade.insert(trade_price.symbol.clone(), trade_price.clone());
                                    // tracing::info!("insert: {trade_price:?}");
                                }
                            }
                            Quotation::Price(snapshot) => {
                                // 有一些指数的信息！！！
                                // 根据时间戳生成日志文件名称
                                let next_filename_price = generate_filename("Price", snapshot.timestamp, freq);
                                // 判断文件名称是否一致
                                if filename_price == next_filename_price {
                                } else {
                                    file_price = OpenOptions::new()
                                        .append(true)
                                        .create(true)
                                        .open(next_filename_price.as_str())
                                        .unwrap();
                                        filename_price = next_filename_price;
                                }
                                // 写入文件
                                if let Err(e) = writeln!(file_price, "{:?}", snapshot) {
                                    eprintln!("Failed to write to file: {}", e);
                                }
                            }
                            Quotation::Depth(depth) => {
                                // 检查字典中是否存在该符号
                                if let Some(last_depth) = symbol_2_depth.get(&depth.symbol) {
                                    // 新的时间戳大于旧的时间戳，写入旧数据
                                    if (depth.sequence as i64 / 1000000000) > (last_depth.sequence as i64 / 1000000000) {
                                        // 将last_depth_price记录下来
                                        let next_filename_depth = generate_filename("Depth", last_depth.sequence as i64 / 1000000000, freq);
                                        // 判断文件名称是否一致
                                        if filename_depth == next_filename_depth {
                                        } else {
                                            file_depth = OpenOptions::new()
                                                .append(true)
                                                .create(true)
                                                .open(next_filename_depth.as_str())
                                                .unwrap();
                                            filename_depth = next_filename_depth;
                                        }
                                        // 写入文件
                                        if let Err(e) = writeln!(file_depth, "{:?}", depth) {
                                            eprintln!("Failed to write to file: {}", e);
                                        }
                                        symbol_2_depth.insert(depth.symbol.clone(), depth.clone());
                                    }
                                    else
                                    {
                                        symbol_2_depth.insert(depth.symbol.clone(), depth.clone());
                                    }
                                }
                                else
                                {
                                    symbol_2_depth.insert(depth.symbol.clone(), depth.clone());
                                }
                            }
                            _ => {
                                // 根据时间戳生成日志文件名称
                                let next_filename_others = generate_filename("Others", global_timestamp, freq);
                                // 判断文件名称是否一致
                                if filename_others == next_filename_others {
                                } else {
                                    file_others = OpenOptions::new()
                                        .append(true)
                                        .create(true)
                                        .open(next_filename_others.as_str())
                                        .unwrap();
                                        filename_others = next_filename_others;
                                }
                                // 写入文件
                                if let Err(e) = writeln!(file_others, "{:?}", quotation) {
                                    eprintln!("Failed to write to file: {}", e);
                                }
                            }
                        }
                    }

                }
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(10000)).await;
}
