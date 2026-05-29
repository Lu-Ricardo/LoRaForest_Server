use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::Mutex,
    time::Duration,
};

use serialport::{DataBits, Parity, StopBits};

static SERIAL_DATA_BUFFER: Mutex<String> = Mutex::new(String::new());
static STOP_FLAG: Mutex<bool> = Mutex::new(true);

enum WriteType {
    Log,
    Data,
}

#[macro_export]
macro_rules! log_out {
    ($($arg:tt)*) => {
        //为输出打上时间戳
        println!("[{}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            format!($($arg)*)
        )
    };
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let baud_rate: u32 = args[2].trim().parse().expect("波特率参数错误");
    start_read_serialport(args[1].to_string(), baud_rate);
    loop {
        //如果连接断开了就重启程序，确保程序一直运行
        if *STOP_FLAG.lock().unwrap() {
            start_tcp_server();
            *STOP_FLAG.lock().unwrap() = false;
        }
    }
}

fn write_file(data: &String, datatype: WriteType) {
    //位于程序运行目录的 data 目录下
    let savepath = Path::new("data");
    let data_path = savepath.join("data.csv");
    let log_path = savepath.join("log");
    if !savepath.exists() || savepath.is_file() {
        match fs::create_dir(savepath) {
            Ok(_) => log_out!("已创建Data文件夹以存放数据"),
            Err(e) => {
                log_out!("文件夹创建失败，{}", e.to_string());
                return;
            }
        };
    }

    let mut log_file = match OpenOptions::new().create(true).append(true).open(&log_path) {
        Ok(file) => file,
        Err(e) => {
            log_out!("{:?} 文件创建失败, {}", &log_path, e.to_string());
            return;
        }
    };
    let mut data_file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&data_path)
    {
        Ok(file) => file,
        Err(e) => {
            log_out!("{:?} 文件创建失败，报错原因: {}", &data_path, e.to_string());
            return;
        }
    };

    match datatype {
        WriteType::Data => {
            let _ = data_file.write(data.as_bytes());
        }
        WriteType::Log => {
            let _ = log_file.write(data.as_bytes());
        }
    }
}

fn start_tcp_server() {
    let listener = TcpListener::bind("0.0.0.0:8899").unwrap();
    match wait_connect(&listener) {
        Ok(send_stream) => {
            let receive_stream = send_stream.try_clone().unwrap();
            //启动TCP收发线程
            tcp_receive_handle(receive_stream);
            tcp_send_handle(send_stream);
        }
        Err(e) => log_out!("连接出错，{}", e.to_string()),
    }
}

fn tcp_send_handle(stream: TcpStream) {
    std::thread::spawn(move || {
        loop {
            if *STOP_FLAG.lock().unwrap() {
                break;
            }
            let mut data = SERIAL_DATA_BUFFER.lock().unwrap();
            if *&data.len() > 0 {
                send_data(&stream, &data.as_bytes());
                *&data.clear();
            }
        }
    });
}

fn tcp_receive_handle(stream: TcpStream) {
    std::thread::spawn(move || {
        loop {
            let data = recive_data(&stream);
            //收到断开连接的信号
            if data == "Disconnect" {
                *STOP_FLAG.lock().unwrap() = true;
                break;
            }
            write_file(&data, WriteType::Data);
        }
    });
}

fn get_complete_line(buffer: &[u8], lines: &mut String) -> String {
    let temp = String::from_utf8_lossy(buffer);
    lines.push_str(&temp);
    if let Some(new_line) = lines.find("\r\n") {
        let complete_line = &lines.drain(..=new_line).collect::<String>();
        lines.clear();
        complete_line.clone()
    } else {
        String::new()
    }
}

fn start_read_serialport(port: String, baud_rate: u32) {
    let serial_port = serialport::new(port, baud_rate)
        .data_bits(DataBits::Eight)
        .stop_bits(StopBits::One)
        .timeout(Duration::from_secs(10))
        .parity(Parity::None);
    let mut sp = match serial_port.open() {
        Ok(sp) => sp,
        Err(e) => {
            log_out!("无法打开串口, {}", e.to_string());
            return;
        }
    };
    std::thread::spawn(move || {
        let mut raw_buffer = [0; 64];
        let mut lines = String::new();
        loop {
            if let Ok(size) = &mut sp.read(&mut raw_buffer) {
                let complete_data =
                    get_complete_line(&mut raw_buffer[..*size], &mut lines).to_string();
                write_file(&complete_data, WriteType::Data);
                (*SERIAL_DATA_BUFFER.lock().unwrap()).clear();
                (*SERIAL_DATA_BUFFER.lock().unwrap()).push_str(&complete_data);
            }
        }
    });
}

fn recive_data(stream: &TcpStream) -> String {
    let mut stream = stream;
    let mut buffer = [0; 64];
    let size = stream.read(&mut buffer).unwrap();
    String::from_utf8_lossy(&mut buffer[..size]).to_string()
}

fn send_data(stream: &TcpStream, data: &[u8]) -> bool {
    let mut stream = stream;
    match stream.write(&data) {
        Ok(_) => true,
        Err(e) => {
            log_out!("发送失败，{}", e.to_string());
            false
        }
    }
}

fn wait_connect(listener: &TcpListener) -> Result<TcpStream, std::io::Error> {
    log_out!("等待客户端连接");
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let mut data = recive_data(&stream);
        if data == "It's from LoRaForest Client!" {
            send_data(&stream, b"It's from LoRaForest Server!");
            data = recive_data(&stream);
            if data == "Ok" {
                log_out!("连接成功，目标地址:{}", stream.peer_addr().unwrap());
                return Ok(stream);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "无可用连接",
    ))
}
