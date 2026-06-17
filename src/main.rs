use regex::Regex;
use serialport::{DataBits, ErrorKind, Parity, StopBits};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::exit,
    sync::Mutex,
    time::Duration,
};

static SERIAL_DATA_BUFFER: Mutex<String> = Mutex::new(String::new());
static STOP_FLAG: Mutex<bool> = Mutex::new(true);

enum WriteType {
    Log,
    Data,
}

struct Data {
    value: [Option<f32>; 5],
    count: usize,
}

impl Data {
    fn new() -> Self {
        Self {
            value: [None, None, None, None, None],
            count: 0,
        }
    }

    fn store(&mut self, index: usize, data: f32) {
        if self.value[index].is_none() {
            self.count += 1;
        }
        self.value[index] = Some(data);
    }

    fn clear(&mut self) {
        self.count = 0;
        self.value = [None, None, None, None, None];
    }
}

#[macro_export]
macro_rules! log_out {
    ($($arg:tt)*) => {
        {
            let msg = format!($($arg)*);
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            let formatted_msg = format!("[{}] {}\n", timestamp, msg);

            // 输出到控制台
            print!("{}", formatted_msg);

            // 写入文件
            $crate::write_file(&formatted_msg, $crate::WriteType::Log);
        }
    };
}

fn main() {
    log_out!("准备启动服务器...");
    let args: Vec<String> = std::env::args().collect();
    let baud_rate: u32 = args[2].trim().parse().expect("波特率参数错误");
    let listen_port: u16 = args[3].trim().parse().expect("端口号无效");
    start_read_serialport(args[1].to_string(), baud_rate);
    loop {
        //如果连接断开了就重启程序，确保程序一直运行
        if *STOP_FLAG.lock().unwrap() {
            start_tcp_server(listen_port);
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

    if !data_path.exists() {
        match File::create(&data_path) {
            Ok(mut file) => {
                let _ = file.write(b"Date,Time,Temperature,Rain,Light,Press,Humidity\n");
            }
            Err(e) => {
                log_out!("无法打开Data文件, {}", e.to_string());
            }
        }
    }

    if !log_path.exists() {
        match File::create(&log_path) {
            Ok(_) => {}
            Err(e) => {
                println!("无法打开Log文件, {}", e.to_string());
            }
        }
    }

    let mut log_file = match OpenOptions::new().append(true).open(&log_path) {
        Ok(file) => file,
        Err(e) => {
            log_out!("{:?} 文件创建失败, {}", &log_path, e.to_string());
            return;
        }
    };
    let mut data_file = match OpenOptions::new().append(true).open(&data_path) {
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

fn start_tcp_server(listen_port: u16) {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), listen_port);
    let listener = match TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(e) => {
            if e.kind() == io::ErrorKind::AddrInUse {
                log_out!("{}端口已被占用，请尝试其他端口重新启动程序...", listen_port);
                exit(0);
            }
            return;
        }
    };
    log_out!("服务器启动成功, 地址: {}", addr.to_string());
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
                tcp_send_data(&stream, &data.as_bytes());
                *&data.clear();
            }
        }
    });
}

fn tcp_receive_handle(stream: TcpStream) {
    std::thread::spawn(move || {
        loop {
            let data = tcp_recive_data(&stream);
            //收到断开连接的信号
            if data == "Disconnect" {
                *STOP_FLAG.lock().unwrap() = true;
                break;
            }
            //write_file(&data, WriteType::Data);
        }
    });
}

fn get_complete_line(buffer: &[u8], lines: &mut String) -> bool {
    let temp = String::from_utf8_lossy(buffer);
    lines.push_str(&temp);
    if let Some(_) = lines.find(";") {
        true
    } else {
        false
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
        let mut data = Data::new();
        loop {
            if let Ok(size) = &mut sp.read(&mut raw_buffer) {
                if get_complete_line(&mut raw_buffer[..*size], &mut lines) {
                    //write_file(&lines, WriteType::Data);
                    group_data(&lines, &mut data);
                    (*SERIAL_DATA_BUFFER.lock().unwrap()).clear();
                    (*SERIAL_DATA_BUFFER.lock().unwrap()).push_str(&lines);
                    lines.clear();
                }
            }
        }
    });
}

fn tcp_recive_data(stream: &TcpStream) -> String {
    let mut stream = stream;
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer).unwrap();
    String::from_utf8_lossy(&mut buffer[..size]).to_string()
}

fn tcp_send_data(stream: &TcpStream, data: &[u8]) -> bool {
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
        let mut data = tcp_recive_data(&stream);
        if data == "It's from LoRaForest Client!" {
            tcp_send_data(&stream, b"It's from LoRaForest Server!");
            data = tcp_recive_data(&stream);
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

fn group_data(data: &String, data_write_buffer: &mut Data) {
    //正则表达式
    let temperature_re: Regex = Regex::new(r"Temperature: (\d+?)C;").unwrap();
    let rain_re: Regex = Regex::new(r"Rain: (\d+?);").unwrap();
    let light_re: Regex = Regex::new(r"Light: (\d+\.?\d*);").unwrap();
    let press_re: Regex = Regex::new(r"Press: (\d+\.?\d*) hPa;").unwrap();
    let humidity_re: Regex = Regex::new(r"Humidity: (\d+?)%;").unwrap();

    if let Some(cap) = temperature_re.captures(&data) {
        let temp: f32 = (&cap[1]).trim().parse().expect("温度数据解析出错");
        data_write_buffer.store(0, temp);
    }
    if let Some(cap) = rain_re.captures(&data) {
        let rain_adc: f32 = (&cap[1]).trim().parse().expect("");
        let rain_ph: f32 = (3940.0 - rain_adc).abs() / 100.0;
        let rain: f32 = if rain_ph > 1.0 { rain_ph } else { 0.0 };
        data_write_buffer.store(1, rain);
    }
    if let Some(cap) = light_re.captures(&data) {
        let light_adc: f32 = (&cap[1]).trim().parse().expect("");
        let v_adc_mv: f32 = light_adc * 3300.0 / 4095.0;
        let r_lux: f32 = 10000.0 * (v_adc_mv / (3300.0 - v_adc_mv));
        let lux: f32 = 10.0 * (7500.0 / r_lux).powi(2);
        data_write_buffer.store(2, lux);
    }
    if let Some(cap) = press_re.captures(&data) {
        let press_adc: f32 = (&cap[1]).trim().parse().expect("");
        data_write_buffer.store(3, press_adc);
    }
    if let Some(cap) = humidity_re.captures(&data) {
        let temp: f32 = (&cap[1]).trim().parse().expect("");
        data_write_buffer.store(4, temp);
    }

    if data_write_buffer.count == 5 {
        let temp = format!(
            "{},{},{:?},{:?},{:?},{:?},{:?}\n",
            chrono::Local::now().format("%Y-%m-%d"),
            chrono::Local::now().format("%H:%M:%S"),
            data_write_buffer.value[0].unwrap(),
            data_write_buffer.value[1].unwrap(),
            data_write_buffer.value[2].unwrap(),
            data_write_buffer.value[3].unwrap(),
            data_write_buffer.value[4].unwrap()
        );
        write_file(&temp, WriteType::Data);
        data_write_buffer.clear();
    }
}
