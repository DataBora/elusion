// use std::fs::File;
// use std::io::{self, BufRead, BufReader};
// use encoding_rs::WINDOWS_1252;

// fn detect_and_convert_invalid_utf8(file_path: &str) -> Result<String, io::Error> {
//     let file = File::open(file_path)?;
//     let reader = BufReader::new(file);

//     let output_file_path = format!("{}_utf8.csv", file_path);
//     let mut output_file = File::create(&output_file_path)?;

//     for line in reader.split(b'\n') {
//         let line = line?;
//         match std::str::from_utf8(&line) {
//             Ok(valid_utf8) => writeln!(output_file, "{}", valid_utf8)?,
//             Err(_) => {
//                 let (decoded, _, had_errors) = WINDOWS_1252.decode(&line);
//                 if had_errors {
//                     eprintln!("Warning: Converted invalid UTF-8 data on a line.");
//                 }
//                 writeln!(output_file, "{}", decoded)?;
//             }
//         }
//     }

//     Ok(output_file_path)
// }
