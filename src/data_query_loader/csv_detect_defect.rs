use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::fs::OpenOptions;
use std::io::Write;
use encoding_rs::WINDOWS_1252;

pub fn csv_detect_defect_utf8(file_path: &str) -> Result<(), io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    for (line_number, line) in reader.split(b'\n').enumerate() {
        let line = line?;
        if let Err(err) = std::str::from_utf8(&line) {
            eprintln!(
                "Invalid UTF-8 detected on line {}: {:?}. Error: {:?}",
                line_number + 1,
                line,
                err
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, err));
        }
    }

    Ok(())
}


pub fn convert_invalid_utf8(file_path: &str) -> Result<(), io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let temp_file_path = format!("{}.temp", file_path);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_file_path)?;

    for line in reader.split(b'\n') {
        let line = line?;
        match std::str::from_utf8(&line) {
            Ok(valid_utf8) => writeln!(temp_file, "{}", valid_utf8)?,
            Err(_) => {
                // Convert invalid UTF-8 to valid UTF-8 using a fallback encoding
                let (decoded, _, had_errors) = WINDOWS_1252.decode(&line);
                if had_errors {
                    eprintln!("Warning: Found invalid UTF-8 data and converted it to valid UTF-8.");
                }
                writeln!(temp_file, "{}", decoded)?;
            }
        }
    }

    // Replace original file with cleaned file
    std::fs::rename(temp_file_path, file_path)?;

    Ok(())
}

