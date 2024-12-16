use chrono::{NaiveDate,Datelike};

pub fn parse_date_with_formats(date_str: &str) -> Option<i32> {
    let formats = vec![
        "%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y", "%d-%b-%Y", "%a, %d %b %Y",
        "%Y/%m/%d", "%Y/%m", "%Y-%m-%dT%H:%M:%S%z", "%d%b%Y",
    ];

    // Days offset from Chrono CE to Unix epoch (1970-01-01)
    const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

    for format in formats {
        if let Ok(date) = NaiveDate::parse_from_str(date_str, format) {
            let days_since_epoch = date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE;
            // println!(
            //     "Parsed '{}' as {:?} (Days since epoch: {}) using format '{}'",
            //     date_str, date, days_since_epoch, format
            // );
            return Some(days_since_epoch);
        }
    }
    // println!("Failed to parse date '{}'", date_str);
    None
}