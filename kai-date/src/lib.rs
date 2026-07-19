use chrono::{Duration, NaiveDate};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Parse date error: {0}")]
    ParseDate(#[from] chrono::ParseError),

    #[error("Invalid time range: {0} > {1}")]
    InvalidTimeRange(String, String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn as_date(time: &str) -> &str {
    time[0..10].as_ref()
}

pub fn as_year(time: &str) -> &str {
    time[0..4].as_ref()
}

pub fn as_minute(time: &str) -> &str {
    time[..16].as_ref()
}

pub fn get_internal_dates(start: &str, end: &str) -> Result<Vec<String>> {
    if start > end {
        return Err(Error::InvalidTimeRange(start.to_string(), end.to_string()));
    }
    let start_date = NaiveDate::parse_from_str(start, "%Y-%m-%d")?;
    let end_date = NaiveDate::parse_from_str(end, "%Y-%m-%d")?;

    let mut dates = Vec::new();
    let mut current = start_date + Duration::days(1);
    while current < end_date {
        dates.push(current.format("%Y-%m-%d").to_string());
        current += Duration::days(1);
    }
    Ok(dates)
}

pub fn get_date_time_segments(start: &str, end: &str) -> Result<Vec<(String, String)>> {
    if start > end {
        return Err(Error::InvalidTimeRange(start.to_string(), end.to_string()));
    }
    let start_date = as_date(start);
    let end_date = as_date(end);

    if start_date == end_date {
        return Ok(vec![(start.to_string(), end.to_string())]);
    }

    let internal_dates = get_internal_dates(&start_date, &end_date)?;

    let mut segments = Vec::new();
    segments.push((start.to_string(), format!("{} 23:59:59", start_date)));
    for date in internal_dates {
        segments.push((format!("{} 00:00:00", date.as_str()), format!("{} 23:59:59", date.as_str())));
    }
    segments.push((format!("{} 00:00:00", end_date), end.to_string()));
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Time functions ==========

    #[test]
    fn test_parse_date_from_time() {
        assert_eq!(as_date("2026-06-24 15:30:00"), "2026-06-24");
        assert_eq!(as_date("2026-01-01 00:00:00"), "2026-01-01");
    }

    #[test]
    fn test_parse_year_from_date_time() {
        assert_eq!(as_year("2026-06-24"), "2026");
        assert_eq!(as_year("2026-01-01 00:00:00"), "2026");
    }

    #[test]
    fn test_as_minute() {
        assert_eq!(as_minute("2026-06-24 10:30:45"), "2026-06-24 10:30");
        assert_eq!(as_minute("2026-01-01 00:00:00"), "2026-01-01 00:00");
        assert_eq!(as_minute("2026-12-31 23:59:59"), "2026-12-31 23:59");
    }

    #[test]
    fn test_get_internal_dates() {
        let dates = get_internal_dates("2026-06-22", "2026-06-25").unwrap();
        assert_eq!(dates, vec!["2026-06-23", "2026-06-24"]);
    }

    #[test]
    fn test_get_internal_dates_same_day() {
        let dates = get_internal_dates("2026-06-22", "2026-06-22").unwrap();
        assert!(dates.is_empty());
    }

    #[test]
    fn test_get_date_time_segments_multi_day() {
        let segments = get_date_time_segments("2026-06-22 14:30:00", "2026-06-24 10:00:00").unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].0, "2026-06-22 14:30:00");
        assert_eq!(segments[0].1, "2026-06-22 23:59:59");
        assert_eq!(segments[1].0, "2026-06-23 00:00:00");
        assert_eq!(segments[1].1, "2026-06-23 23:59:59");
        assert_eq!(segments[2].0, "2026-06-24 00:00:00");
        assert_eq!(segments[2].1, "2026-06-24 10:00:00");
    }

    #[test]
    fn test_get_date_time_segments_same_day() {
        let segments = get_date_time_segments("2026-06-22 14:30:00", "2026-06-22 15:00:00").unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].0, "2026-06-22 14:30:00");
        assert_eq!(segments[0].1, "2026-06-22 15:00:00");
    }

    #[test]
    fn test_get_date_time_segments_reversed() {
        let result = get_date_time_segments("2026-06-24 10:00:00", "2026-06-22 14:30:00");
        assert!(result.is_err());
    }
}